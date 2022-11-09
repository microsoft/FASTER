using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using FASTER.common;
using FASTER.core;
using FASTER.libdpr;

namespace FASTER.client
{
    public interface IDarqClusterInfo
    {
        IDprFinder GetNewDprFinder();

        (string, int) GetWorker(WorkerId worker);
    }

    public class RollbackException : FasterException
    {
        public readonly long worldLine;

        public RollbackException(long worldLine)
        {
            this.worldLine = worldLine;
        }
    }

    [Serializable]
    public class HardCodedClusterInfo : IDarqClusterInfo
    {
        private Dictionary<WorkerId, (string, int)> workerMap;
        private string dprFinderIp;
        private int dprFinderPort = -1;

        public HardCodedClusterInfo()
        {
            workerMap = new Dictionary<WorkerId, (string, int)>();
        }

        public HardCodedClusterInfo AddWorker(WorkerId worker, string ip, int port)
        {
            workerMap.Add(worker, (ip, port));
            return this;
        }

        public HardCodedClusterInfo SetDprFinder(string ip, int port)
        {
            dprFinderIp = ip;
            dprFinderPort = port;
            return this;
        }

        public (string, int) GetDprFinderInfo() => (dprFinderIp, dprFinderPort);

        public IDprFinder GetNewDprFinder()
        {
            if (dprFinderPort == -1 || dprFinderIp == null)
                throw new FasterException("DprFinder location not set!");
            return new RespGraphDprFinder(dprFinderIp, dprFinderPort);
        }

        public (string, int) GetWorker(WorkerId worker)
        {
            return workerMap[worker];
        }

        public IEnumerable<(WorkerId, string, int)> GetWorkers() =>
            workerMap.Select(e => (e.Key, e.Value.Item1, e.Value.Item2));
    }

    /// <summary>
    /// Producer client to add sprints to DARQ.
    /// </summary>
    public class DarqProducerClient : IDisposable
    {
        private IDarqClusterInfo darqClusterInfo;
        private ConcurrentDictionary<WorkerId, SingleDarqProducerClient> clients;
        private DprSession dprSession;

        /// <summary>
        /// Creates a new DarqProducerClient
        /// </summary>
        /// <param name="darqClusterInfo"> Cluster information </param>
        /// <param name="session"> The DprClientSession to use (optional) </param>
        public DarqProducerClient(IDarqClusterInfo darqClusterInfo, DprSession session = null)
        {
            this.darqClusterInfo = darqClusterInfo;
            clients = new ConcurrentDictionary<WorkerId, SingleDarqProducerClient>();
            dprSession = session ?? new DprSession();
        }

        public void Dispose()
        {
            foreach (var client in clients.Values)
                client.Dispose();
        }

        /// <summary>
        /// Enqueues a sprint into the DARQ. Task will complete when DARQ has acked the enqueue, or when the enqueue is
        /// committed and recoverable if waitCommit is true.
        /// </summary>
        /// <param name="darqId">ID of the DARQ to enqueue onto</param>
        /// <param name="message"> body of the sprint </param>
        /// <param name="producerId"> producer ID to use (for deduplication purposes), or -1 if none </param>
        /// <param name="lsn">lsn to use (for deduplication purposes), should be monotonically increasing in every producer</param>
        /// <param name="forceFlush">
        /// whether to force flush buffer and send all requests. If false, requests are buffered
        /// until a set number has been accumulated or until forced to flush
        /// </param>
        /// <param name="waitCommit">whether to wait until the enqueue is committed to complete the async task </param>
        /// <returns></returns>
        public Task EnqueueMessageAsync(WorkerId darqId, ReadOnlySpan<byte> message, long producerId, long lsn,
            bool forceFlush = true, bool waitCommit = false)
        {
            var singleClient = clients.GetOrAdd(darqId, w =>
            {
                var (ip, port) = darqClusterInfo.GetWorker(w);
                return new SingleDarqProducerClient(dprSession, w, ip, port);
            });
            var task = singleClient.EnqueueMessageAsync(message, producerId, lsn, waitCommit);

            if (forceFlush)
            {
                foreach (var client in clients.Values)
                    client.Flush();
            }

            return task;
        }

        public void EnqueueMessageWithCallback(WorkerId darqId, ReadOnlySpan<byte> message, long producerId, long lsn,
            Action<long> callback,
            bool forceFlush = true, bool waitCommit = false)
        {
            var singleClient = clients.GetOrAdd(darqId, w =>
            {
                var (ip, port) = darqClusterInfo.GetWorker(w);
                return new SingleDarqProducerClient(dprSession, w, ip, port);
            });

            singleClient.EnqueueMessageWithCallback(message, producerId, lsn, callback, waitCommit);
            if (forceFlush)
            {
                foreach (var client in clients.Values)
                    client.Flush();
            }
        }

        public void ForceFlush()
        {
            foreach (var client in clients.Values)
                client.Flush();
        }


        /// <summary>
        /// Periodically invoke if waiting for a task to become committed
        /// </summary>
        public void Refresh(IDprStateSnapshot state)
        {
            foreach (var client in clients.Values)
                client.ComputeCommit(state);
        }
    }

    internal class SingleDarqProducerClient : IDisposable, INetworkMessageConsumer
    {
        private DprSession dprClientSession;
        private readonly INetworkSender networkSender;
        private WorkerId target;

        // TODO(Tianyu): Change to something else for DARQ
        private readonly MaxSizeSettings maxSizeSettings;
        readonly int bufferSize;

        private bool disposed;
        private int offset;
        private int numMessages;
        private const int reservedDprHeaderSpace = 160;
        private ElasticCircularBuffer<Action<long>> callbackQueue;
        private ElasticCircularBuffer<(long, Action<long>)> commitQueue;
        private long committed = 0;

        private long rolledbackWorldline = -1;

        public SingleDarqProducerClient(DprSession dprClientSession, WorkerId target, string address, int port)
        {
            this.dprClientSession = dprClientSession;
            this.target = target;
            maxSizeSettings = new MaxSizeSettings();
            bufferSize = BufferSizeUtils.ClientBufferSize(maxSizeSettings);

            networkSender = new TcpNetworkSender(GetSendSocket(address, port), maxSizeSettings);
            networkSender.GetResponseObject();
            offset = 2 * sizeof(int) + reservedDprHeaderSpace + BatchHeader.Size;
            numMessages = 0;

            callbackQueue = new ElasticCircularBuffer<Action<long>>();
            commitQueue = new ElasticCircularBuffer<(long, Action<long>)>();
        }

        public void Dispose()
        {
            disposed = true;
            networkSender.Dispose();
        }

        internal unsafe void Flush()
        {
            if (offset > 2 * sizeof(int) + reservedDprHeaderSpace + BatchHeader.Size)
            {
                var head = networkSender.GetResponseObjectHead();
                // Set packet size in header
                *(int*) head = -(offset - sizeof(int));
                head += sizeof(int);

                ((BatchHeader*) head)->SetNumMessagesProtocol(numMessages, WireFormat.DarqProducer);
                head += sizeof(BatchHeader);

                // Set DprHeader size
                *(int*) head = reservedDprHeaderSpace;
                head += sizeof(int);

                // populate DPR header
                var headerBytes = new Span<byte>(head, reservedDprHeaderSpace);
                if (dprClientSession.ComputeHeaderForSend(headerBytes) < 0)
                    // TODO(Tianyu): Handle size mismatch by probably copying into a new array and up-ing reserved space in the future
                    throw new NotImplementedException();
                if (!networkSender.SendResponse(0, offset))
                {
                    throw new ObjectDisposedException("socket closed");
                }

                networkSender.GetResponseObject();
                offset = 2 * sizeof(int) + reservedDprHeaderSpace + BatchHeader.Size;
                numMessages = 0;
            }
        }

        internal void ComputeCommit(IDprStateSnapshot snapshot)
        {
            if (snapshot.SystemWorldLine() != dprClientSession.WorldLine)
            {
                rolledbackWorldline = snapshot.SystemWorldLine();
                throw new RollbackException(rolledbackWorldline);
            }
            committed = snapshot.SafeVersion(target);
            while (!commitQueue.IsEmpty())
            {
                var (v, a) = commitQueue.PeekFirst();
                if (v > committed) break;
                commitQueue.Dequeue();
                a(v);
            }
        }

        public Task EnqueueMessageAsync(ReadOnlySpan<byte> message, long producerId, long lsn, bool waitCommit = false)
        {
            var tcs = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);
            EnqueueMessageWithCallback(message, producerId, lsn, v => tcs.SetResult(null), waitCommit);
            return tcs.Task;
        }

        public void EnqueueMessageWithCallback(ReadOnlySpan<byte> message, long producerId, long lsn,
            Action<long> callback,
            bool waitCommit)
        {
            if (rolledbackWorldline != -1)
                throw new RollbackException(rolledbackWorldline);
            if (waitCommit)
            {
                EnqueueMessageInternal(message, producerId, lsn, v =>
                {
                    // TODO(Tianyu): This may not guarantee that a callback is always invoked if races, especially if there won't be any future versions
                    if (committed > v)
                        callback(v);
                    else
                        commitQueue.Enqueue((lsn, callback));
                });
            }
            else
            {
                EnqueueMessageInternal(message, producerId, lsn, callback);
            }
        }

        internal unsafe void EnqueueMessageInternal(ReadOnlySpan<byte> message, long id, long lsn, Action<long> action)
        {
            byte* curr, end;
            var entryBatchSize = SerializedDarqEntryBatch.ComputeSerializedSize(message);
            while (true)
            {
                end = networkSender.GetResponseObjectHead() + bufferSize;
                curr = networkSender.GetResponseObjectHead() + offset;
                var serializedSize = sizeof(byte) + sizeof(long) * 2 + entryBatchSize;
                if (end - curr >= serializedSize) break;
                Flush();
            }

            *curr = (byte) MessageType.DarqEnqueue;
            curr += sizeof(byte);

            *(long*) curr = id;
            curr += sizeof(long);
            *(long*) curr = lsn;
            curr += sizeof(long);

            var batch = new SerializedDarqEntryBatch(curr);
            batch.SetContent(message);
            curr += entryBatchSize;
            offset = (int) (curr - networkSender.GetResponseObjectHead());
            numMessages++;
            callbackQueue.Enqueue(action);
        }

        unsafe void INetworkMessageConsumer.ProcessReplies(byte[] buf, int startOffset, int size)
        {
            if (rolledbackWorldline != -1) return;
            
            fixed (byte* b = buf)
            {
                var src = b + startOffset;
                
                var count = ((BatchHeader*) src)->NumMessages;
                src += BatchHeader.Size;

                var dprHeader = new ReadOnlySpan<byte>(src, DprBatchHeader.FixedLenSize);
                src += DprBatchHeader.FixedLenSize;

                // TODO(Tianyu): handle here
                if (dprClientSession.ReceiveHeader(dprHeader, out var wv) != DprBatchStatus.OK)
                {
                    rolledbackWorldline = dprClientSession.TerminalWorldLine;
                    return;
                }
                
                for (int i = 0; i < count; i++)
                {
                    switch ((MessageType) (*src++))
                    {
                        case MessageType.DarqEnqueue:
                            // Pretty sure don't actually need to check for Math.Max here because requests are serviced in order?
                            callbackQueue.Dequeue()(wv.Version);
                            break;
                        default:
                            throw new FasterException("Unexpected return type");
                    }
                }
            }
        }

        private Socket GetSendSocket(string address, int port, int millisecondsTimeout = -2)
        {
            var ip = IPAddress.Parse(address);
            var endPoint = new IPEndPoint(ip, port);
            var socket = new Socket(ip.AddressFamily, SocketType.Stream, ProtocolType.Tcp)
            {
                NoDelay = true
            };

            if (millisecondsTimeout != -2)
            {
                IAsyncResult result = socket.BeginConnect(endPoint, null, null);
                result.AsyncWaitHandle.WaitOne(millisecondsTimeout, true);
                if (socket.Connected)
                    socket.EndConnect(result);
                else
                {
                    socket.Close();
                    throw new Exception("Failed to connect server.");
                }
            }
            else
            {
                socket.Connect(endPoint);
            }

            // Ok to create new event args on accept because we assume a connection to be long-running
            var receiveEventArgs = new SocketAsyncEventArgs();
            var bufferSize = BufferSizeUtils.ServerBufferSize(maxSizeSettings);
            receiveEventArgs.SetBuffer(new byte[bufferSize], 0, bufferSize);
            receiveEventArgs.UserToken = new DarqClientNetworkSession<SingleDarqProducerClient>(socket, this);
            receiveEventArgs.Completed += RecvEventArg_Completed;
            var response = socket.ReceiveAsync(receiveEventArgs);
            Debug.Assert(response);
            return socket;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool HandleReceiveCompletion(SocketAsyncEventArgs e)
        {
            var connState = (DarqClientNetworkSession<SingleDarqProducerClient>) e.UserToken;
            if (e.BytesTransferred == 0 || e.SocketError != SocketError.Success || disposed)
            {
                connState.socket.Dispose();
                e.Dispose();
                return false;
            }

            connState.AddBytesRead(e.BytesTransferred);
            var newHead = connState.TryConsumeMessages(e.Buffer);
            if (newHead == e.Buffer.Length)
            {
                // Need to grow input buffer
                var newBuffer = new byte[e.Buffer.Length * 2];
                Array.Copy(e.Buffer, newBuffer, e.Buffer.Length);
                e.SetBuffer(newBuffer, newHead, newBuffer.Length - newHead);
            }
            else
                e.SetBuffer(newHead, e.Buffer.Length - newHead);

            return true;
        }

        private void RecvEventArg_Completed(object sender, SocketAsyncEventArgs e)
        {
            try
            {
                var connState = (DarqClientNetworkSession<SingleDarqProducerClient>) e.UserToken;
                do
                {
                    // No more things to receive
                    if (!HandleReceiveCompletion(e)) break;
                } while (!connState.socket.ReceiveAsync(e));
            }
            // ignore session socket disposed due to client session dispose
            catch (ObjectDisposedException)
            {
            }
        }
    }
}