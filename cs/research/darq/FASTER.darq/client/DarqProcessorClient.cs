using System;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using FASTER.common;
using FASTER.darq;
using FASTER.libdpr;

namespace FASTER.client
{
    internal class DarqProcessorWriteClient : IDisposable, INetworkMessageConsumer
    {
        private DprSession dprSession;
        private readonly INetworkSender networkSender;

        // TODO(Tianyu): Change to something else for DARQ
        private readonly MaxSizeSettings maxSizeSettings;
        readonly int bufferSize;

        private bool disposed;
        private int offset;
        private int numMessages;
        private readonly int maxOutstanding;
        private volatile int numOutstanding;
        private const int reservedDprHeaderSpace = 80;

        private TaskCompletionSource<long> outstandingRegistrationRequest;
        private ElasticCircularBuffer<TaskCompletionSource<StepStatus>> outstandingStepQueue = new();

        public DarqProcessorWriteClient(DprSession dprSession, string address, int port, int maxOutstanding)
        {
            this.dprSession = dprSession;
            maxSizeSettings = new MaxSizeSettings();
            bufferSize = BufferSizeUtils.ClientBufferSize(maxSizeSettings);

            networkSender = new TcpNetworkSender(GetSendSocket(address, port), maxSizeSettings);
            networkSender.GetResponseObject();
            offset = 2 * sizeof(int) + reservedDprHeaderSpace + BatchHeader.Size;
            numMessages = 0;
            this.maxOutstanding = maxOutstanding;
        }

        public void Dispose()
        {
            disposed = true;
            networkSender.Dispose();
        }

        public unsafe void Flush()
        {
            if (offset > 2 * sizeof(int) + reservedDprHeaderSpace + BatchHeader.Size)
            {
                var head = networkSender.GetResponseObjectHead();
                // Set packet size in header
                *(int*) head = -(offset - sizeof(int));
                head += sizeof(int);

                ((BatchHeader*) head)->SetNumMessagesProtocol(numMessages, WireFormat.DarqProcessor);
                head += sizeof(BatchHeader);

                // Set DprHeader size
                *(int*) head = reservedDprHeaderSpace;
                head += sizeof(int);

                // populate DPR header
                var headerBytes = new Span<byte>(head, reservedDprHeaderSpace);
                if (dprSession.ComputeHeaderForSend(headerBytes) < 0)
                    // TODO(Tianyu): Handle size mismatch by probably copying into a new array and up-ing reserved space in the future
                    throw new NotImplementedException();
                
                Interlocked.Add(ref numOutstanding, numMessages);
                while (numOutstanding >= maxOutstanding)
                {
                    //  Expecting a fairly quick turn around, so just spin
                }

                networkSender.SendResponse(0, offset);
                networkSender.GetResponseObject();
                offset = 2 * sizeof(int) + reservedDprHeaderSpace + BatchHeader.Size;
                numMessages = 0;
            }
        }
        
        public unsafe Task<StepStatus> Step(StepRequest stepRequest, long incarnation, bool forceFlush = true)
        {
            byte* curr, end;
            var entryBatchSize = SerializedDarqEntryBatch.ComputeSerializedSize(stepRequest);
            while (true)
            {
                end = networkSender.GetResponseObjectHead() + bufferSize;
                curr = networkSender.GetResponseObjectHead() + offset;
                var serializedSize = sizeof(byte) + sizeof(WorkerId) + sizeof(long) * 2 + entryBatchSize;
                if (end - curr >= serializedSize && numMessages < maxOutstanding) break;
                Flush();
            }

            *curr = (byte) DarqCommandType.DarqStep;
            curr += sizeof(byte);

            *(long*) curr = incarnation;
            curr += sizeof(long);

            var batch = new SerializedDarqEntryBatch(curr);
            batch.SetContent(stepRequest);
            stepRequest.Dispose();
            curr += entryBatchSize;
            offset = (int) (curr - networkSender.GetResponseObjectHead());
            numMessages++;
            var result = new TaskCompletionSource<StepStatus>();
            outstandingStepQueue.Enqueue(result);
            if (forceFlush) Flush();
            return result.Task;
        }

        public unsafe long RegisterProcessor()
        {
            Debug.Assert(outstandingRegistrationRequest == null);
            byte* curr, end;
            while (true)
            {
                end = networkSender.GetResponseObjectHead() + bufferSize;
                curr = networkSender.GetResponseObjectHead() + offset;
                var serializedSize = sizeof(byte);
                if (end - curr >= serializedSize) break;
                Flush();
            }

            *curr = (byte) DarqCommandType.DarqRegisterProcessor;
            curr += sizeof(byte);

            offset = (int) (curr - networkSender.GetResponseObjectHead());
            numMessages++;
            outstandingRegistrationRequest = new TaskCompletionSource<long>();
            Flush();
            var incarnation = outstandingRegistrationRequest.Task.GetAwaiter().GetResult();
            outstandingRegistrationRequest = null;
            return incarnation;
        }
        
        unsafe void INetworkMessageConsumer.ProcessReplies(byte[] buf, int offset, int size)
        {
            fixed (byte* b = buf)
            {
                var src = b + offset;
                var batchHeader = *(BatchHeader*) src;
                src += sizeof(BatchHeader);

                var dprHeader = new ReadOnlySpan<byte>(src, DprBatchHeader.FixedLenSize);
                src += DprBatchHeader.FixedLenSize;

                if (dprSession.ReceiveHeader(dprHeader, out _) != DprBatchStatus.OK)
                    // TODO(Tianyu): Implement
                    throw new NotImplementedException();

                // TODO(Tianyu): Handle consumer id  mismatch cases
                for (var i = 0; i < batchHeader.NumMessages; i++)
                {
                    var type = *(DarqCommandType*) src;
                    src += sizeof(DarqCommandType);
                    switch (type)
                    {
                        case DarqCommandType.DarqStep:
                            var stepStatus = *(StepStatus*) src;
                            src += sizeof(StepStatus);
                            if (stepStatus == StepStatus.REINCARNATED)
                                // TODO: Terminate execution here  
                                throw new NotImplementedException();
                            var request = outstandingStepQueue.Dequeue();
                            Interlocked.Decrement(ref numOutstanding);
                            request.SetResult(stepStatus);
                            break;
                        case DarqCommandType.DarqRegisterProcessor:
                            Debug.Assert(outstandingRegistrationRequest != null);
                            outstandingRegistrationRequest.SetResult(*(long*) src);
                            outstandingRegistrationRequest = null;
                            break;
                        default:
                            throw new NotImplementedException();
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
            receiveEventArgs.UserToken = new DarqClientNetworkSession<DarqProcessorWriteClient>(socket, this);
            receiveEventArgs.Completed += RecvEventArg_Completed;
            var response = socket.ReceiveAsync(receiveEventArgs);
            Debug.Assert(response);
            return socket;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool HandleReceiveCompletion(SocketAsyncEventArgs e)
        {
            var connState = (DarqClientNetworkSession<DarqProcessorWriteClient>) e.UserToken;
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
                var connState = (DarqClientNetworkSession<DarqProcessorWriteClient>) e.UserToken;
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

    internal class DarqProcessorReadClient : IDisposable, INetworkMessageConsumer
    {
        internal ElasticCircularBuffer<DarqMessage> pendingMessages;
        internal SimpleObjectPool<DarqMessage> messagePool;
        private MaxSizeSettings maxSizeSettings;
        private readonly INetworkSender networkSender;
        private bool disposed;
        private int maxBuffered;
        private DprSession session;
        private bool rolledBack = false;

        public DarqProcessorReadClient(DprSession session, string address, int port, int maxBuffered)
        {
            maxSizeSettings = new MaxSizeSettings();
            networkSender = new TcpNetworkSender(GetSendSocket(address, port), maxSizeSettings);
            this.maxBuffered = maxBuffered;
            messagePool = new SimpleObjectPool<DarqMessage>(() => new DarqMessage(messagePool),  2 * maxBuffered);
            pendingMessages = new ElasticCircularBuffer<DarqMessage>();
            this.session = session;
        }

        public void Dispose()
        {
            disposed = true;
            networkSender.Dispose();
        }

        public unsafe void StartReceivePush()
        {
            var offset = sizeof(int) + BatchHeader.Size;
            var numMessages = 0;
            networkSender.GetResponseObject();
            var curr = networkSender.GetResponseObjectHead() + offset;
            *curr = (byte) DarqCommandType.DarqStartPush;
            curr += sizeof(byte);
            *curr = 1;
            curr += sizeof(byte);
            
            offset = (int) (curr - networkSender.GetResponseObjectHead());
            numMessages++;
            var head = networkSender.GetResponseObjectHead();
            // Set packet size in header
            *(int*) head = -(offset - sizeof(int));
            head += sizeof(int);

            ((BatchHeader*) head)->SetNumMessagesProtocol(numMessages, WireFormat.DarqSubscribe);

            networkSender.SendResponse(0, offset);
        }

        unsafe void INetworkMessageConsumer.ProcessReplies(byte[] buf, int offset, int size)
        {
            if (rolledBack) return;
            
            fixed (byte* b = buf)
            {
                var src = b + offset;

                var count = ((BatchHeader*) src)->NumMessages;
                src += BatchHeader.Size;

                var dprOffset = *(int*) src;
                src += sizeof(int);

                var dprHeaderSize = *(int*) (src + dprOffset);
                var dprHeader = new ReadOnlySpan<byte>(src + dprOffset + sizeof(int), dprHeaderSize);
                var status = session.ReceiveHeader(dprHeader, out var wv);
                if (status == DprBatchStatus.IGNORE)
                    return;
                if (status == DprBatchStatus.ROLLBACK)
                {
                    var m = messagePool.Checkout();
                    // Use a special message to notify of rollback and then go to a sink state
                    m.Reset(DarqMessageType.IN, -1, -1, default, ReadOnlySpan<byte>.Empty);
                    pendingMessages.Enqueue(m);
                    rolledBack = true;
                    return;
                }
                for (int i = 0; i < count; i++)
                {
                    var lsn = *(long*) src;
                    src += sizeof(long);
                    var nextLsn = *(long*) src;
                    src += sizeof(long);
                    var type = *(DarqMessageType*) src;
                    src += sizeof(DarqMessageType);
                    var len = *(int*) src;
                    src += sizeof(int);
                    Debug.Assert(type is DarqMessageType.IN or DarqMessageType.SELF);
                    var m = messagePool.Checkout();
                    m.Reset(type, lsn, nextLsn, wv, new ReadOnlySpan<byte>(src, len));
                    pendingMessages.Enqueue(m);
                    src += len;
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
            receiveEventArgs.UserToken = new DarqClientNetworkSession<DarqProcessorReadClient>(socket, this);
            receiveEventArgs.Completed += RecvEventArg_Completed;
            var response = socket.ReceiveAsync(receiveEventArgs);
            Debug.Assert(response);
            return socket;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool HandleReceiveCompletion(SocketAsyncEventArgs e)
        {
            var connState = (DarqClientNetworkSession<DarqProcessorReadClient>) e.UserToken;
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
                var connState = (DarqClientNetworkSession<DarqProcessorReadClient>) e.UserToken;
                do
                {
                    // No more things to receive
                    if (!HandleReceiveCompletion(e)) break;
                    while (pendingMessages.ApproxCount >= maxBuffered)
                    {
                        // Wait for processor to keep up
                    }
                } while (!connState.socket.ReceiveAsync(e));
            }
            // ignore session socket disposed due to client session dispose
            catch (ObjectDisposedException)
            {
            }
        }
    }


    // TODO(Tianyu): remote client is more simplistic and does not optimize for DPR
    public class DarqProcessorClient : IDarqProcessorClient, IDarqProcessorClientCapabilities, IDisposable
    {
        private string address;
        private int port;
        private ManualResetEventSlim terminationStart, terminationComplete;

        private long incarnation;
        private DprSession session;
        private DarqProcessorReadClient readClient;
        // TODO(Tianyu): May need to make this thread-safe
        private DarqProcessorWriteClient writeClient;

        private int maxOutstandingSteps, maxReadBuffer;

        public DarqProcessorClient(string address, int port, int maxOutstandingSteps = 1 << 10, int maxReadBuffer = 1 << 10)
        {
            this.address = address;
            this.port = port;
            session = new DprSession();
            this.maxOutstandingSteps = maxOutstandingSteps;
            this.maxReadBuffer = maxReadBuffer;
        }

        /// <inheritdoc/>
        public void StartProcessing<T>(T processor) where T : IDarqProcessor
        {
            StartProcessingAsync(processor).GetAwaiter().GetResult();
        }
        
        public unsafe ValueTask<StepStatus> Step(StepRequest request)
        {
            return new ValueTask<StepStatus>(writeClient.Step(request, incarnation, false));
        }

        public DprSession StartUsingDprSessionExternally()
        {
            return session;
        }

        public void StopUsingDprSessionExternally()
        {
        }

        /// <inheritdoc/>
        public async Task StartProcessingAsync<T>(T processor) where T : IDarqProcessor
        {
            var terminationToken = new ManualResetEventSlim();
            if (Interlocked.CompareExchange(ref terminationStart, terminationToken, null) != null)
                // already started
                return;
            terminationComplete = new ManualResetEventSlim();

            readClient = new DarqProcessorReadClient(session, address, port, maxReadBuffer);
            writeClient = new DarqProcessorWriteClient(session, address, port, maxOutstandingSteps);
            incarnation = writeClient.RegisterProcessor();
            readClient.StartReceivePush();
            processor.OnRestart(this);

            while (!terminationStart.IsSet)
            {
                if (!readClient.pendingMessages.IsEmpty())
                {
                    var m = readClient.pendingMessages.Dequeue();
                    // This is a special rollback signal
                    if (m.GetLsn() == -1 && m.GetNextLsn() == -1)
                    {
                        session = new DprSession(session);
                        readClient = new DarqProcessorReadClient(session, address, port, maxReadBuffer);
                        writeClient = new DarqProcessorWriteClient(session, address, port, maxOutstandingSteps);
                        readClient.StartReceivePush();
                        processor.OnRestart(this);
                        continue;
                    }
                    
                    switch (m.GetMessageType())
                    {
                        case DarqMessageType.IN:
                        case DarqMessageType.SELF:
                            // TODO(Tianyu): Hacky
                            if (!processor.ProcessMessage(m))
                            {
                                // TODO(Tianyu): Need to worry about clean shutdown?
                                writeClient.Flush();
                                terminationComplete.Set();
                                return;
                            }
                            break;
                        default:
                            throw new NotImplementedException();
                    }
                }
                writeClient.Flush();
                // Otherwise, just continue looping
            }

            terminationComplete.Set();
        }

        /// <inheritdoc/>
        public void StopProcessing()
        {
            StopProcessingAsync().GetAwaiter().GetResult();
        }

        /// <inheritdoc/>
        public async Task StopProcessingAsync()
        {
            var t = terminationStart;
            var c = terminationComplete;
            if (t == null) return;
            t.Set();
            while (!c.IsSet)
                await Task.Delay(10);
            terminationStart = null;

        }

        public void Dispose()
        {
            readClient.Dispose();
            writeClient.Dispose();
        }
    }
}