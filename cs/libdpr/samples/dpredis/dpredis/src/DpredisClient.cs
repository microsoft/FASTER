using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using FASTER.libdpr;

namespace dpredis
{
    internal class DpredisBatch
    {
        private RedisClientBuffer clientBuffer = new RedisClientBuffer();
        private List<TaskCompletionSource<(long, string)>> tcs = new List<TaskCompletionSource<(long, string)>>();

        public void AddGetCommand(ulong key, TaskCompletionSource<(long, string)> tcs)
        {
            
            var ret = clientBuffer.TryAddGetCommand(key);
            if (!ret) throw new NotImplementedException();
            this.tcs.Add(tcs);
        }
        
        public void AddSetCommand(ulong key, ulong value, TaskCompletionSource<(long, string)> tcs)
        {
            
            var ret = clientBuffer.TryAddSetCommand(key, value);
            if (!ret) throw new NotImplementedException();
            this.tcs.Add(tcs);
        }

        public Span<byte> Body() => clientBuffer.GetCurrentBytes();

        public int CommandCount() => clientBuffer.CommandCount();

        public long StartSeq { get; set; }

        public List<TaskCompletionSource<(long, string)>> GetTcs() => tcs;

        public void Reset()
        {
            clientBuffer.Reset();
            // Have to create new one. Old one is referred to by responses.
            tcs.Clear();
        }
    }

    internal class DpredisClientDprConnState : MessageUtil.AbstractDprConnState
    {
        private DprClientSession dprSession;
        private DpredisClientSession redisSession;
        private static readonly char[] redisSeparators = {'+', '\r', '\n'};

        public DpredisClientDprConnState(Socket socket, DprClientSession dprSession, DpredisClientSession redisSession) : base(socket)
        {
            this.dprSession = dprSession;
            this.redisSession = redisSession;
        }

        protected override unsafe void HandleMessage(byte[] buf, int offset, int size)
        {
            fixed (byte* b = buf)
            {
                ref var header = ref Unsafe.AsRef<DprBatchResponseHeader>(b + offset);
                dprSession.ResolveBatch(ref header);
                // TODO(Tianyu): Eventually add more Redis types. Now we assume all responses are simple strings and therefore
                // use very simplistic parsing
                var redisResponse = Encoding.ASCII.GetString(buf, offset + header.Size(), size - header.Size());
                var entries = redisResponse.Split(redisSeparators, StringSplitOptions.RemoveEmptyEntries);
                var completedBatch = redisSession.GetOutstandingBatch(header.batchId);
                // Should be a one-to-one mapping between requests and reply
                Debug.Assert(entries.Length == completedBatch.GetTcs().Count);
                for (var i = 0; i < entries.Length; i++)
                    completedBatch.GetTcs()[i].SetResult(ValueTuple.Create(completedBatch.StartSeq + i, entries[i]));
                redisSession.ReturnResolvedBatch(completedBatch);
            }
        }
    }

    public class DpredisClientSession
    {
        private long seqNum = 0;
        private int batchSize;
        private int numOutstanding;

        private SimpleObjectPool<DpredisBatch> batchPool;
        private Dictionary<Worker, DpredisBatch> batches;
        private ConcurrentDictionary<int, DpredisBatch> outstandingBatches;
        private ConcurrentDictionary<Worker, (string, int)> routingTable;
        private Dictionary<Worker, Socket> conns;

        private DprClientSession dprSession;

        public DpredisClientSession(DprClient client,
            Guid id,
            ConcurrentDictionary<Worker, (string, int)> routingTable,
            int batchSize)
        {
            seqNum = 0;
            this.batchSize = batchSize;
            batchPool = new SimpleObjectPool<DpredisBatch>(() => new DpredisBatch());
            batches = new Dictionary<Worker, DpredisBatch>();
            outstandingBatches = new ConcurrentDictionary<int, DpredisBatch>();
            this.routingTable = routingTable;
            conns = new Dictionary<Worker, Socket>();
            dprSession = client.GetSession(id);
        }

        private Socket GetRedisConnection(Worker worker)
        {
            if (conns.TryGetValue(worker, out var result)) return result;
            var (ip, port) = routingTable[worker];
            var ipAddr = IPAddress.Parse(ip);
            result = new Socket(ipAddr.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
            result.Connect(new IPEndPoint(ipAddr, port));
            conns.Add(worker, result);
            
            var saea = new SocketAsyncEventArgs();
            // TODO(Tianyu): Magic number buffer size
            saea.SetBuffer(new byte[1 << 20]);
            saea.Completed += MessageUtil.AbstractDprConnState.RecvEventArg_Completed;
            saea.UserToken = new DpredisClientDprConnState(result, dprSession, this);
            while (!result.ReceiveAsync(saea))
                MessageUtil.AbstractDprConnState.RecvEventArg_Completed(null, saea);
            return result;
        }

        private DpredisBatch GetCurrentBatch(Worker worker)
        {
            if (!batches.TryGetValue(worker, out var batch))
            {
                batch = batchPool.Checkout();
                batch.Reset();
                batches.Add(worker, batch);
            }
            return batch;
        }

        private void IssueBatch(Worker worker, DpredisBatch batch)
        {
            batch.StartSeq = seqNum;
            seqNum += batch.CommandCount();
            dprSession.IssueBatch(batch.CommandCount(), worker, out var dprBytes);
            var sock = GetRedisConnection(worker);
            sock.SendDpredisRequest(dprBytes, batch.Body());
            unsafe
            {
                fixed (byte* header = dprBytes)
                {
                    outstandingBatches.TryAdd(Unsafe.AsRef<DprBatchRequestHeader>(header).batchId, batch);
                }
            }
            batch = batchPool.Checkout();
            batch.Reset();
            batches[worker] = batch;
        }

        public Task<(long, string)> IssueGetCommand(Worker worker, ulong key)
        {
            numOutstanding++;
            var tcs = new TaskCompletionSource<(long, string)>();
            var batch = GetCurrentBatch(worker);
            batch.AddGetCommand(key, tcs);
            if (batch.CommandCount() == batchSize)
                IssueBatch(worker, batch);
            return tcs.Task;
        }
        
        public Task<(long, string)> IssueSetCommand(Worker worker, ulong key, ulong value)
        {
            numOutstanding++;
            var tcs = new TaskCompletionSource<(long, string)>();
            var batch = GetCurrentBatch(worker);
            batch.AddSetCommand(key, value, tcs);
            if (batch.CommandCount() == batchSize)
                IssueBatch(worker, batch);
            return tcs.Task;
        }
        
        public void FlushAll()
        {
            foreach (var batch in batches)
            {
                if (batch.Value.CommandCount() != 0)
                    IssueBatch(batch.Key, batch.Value);
            }
        }

        public int NumOutstanding() => numOutstanding;

        public long IssuedOps() => seqNum;

        internal DpredisBatch GetOutstandingBatch(int batchId)
        {
            outstandingBatches.Remove(batchId, out var result);
            return result;
        }

        internal void ReturnResolvedBatch(DpredisBatch batch)
        {
            numOutstanding -= batch.CommandCount();
            batchPool.Return(batch);
        } 
    }

    public class DpredisClient
    {
        private DprClient dprClient;
        private ConcurrentDictionary<Worker, (string, int)> routingTable;

        public DpredisClient(IDprFinder dprFinder, ConcurrentDictionary<Worker, (string, int)> routingTable)
        {
            dprClient = new DprClient(dprFinder);
            this.routingTable = routingTable;
        }

        public void Start()
        {
            dprClient.Start();
        }

        public void End()
        {
            dprClient.End();
        }

        public DpredisClientSession NewSession(int batchSize)
        {
            return new DpredisClientSession(dprClient, Guid.NewGuid(), routingTable, batchSize);
        }
    }
}