using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FASTER.libdpr;

namespace dpredis
{
    internal class DpredisBatch
    {
        private RedisClientBuffer clientBuffer = new RedisClientBuffer();
        private List<TaskCompletionSource<(long, string)>> tcs = new List<TaskCompletionSource<(long, string)>>();
        public List<long> startTimes = new List<long>();

        public void AddGetCommand(ulong key, TaskCompletionSource<(long, string)> tcs, long startTime)
        {
            
            var ret = clientBuffer.TryAddGetCommand(key);
            if (!ret) throw new NotImplementedException();
            this.tcs.Add(tcs);
            startTimes.Add(startTime);
        }
        
        public void AddSetCommand(ulong key, ulong value, TaskCompletionSource<(long, string)> tcs, long startTime)
        {
            
            var ret = clientBuffer.TryAddSetCommand(key, value);
            if (!ret) throw new NotImplementedException();
            this.tcs.Add(tcs);
            startTimes.Add(startTime);
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

    public class RedisDirectConnectionSession
    {
        private int batchSize;
        private int numOutstanding;
        private long numOps = 0;
        private Dictionary<Worker, ConcurrentQueue<(TaskCompletionSource<string>, long)>> outstandingOps;
        private ConcurrentDictionary<Worker, (string, int, RedisShard)> routingTable;
        private Dictionary<Worker, RedisDirectConnection> conns;
        
        public long totalLatency = 0;
        private Stopwatch stopwatch;
        
        public RedisDirectConnectionSession(
            ConcurrentDictionary<Worker, (string, int, RedisShard)> routingTable,
            int batchSize)
        {
            this.batchSize = batchSize;
            outstandingOps = new Dictionary<Worker, ConcurrentQueue<(TaskCompletionSource<string>, long)>>();
            this.routingTable = routingTable;
            conns = new Dictionary<Worker, RedisDirectConnection>();
            stopwatch = Stopwatch.StartNew();
        }
        
         private RedisDirectConnection GetDirectConnection(Worker worker)
        {
            if (conns.TryGetValue(worker, out var result)) return result;
            outstandingOps.Add(worker, new ConcurrentQueue<(TaskCompletionSource<string>, long)>());
            result = new RedisDirectConnection(routingTable[worker].Item3, batchSize, -1, s =>
            {
                if (outstandingOps[worker].TryDequeue(out var pair))
                {
                    pair.Item1.SetResult(s);
                    Interlocked.Add(ref totalLatency, stopwatch.ElapsedTicks - pair.Item2);
                    Interlocked.Decrement(ref numOutstanding);
                }
            });
            conns.Add(worker, result);
            return result;
        }

        public Task<string> IssueGetCommand(Worker worker, ulong key)
        {
            numOps++;

            Interlocked.Increment(ref numOutstanding);

            var tcs = new TaskCompletionSource<string>();
            GetDirectConnection(worker).SendGetCommand(key);
            outstandingOps[worker].Enqueue(ValueTuple.Create(tcs, stopwatch.ElapsedTicks));
            
            return tcs.Task;
        }
        
        public Task<string> IssueSetCommand(Worker worker, ulong key, ulong value)
        {
            numOps++;

            Interlocked.Increment(ref numOutstanding);

            var tcs = new TaskCompletionSource<string>();
            GetDirectConnection(worker).SendSetCommand(key, value);
            outstandingOps[worker].Enqueue(ValueTuple.Create(tcs, stopwatch.ElapsedTicks));
            
            return tcs.Task;
        }
        
        public void FlushAll()
        {
            foreach (var conn in conns.Values)
            {
                conn.Flush();
            }
        }

        public int NumOutstanding() => numOutstanding;

        public long IssuedOps() => numOps;
    }
    

    public class DpredisClientSession
    {
        internal class ConnState : MessageUtil.AbstractDprConnState
        {
            private DprClientSession dprSession;
            private DpredisClientSession redisSession;
            private SimpleRedisParser parser;
            public ConnState(Socket socket, DprClientSession dprSession, DpredisClientSession redisSession) : base(socket)
            {
                this.dprSession = dprSession;
                this.redisSession = redisSession;
                parser = new SimpleRedisParser
                {
                    currentMessageType = default,
                    currentMessageStart = -1,
                    subMessageCount = 0
                };
            }

            protected override unsafe void HandleMessage(byte[] buf, int offset, int size)
            {
                fixed (byte* b = buf)
                {
                    ref var header = ref Unsafe.AsRef<DprBatchResponseHeader>(b + offset);
                    dprSession.ResolveBatch(ref header);
                    var completedBatch = redisSession.GetOutstandingBatch(header.batchId);
                    var batchOffset = 0;
                    for (var i = header.Size(); i < size; i++)
                    {
                        if (parser.ProcessChar(offset + i, buf))
                        {
                            var message = new ReadOnlySpan<byte>(buf, parser.currentMessageStart, offset + i - parser.currentMessageStart);
                            completedBatch.GetTcs()[batchOffset].SetResult(ValueTuple.Create(completedBatch.StartSeq + batchOffset, Encoding.ASCII.GetString(message)));
                            parser.currentMessageStart = -1;
                            Interlocked.Add(ref redisSession.totalLatency,
                                redisSession.stopwatch.ElapsedTicks - completedBatch.startTimes[batchOffset]);
                            batchOffset++;
                        }
                    }
                    // Should be a one-to-one mapping between requests and reply
                    Debug.Assert(batchOffset == completedBatch.GetTcs().Count);
                    redisSession.ReturnResolvedBatch(completedBatch);
                }
            }
        }
        
        private long seqNum;
        private int batchSize;
        private int numOutstanding;

        private SimpleObjectPool<DpredisBatch> batchPool;
        private Dictionary<Worker, DpredisBatch> batches;
        private List<KeyValuePair<Worker, DpredisBatch>> toIssue = new List<KeyValuePair<Worker, DpredisBatch>>();
        private ConcurrentDictionary<int, DpredisBatch> outstandingBatches;
        private ConcurrentDictionary<Worker, (string, int, RedisShard)> routingTable;
        private Dictionary<Worker, Socket> conns;

        private DprClientSession dprSession;

        public long totalLatency;
        private Stopwatch stopwatch;

        public DpredisClientSession(DprClient client,
            Guid id,
            ConcurrentDictionary<Worker, (string, int, RedisShard)> routingTable,
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
            stopwatch = Stopwatch.StartNew();
        }
        
        private Socket GetProxyConnection(Worker worker)
        {
            if (conns.TryGetValue(worker, out var result)) return result;
            var (ip, port, _) = routingTable[worker];
            var ipAddr = IPAddress.Parse(ip);
            result = new Socket(ipAddr.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
            result.Connect(new IPEndPoint(ipAddr, port));
            conns.Add(worker, result);
            
            var saea = new SocketAsyncEventArgs();
            // TODO(Tianyu): Magic number buffer size
            saea.SetBuffer(new byte[1 << 20], 0, 1 << 20);
            saea.Completed += MessageUtil.AbstractDprConnState.RecvEventArg_Completed;
            saea.UserToken = new ConnState(result, dprSession, this);
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
            var sock = GetProxyConnection(worker);
            unsafe
            {
                fixed (byte* header = dprBytes)
                {
                    outstandingBatches.TryAdd(Unsafe.AsRef<DprBatchRequestHeader>(header).batchId, batch);
                }
            }
            sock.SendDpredisRequest(dprBytes, batch.Body());
            batch = batchPool.Checkout();
            batch.Reset();
            batches[worker] = batch;
        }

        public Task<(long, string)> IssueGetCommand(Worker worker, ulong key)
        {
            Interlocked.Increment(ref numOutstanding);
            var tcs = new TaskCompletionSource<(long, string)>();
            var batch = GetCurrentBatch(worker);
            batch.AddGetCommand(key, tcs, stopwatch.ElapsedTicks);
            if (batch.CommandCount() == batchSize)
                IssueBatch(worker, batch);
            return tcs.Task;
        }
        
        public Task<(long, string)> IssueSetCommand(Worker worker, ulong key, ulong value)
        {
            Interlocked.Increment(ref numOutstanding);
            var tcs = new TaskCompletionSource<(long, string)>();
            var batch = GetCurrentBatch(worker);
            batch.AddSetCommand(key, value, tcs, stopwatch.ElapsedTicks);
            if (batch.CommandCount() == batchSize)
                IssueBatch(worker, batch);
            return tcs.Task;
        }
        
        public void FlushAll()
        {
            foreach (var batch in batches)
            {
                if (batch.Value.CommandCount() != 0)
                    toIssue.Add(batch);
            }

            foreach (var w in toIssue)
                IssueBatch(w.Key, w.Value);

            toIssue.Clear();

        }

        public int NumOutstanding() => numOutstanding;

        public long IssuedOps() => seqNum;

        private DpredisBatch GetOutstandingBatch(int batchId)
        {
            outstandingBatches.Remove(batchId, out var result);
            return result;
        }

        private void ReturnResolvedBatch(DpredisBatch batch)
        {
            Interlocked.Add(ref numOutstanding, -batch.CommandCount());
            batchPool.Return(batch);
        } 
    }

    public class DpredisClient
    {
        private DprClient dprClient;
        private ConcurrentDictionary<Worker, (string, int, RedisShard)> routingTable;

        public DpredisClient(IDprFinder dprFinder, ConcurrentDictionary<Worker, (string, int, RedisShard)> routingTable)
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

        public RedisDirectConnectionSession NewDirectSession(int batchSize)
        {
            return new RedisDirectConnectionSession(routingTable, batchSize);
        }

        public DpredisClientSession NewSession(int batchSize)
        {
            return new DpredisClientSession(dprClient, Guid.NewGuid(), routingTable, batchSize);
        }
    }
}