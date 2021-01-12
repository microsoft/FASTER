using System;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Text;
using FASTER.libdpr;

namespace dpredis.ycsb
{
    public class YcsbProxy
    {
        private int workerId;
        internal long[] init_keys_;

        public YcsbProxy(int workerId)
        {
            this.workerId = workerId;
        }

        private void PrintToCoordinator(string message, Socket coordinatorConn)
        {
            Console.WriteLine(message);
            coordinatorConn.SendBenchmarkInfoMessage($"worker {workerId}: {message}" + Environment.NewLine);
        }

        public void Run()
        {
            var info = YcsbCoordinator.clusterConfig.GetInfo(workerId);
            var addr = IPAddress.Parse(info.ip);
            var servSock = new Socket(addr.AddressFamily,
                SocketType.Stream, ProtocolType.Tcp);
            // Use port + 1 as control port to communicate with coordinator
            var local = new IPEndPoint(addr, info.port + 1);
            servSock.Bind(local);
            servSock.Listen(512);

            var clientSocket = servSock.Accept();
            var message = clientSocket.ReceiveBenchmarkMessage();
            Debug.Assert(message.type == 1);
            var config = (BenchmarkConfiguration) message.content;
            Execute(info, config, clientSocket);
            clientSocket.Close();
        }

        private void Execute(ClusterWorker info, BenchmarkConfiguration config, Socket coordinatorConn)
        {
            var me = new Worker(workerId);
            var redisBackend = new RedisStateObject(info.redisBackend);
            var dprManager = new DprServer<RedisStateObject, long>(
                new TestDprFinder(config.dprFinderIP, config.dprFinderPort), me, redisBackend, config.checkpointMilli);
            var proxy = new DpredisProxy(info.ip, info.port, dprManager);
            proxy.StartServer();
            if (config.load)
            {
                LoadData(config, coordinatorConn);
                
            }

            coordinatorConn.SendBenchmarkControlMessage("setup finished");
            coordinatorConn.ReceiveBenchmarkMessage();
            dprManager.Start();
            coordinatorConn.ReceiveBenchmarkMessage();
            dprManager.End();
            proxy.StopServer();
        }

        #region Load Data
        private void Setup(DpredisProxy proxy, BenchmarkConfiguration configuration)
        {
            var socket = proxy.GetDirectConnection();
            var buffer = new byte[1 << 20];
            Array.Copy(Encoding.ASCII.GetBytes("*3\r\n$3\r\nSET\r\n"), buffer, 12);
            
            var setupSessions = new Thread[Environment.ProcessorCount];
            var countdown = new CountdownEvent(setupSessions.Length);
            var completed = new ManualResetEventSlim();
            for (var idx = 0; idx < setupSessions.Length; ++idx)
            {
                var x = idx;
                setupSessions[idx] = new Thread(() =>
                {
                    var s = fasterServerless.NewServerlessSession(configuration.windowSize,
                        configuration.batchSize, configuration.checkpointMilli != -1);

                    Value value = default;
                    for (var chunkStart = Interlocked.Add(ref idx_, BenchmarkConsts.kChunkSize) -
                                          BenchmarkConsts.kChunkSize;
                        chunkStart < BenchmarkConsts.kInitCount;
                        chunkStart = Interlocked.Add(ref idx_, BenchmarkConsts.kChunkSize) -
                                     BenchmarkConsts.kChunkSize)
                    {
                        for (var idx = chunkStart; idx < chunkStart + BenchmarkConsts.kChunkSize; ++idx)
                        {
                            if (idx % 256 == 0)
                            {
                                s.Refresh();
                            }

                            var status = s.Upsert(ref init_keys_[idx], ref value, out _);
                            Debug.Assert(status == Status.OK);
                        }
                    }

                    countdown.Signal();
                    while (!completed.IsSet) s.Refresh(); 
                    s.Dispose();
                });
                setupSessions[idx].Start();
            }

            countdown.Wait();
            AsyncContext.Run(async () => await fasterServerless.PerformCheckpoint());
            completed.Set();
            foreach (var s in setupSessions)
                s.Join();
        }          

        public static long KeyForWorker(long original, int workerId)
        {
            // Construct the local key by dropping the highest-order 8 bits and replacing with worker id
            return (long) ((ulong) original >> BenchmarkConsts.kWorkerIdBits) |
                   ((long) workerId << (64 - BenchmarkConsts.kWorkerIdBits));
        }

        private unsafe void LoadDataFromFile(string filePath, BenchmarkConfiguration configuration,
            Socket coordinatorConn)
        {
            var init_filename = filePath + "\\load_" + configuration.distribution + "_250M_raw.dat";
            var txn_filename = filePath + "\\run_" + configuration.distribution + "_250M_1000M_raw.dat";

            long count = 0;
            using (var stream = File.Open(init_filename, FileMode.Open, FileAccess.Read,
                FileShare.Read))
            {
                PrintToCoordinator("loading keys from " + init_filename + " into memory...", coordinatorConn);
                init_keys_ = new long[BenchmarkConsts.kInitCount];

                var chunk = new byte[BenchmarkConsts.kFileChunkSize];
                var chunk_handle = GCHandle.Alloc(chunk, GCHandleType.Pinned);
                var chunk_ptr = (byte*) chunk_handle.AddrOfPinnedObject();

                long offset = 0;

                while (true)
                {
                    stream.Position = offset;
                    int size = stream.Read(chunk, 0, BenchmarkConsts.kFileChunkSize);
                    for (int idx = 0; idx < size; idx += 8)
                    {
                        init_keys_[count] = KeyForWorker(*(long*) (chunk_ptr + idx), workerId);
                        ++count;
                    }

                    if (size == BenchmarkConsts.kFileChunkSize)
                        offset += BenchmarkConsts.kFileChunkSize;
                    else
                        break;

                    if (count == BenchmarkConsts.kInitCount)
                        break;
                }

                if (count != BenchmarkConsts.kInitCount)
                {
                    throw new InvalidDataException("Init file load fail!");
                }
            }

            PrintToCoordinator($"loaded {BenchmarkConsts.kInitCount} keys.", coordinatorConn);
        }

        private void LoadData(BenchmarkConfiguration configuration, Socket coordinatorConn)
        {
            if (BenchmarkConsts.kUseSyntheticData)
            {
                LoadSyntheticData(coordinatorConn);
                return;
            }

            var filePath = "Z:\\ycsb_files";
            LoadDataFromFile(filePath, configuration, coordinatorConn);
        }

        private void LoadSyntheticData(Socket coordinatorConn)
        {
            PrintToCoordinator("Loading synthetic data (uniform distribution)", coordinatorConn);

            init_keys_ = new long[BenchmarkConsts.kInitCount];
            long val = 0;
            for (var idx = 0; idx < BenchmarkConsts.kInitCount; idx++)
            {
                var generatedValue = val++;
                init_keys_[idx] = KeyForWorker(generatedValue, workerId);
            }

            PrintToCoordinator($"loaded {BenchmarkConsts.kInitCount} keys.", coordinatorConn);
        }

        #endregion
    }
}