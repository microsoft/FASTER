using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Contracts;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Threading;
using FASTER.libdpr;

namespace dpredis.ycsb
{
    public enum Op : ulong
    {
        Upsert = 0,
        Read = 1,
        ReadModifyWrite = 2
    }

    public class YcsbClient
    {
        private int workerId;
        internal volatile bool done;


        public YcsbClient(int workerId)
        {
            this.workerId = workerId;
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
            Execute(config, clientSocket);
            clientSocket.Close();
        }

        private Worker GetWorker(ulong key)
        {
            return new Worker((long) key >> 61);
        }

        private void Execute(BenchmarkConfiguration config, Socket coordinatorConn)
        {
            var dprFinder = new TestDprFinder(config.dprFinderIP, config.dprFinderPort);
            var routingTable = new ConcurrentDictionary<Worker, (string, int)>();
            foreach (var worker in YcsbCoordinator.clusterConfig.workers)
            {
                if (worker.type == WorkerType.CLIENT) continue;
                routingTable.TryAdd(new Worker(worker.id), ValueTuple.Create(worker.ip, worker.port));
            }

            LoadData(config, coordinatorConn);
            coordinatorConn.SendBenchmarkControlMessage("setup finished");
            coordinatorConn.ReceiveBenchmarkMessage();
            // TODO(Tianyu): Replace with real workload
            long idx_ = 0;
            var execThreads = new Thread[config.clientThreadCount];
            var localSessions = new DpredisClientSession[config.clientThreadCount];
            var client = new DpredisClient(dprFinder, routingTable);
            var sw = new Stopwatch();
            long totalOps = 0;
            sw.Start();
            for (var idx = 0; idx < config.clientThreadCount; ++idx)
            {
                var x = idx;
                execThreads[idx] = new Thread(() =>
                {
                    var s = client.NewSession(config.batchSize);
                    localSessions[x] = s;

                    var rng = new RandomGenerator((uint) (workerId * YcsbCoordinator.clusterConfig.clients.Count + x));
                    while (!done)
                    {
                        var chunk_idx = Interlocked.Add(ref idx_, BenchmarkConsts.kChunkSize) -
                                        BenchmarkConsts.kChunkSize;
                        while (chunk_idx >= BenchmarkConsts.kTxnCount)
                        {
                            if (chunk_idx == BenchmarkConsts.kTxnCount) idx_ = 0;
                            chunk_idx = Interlocked.Add(ref idx_, BenchmarkConsts.kChunkSize) -
                                        BenchmarkConsts.kChunkSize;
                        }

                        for (var idx = chunk_idx; idx < chunk_idx + BenchmarkConsts.kChunkSize && !done; ++idx)
                        {
                            Op op;
                            int r = (int) rng.Generate(100);
                            if (r < config.readPercent)
                                op = Op.Read;
                            else if (config.readPercent >= 0)
                                op = Op.Upsert;
                            else
                                throw new NotImplementedException();
                            var key = txn_keys_[idx];

                            while (s.NumOutstanding() >= config.windowSize)
                                Thread.Yield();
                            
                            switch (op)
                            {
                                case Op.Upsert:
                                    s.IssueSetCommand(GetWorker(key), key, 0);
                                    break;
                                case Op.Read:
                                    s.IssueGetCommand(GetWorker(key), key);
                                    break;
                                default:
                                    throw new InvalidOperationException("Unexpected op: " + op);
                            }
                        }
                    }

                    s.FlushAll();
                    while (s.NumOutstanding() != 0)
                        Thread.Yield();
                    Interlocked.Add(ref totalOps,  s.IssuedOps());
                });
                execThreads[idx].Start();
            }

            done = true;
            PrintToCoordinator($"Done issuing operations", coordinatorConn);
            foreach (var session in execThreads)
                session.Join();
            sw.Stop();
            var seconds = sw.ElapsedMilliseconds / 1000.0;

            // Signal completion
            PrintToCoordinator($"##, {totalOps / seconds}", coordinatorConn);
            coordinatorConn.SendBenchmarkControlMessage(totalOps / seconds);
            coordinatorConn.ReceiveBenchmarkMessage();
            dprFinder.Clear();
            client.End();
        }

        #region Load Data
        internal ulong[] txn_keys_;

        public static ulong KeyForWorker(ulong original, int workerId)
        {
            // Construct the local key by dropping the highest-order 8 bits and replacing with worker id
            return (original >> BenchmarkConsts.kWorkerIdBits) |
                   ((ulong) workerId << (64 - BenchmarkConsts.kWorkerIdBits));
        }

        private void PrintToCoordinator(string message, Socket coordinatorConn)
        {
            Console.WriteLine(message);
            coordinatorConn.SendBenchmarkInfoMessage($"worker {workerId}: {message}" + Environment.NewLine);
        }

        private unsafe void LoadDataFromFile(string filePath, BenchmarkConfiguration configuration,
            Socket coordinatorConn)
        {
            var txn_filename = filePath + "\\run_" + configuration.distribution + "_250M_1000M_raw.dat";

            long count = 0;

            using (var stream = File.Open(txn_filename, FileMode.Open, FileAccess.Read, FileShare.Read))
            {
                var chunk = new byte[BenchmarkConsts.kFileChunkSize];
                var chunk_handle = GCHandle.Alloc(chunk, GCHandleType.Pinned);
                var chunk_ptr = (byte*) chunk_handle.AddrOfPinnedObject();

                PrintToCoordinator($"loading txns from {txn_filename} into memory...", coordinatorConn);

                txn_keys_ = new ulong[BenchmarkConsts.kTxnCount];

                long offset = 0;

                var rng = new RandomGenerator((uint) workerId);

                while (true)
                {
                    stream.Position = offset;
                    var size = stream.Read(chunk, 0, BenchmarkConsts.kFileChunkSize);
                    for (var idx = 0; idx < size; idx += 8)
                    {
                        var owner = (int) rng.Generate((uint) YcsbCoordinator.clusterConfig.proxies.Count);
                        txn_keys_[count] = KeyForWorker(*(ulong*) (chunk_ptr + idx), owner);
                        ++count;

                        if (count % (BenchmarkConsts.kTxnCount / 100) == 0)
                            Console.Write(".");
                    }

                    if (size == BenchmarkConsts.kFileChunkSize)
                        offset += BenchmarkConsts.kFileChunkSize;
                    else
                        break;

                    if (count == BenchmarkConsts.kTxnCount)
                        break;
                }

                if (count != BenchmarkConsts.kTxnCount)
                {
                    throw new InvalidDataException($"Txn file load fail! {count}: {BenchmarkConsts.kTxnCount}");
                }
            }

            PrintToCoordinator($"loaded {BenchmarkConsts.kTxnCount} txns.", coordinatorConn);
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

            var generator = new RandomGenerator();

            txn_keys_ = new ulong[BenchmarkConsts.kTxnCount];
            var rng = new RandomGenerator((uint) workerId);

            for (var idx = 0; idx < BenchmarkConsts.kTxnCount; idx++)
            {
                var owner = (int) rng.Generate((uint) YcsbCoordinator.clusterConfig.proxies.Count);
                var generatedValue = generator.Generate64(BenchmarkConsts.kInitCount);
                txn_keys_[idx] = KeyForWorker(generatedValue, owner);
            }

            PrintToCoordinator($"loaded {BenchmarkConsts.kTxnCount} txns.", coordinatorConn);
        }

        #endregion
    }
}