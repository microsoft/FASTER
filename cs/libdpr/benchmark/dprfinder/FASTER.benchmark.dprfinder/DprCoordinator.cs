using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using FASTER.core;
using FASTER.libdpr;

namespace FASTER.benchmark
{
    public class DprCoordinator
    {
        public static ClusterConfiguration clusterConfig;
        static DprCoordinator()
        {
            clusterConfig = new ClusterConfiguration();
            clusterConfig.coordinatorIp = "127.0.0.1";
            clusterConfig.coordinatorPort = 15721;
            
            clusterConfig.AddWorker("127.0.0.1", 15445);
        }
        
        private BenchmarkConfiguration benchmarkConfig;
        private IDisposable dprFinderServer;

        public DprCoordinator(BenchmarkConfiguration benchmarkConfig)
        {
            this.benchmarkConfig = benchmarkConfig;
        }

        private void RunDprFinderServer()
        {
            var localDevice1 = new LocalStorageDevice("dpr1.dat", deleteOnClose: true);
            var localDevice2 = new LocalStorageDevice("dpr2.dat", deleteOnClose: true);
            var device = new PingPongDevice(localDevice1, localDevice2);
            if (benchmarkConfig.dprType.Equals("basic"))
            {
                var backend = new GraphDprFinderBackend(device);
                var server =
                    new GraphDprFinderServer(clusterConfig.coordinatorIp, clusterConfig.coordinatorPort, backend);
                server.StartServer();
                dprFinderServer = server;
            }
            else if (benchmarkConfig.dprType.Equals("enhanced"))
            {
                var backend = new EnhancedDprFinderBackend(device);
                var server =
                    new EnhancedDprFinderServer(clusterConfig.coordinatorIp, clusterConfig.coordinatorPort, backend);
                server.StartServer();
                dprFinderServer = server;
            }
            else
            {
                throw new Exception("Unrecognized argument");
            }
        }

        public void Run()
        {
            RunDprFinderServer();
            
            var workerResults = new List<long>();
            var handlerThreads = new List<Thread>();
            var setupFinished = new CountdownEvent(clusterConfig.pods.Count);
            DateTimeOffset start;
            foreach (var workerInfo in clusterConfig.pods)
            {
                var ip = IPAddress.Parse(workerInfo.ip);
                var endPoint = new IPEndPoint(ip, workerInfo.port + 1);
                var sender = new Socket(ip.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
                sender.Connect(endPoint);

                sender.SendBenchmarkControlMessage(benchmarkConfig);
                var handlerThread = new Thread(() =>
                {
                    while (true)
                    {
                        var message = sender.ReceiveBenchmarkMessage();
                        start = DateTimeOffset.UtcNow + TimeSpan.FromSeconds(1);
                        if (message.type == 1)
                        {
                            setupFinished.Signal();
                            break;
                        }
                    }

                    setupFinished.Wait();
                    sender.SendBenchmarkControlMessage(start);

                    while (true)
                    {
                        var message = sender.ReceiveBenchmarkMessage();
                        if (message == null) break;
                        if (message.type == 1)
                        {
                            var result = (List<List<long>>) message.content;
                            lock (workerResults)
                            {
                                foreach(var l in result)
                                    workerResults.AddRange(l);
                            }
                        }
                    }
                    sender.Close();
                });
                handlerThreads.Add(handlerThread);
                handlerThread.Start();
            }

            foreach (var thread in handlerThreads)
                thread.Join();

            workerResults.Sort();
            var avg = workerResults.Average();
            var p99 = workerResults[^(workerResults.Count / 100)];
            Console.WriteLine($"######reported average commit latency {avg}, p99 latency {p99}, {benchmarkConfig}");
            // foreach (var datapoint in workerResults)
                // Console.WriteLine(datapoint);
            dprFinderServer?.Dispose();
        }
    }
}
