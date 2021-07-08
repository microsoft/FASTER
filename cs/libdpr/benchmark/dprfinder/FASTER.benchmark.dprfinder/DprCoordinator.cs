using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;

namespace FASTER.benchmark
{
    public class DprCoordinator
    {
        public static ClusterConfiguration clusterConfig;
        static DprCoordinator()
        {
            clusterConfig = new ClusterConfiguration();
            clusterConfig.AddWorker("10.0.1.8", 15721)
            .AddWorker("10.0.1.9", 15721)
            .AddWorker("10.0.1.11", 15721)
            .AddWorker("10.0.1.10", 15721)
            .AddWorker("10.0.1.12", 15721)
            .AddWorker("10.0.1.13", 15721)
            .AddWorker("10.0.1.14", 15721)
            .AddWorker("10.0.1.15", 15721);
        }
        
        private BenchmarkConfiguration benchmarkConfig;

        public DprCoordinator(BenchmarkConfiguration benchmarkConfig)
        {
            this.benchmarkConfig = benchmarkConfig;
        }

        public void Run()
        {
            // foreach (var workerInfo in clusterConfig.pods)
            // {
            //     var ip = IPAddress.Parse(workerInfo.GetAddress());
            //     var endPoint = new IPEndPoint(ip, 15000);
            //     var sender = new Socket(ip.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
            //     sender.NoDelay = true;
            //     sender.Connect(endPoint);
            //     sender.Close();
            // }

            // Thread.Sleep(5000);
            
            var conn = new SqlConnection(benchmarkConfig.connString);
            conn.Open();
            var deleteCommand = new SqlCommand("EXEC cleanup", conn);
            deleteCommand.ExecuteNonQuery();
            conn.Close();
            
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
            conn.Dispose();

            workerResults.Sort();
            var avg = workerResults.Average();
            var p99 = workerResults[^(workerResults.Count / 100)];
            Console.WriteLine($"######reported average commit latency {avg}, p99 latency {p99}, {benchmarkConfig}");
            // foreach (var datapoint in workerResults)
                // Console.WriteLine(datapoint);
        }
    }
}
