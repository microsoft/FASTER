using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Threading;

namespace dpredis.ycsb
{
    [Serializable]
    public class BenchmarkConfiguration
    {
        public int clientThreadCount;
        public string distribution;
        public int readPercent;
        public int checkpointMilli;
        public int windowSize;
        public int batchSize;
        public string dprFinderIP;
        public int dprFinderPort;
        public bool load, useProxy;

        public override string ToString()
        {
            return $"{nameof(clientThreadCount)}: {clientThreadCount}," +
                   $"{nameof(distribution)}: {distribution}," +
                   $"{nameof(readPercent)}: {readPercent}," +
                   $"{nameof(checkpointMilli)}: {checkpointMilli}," +
                   $"{nameof(windowSize)}: {windowSize}," +
                   $"{nameof(batchSize)}: {batchSize}";
        }
    }

    public enum WorkerType
    {
        PROXY, CLIENT
    }
    public struct ClusterWorker
    {
        public WorkerType type;
        public long id;
        public string ip;
        public int port;
        public RedisShard redisBackend;
    }
    
    public class ClusterConfiguration
    {
        public List<ClusterWorker> workers = new List<ClusterWorker>();
        public List<ClusterWorker> clients = new List<ClusterWorker>();
        public List<ClusterWorker> proxies = new List<ClusterWorker>();

        public ClusterConfiguration AddClient(string ip, int port)
        {
            var worker = new ClusterWorker {type = WorkerType.CLIENT, id = workers.Count, ip = ip, port = port};
            workers.Add(worker);
            clients.Add(worker);
            return this;
        }

        public ClusterConfiguration AddProxy(string ip, int port, RedisShard redisBackend)
        {
            var worker = new ClusterWorker
                {type = WorkerType.PROXY, id = workers.Count, ip = ip, port = port, redisBackend = redisBackend};
            workers.Add(worker);
            proxies.Add(worker);
            return this;
        }

        public ClusterWorker GetInfo(int id) => workers[id];
    }

    public class YcsbCoordinator
    {
        public static ClusterConfiguration clusterConfig;

        private BenchmarkConfiguration config;

        public YcsbCoordinator(BenchmarkConfiguration config)
        {
            this.config = config;
        }

        public void Run()
        {
            var handlerThreads = new List<Thread>();
            var setupFinished = new CountdownEvent(clusterConfig.workers.Count);
            var clientCountdown = new CountdownEvent(clusterConfig.clients.Count);
            var shutdown = new ManualResetEventSlim();
            long totalOps = 0;
            var stopwatch = new Stopwatch();

            foreach (var memberInfo in clusterConfig.clients)
            {
                var ip = IPAddress.Parse(memberInfo.ip);
                var endPoint = new IPEndPoint(ip, memberInfo.port + 1);
                var sender = new Socket(ip.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
                sender.Connect(endPoint);
                
                sender.SendBenchmarkControlMessage(config);
                var handlerThread = new Thread(() =>
                {
                    while (true)
                    {
                        var message = sender.ReceiveBenchmarkMessage();
                        if (message.type == 1)
                        {
                            if (setupFinished.Signal())
                                stopwatch.Start();
                            break;
                        }
                    }

                    setupFinished.Wait();
                    sender.SendBenchmarkControlMessage("start benchmark");

                    while (true)
                    {
                        var message = sender.ReceiveBenchmarkMessage();
                        if (message == null) break;
                        if (message.type == 1)
                        {
                            var ops = (long) message.content;
                            lock (this)
                            {
                                totalOps += ops;
                            }
                            clientCountdown.Signal();
                            shutdown.Wait();
                            sender.SendBenchmarkControlMessage("shutdown");
                        }
                    }
                    sender.Close();
                });
                handlerThreads.Add(handlerThread);
                handlerThread.Start();
            }
            
            foreach (var memberInfo in clusterConfig.proxies)
            {
                var ip = IPAddress.Parse(memberInfo.ip);
                var endPoint = new IPEndPoint(ip, memberInfo.port + 1);
                var sender = new Socket(ip.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
                sender.Connect(endPoint);

                sender.SendBenchmarkControlMessage(config);
                var handlerThread = new Thread(() =>
                {
                    while (true)
                    {
                        var message = sender.ReceiveBenchmarkMessage();
                        if (message.type == 1)
                        {
                            if (setupFinished.Signal())
                                stopwatch.Start();
                            break;
                        }
                    }

                    setupFinished.Wait();
                    sender.SendBenchmarkControlMessage("start benchmark");
                    
                    shutdown.Wait();
                    sender.SendBenchmarkControlMessage("shutdown");
                    sender.Close();
                });
                handlerThreads.Add(handlerThread);
                handlerThread.Start();
            }
            
            clientCountdown.Wait();
            stopwatch.Stop();
            shutdown.Set();
            foreach (var thread in handlerThreads)
                thread.Join();
            Console.WriteLine((double) totalOps / stopwatch.ElapsedMilliseconds);
        }
    }
}