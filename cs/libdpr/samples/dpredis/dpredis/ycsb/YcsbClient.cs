using System;
using System.Collections.Concurrent;
using FASTER.libdpr;

namespace dpredis.ycsb
{
    public class YcsbClient
    {
        private int workerId;

        public YcsbClient(int workerId)
        {
            this.workerId = workerId;
        }

        public void Run(BenchmarkConfiguration config)
        {
            var dprFinder = new TestDprFinder(config.dprFinderIP, config.dprFinderPort);
            var routingTable = new ConcurrentDictionary<Worker, (string, int)>();
            foreach (var worker in YcsbCoordinator.clusterConfig.workers)
            {
                if (worker.type == WorkerType.CLIENT) continue;
                routingTable.TryAdd(new Worker(worker.id), ValueTuple.Create(worker.ip, worker.port));
            }

            var client = new DpredisClient(dprFinder, routingTable);
            var clientSession = client.NewSession(config.batchSize);
            var res = clientSession.IssueCommand(new Worker(0), "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
            clientSession.FlushAll();
            Console.WriteLine(res.Result.Item2);
            res = clientSession.IssueCommand(new Worker(0), "*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n");
            clientSession.FlushAll();
            Console.WriteLine(res.Result.Item2);
        }
    }
}