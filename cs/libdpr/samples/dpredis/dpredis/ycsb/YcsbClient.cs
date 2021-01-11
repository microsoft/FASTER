using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
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

        private void Execute(BenchmarkConfiguration config, Socket coordinatorConn)
        {
            var dprFinder = new TestDprFinder(config.dprFinderIP, config.dprFinderPort);
            var routingTable = new ConcurrentDictionary<Worker, (string, int)>();
            foreach (var worker in YcsbCoordinator.clusterConfig.workers)
            {
                if (worker.type == WorkerType.CLIENT) continue;
                routingTable.TryAdd(new Worker(worker.id), ValueTuple.Create(worker.ip, worker.port));
            }
            // TODO(Tianyu): Load in Ycsb txn keys here
            coordinatorConn.SendBenchmarkControlMessage("setup finished");
            coordinatorConn.ReceiveBenchmarkMessage();
            // TODO(Tianyu): Replace with real workload
            var client = new DpredisClient(dprFinder, routingTable);
            var clientSession = client.NewSession(config.batchSize);
            var res = clientSession.IssueCommand(new Worker(0), "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
            clientSession.FlushAll();
            Console.WriteLine(res.Result.Item2);
            res = clientSession.IssueCommand(new Worker(0), "*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n");
            clientSession.FlushAll();
            Console.WriteLine(res.Result.Item2);
            // Signal completion
            coordinatorConn.SendBenchmarkControlMessage(0.0);
            coordinatorConn.ReceiveBenchmarkMessage();
            dprFinder.Clear();
            client.End();
        }
    }
}