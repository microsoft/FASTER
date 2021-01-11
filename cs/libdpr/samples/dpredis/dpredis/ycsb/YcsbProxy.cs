using System;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using FASTER.libdpr;

namespace dpredis.ycsb
{

    public class YcsbProxy
    {
        private int workerId;

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
            var dprManager = new DprServer<RedisStateObject, long>(new TestDprFinder(config.dprFinderIP, config.dprFinderPort), me, redisBackend, config.checkpointMilli);
            var proxy = new DpredisProxy(info.ip, info.port, dprManager);
            proxy.StartServer();
            // TODO(Tianyu): Load initial keys into Redis here / make sure right checkpoint is loaded
            coordinatorConn.SendBenchmarkControlMessage("setup finished");
            coordinatorConn.ReceiveBenchmarkMessage();
            dprManager.Start();
            coordinatorConn.ReceiveBenchmarkMessage();
            dprManager.End();
            proxy.StopServer();
        }
    }
}