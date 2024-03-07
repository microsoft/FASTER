using System;
using System.Collections.Generic;
using System.Net;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using FASTER.core;
using FASTER.libdpr;

namespace DprCounters
{
    class Program
    {
        public static async Task Main(string[] args)
        {
            // Use a simple pair of in-memory storage to back our DprFinder server for now. Start a local DPRFinder
            // server for the cluster
            var localDevice1 = new LocalMemoryDevice(1 << 20, 1 << 20, 1);
            var localDevice2 = new LocalMemoryDevice(1 << 20, 1 << 20, 1);
            var device = new PingPongDevice(localDevice1, localDevice2);
            using var dprFinderServer = new RespGraphDprFinderServer("127.0.0.1", 15721, new GraphDprFinderBackend(device));
            dprFinderServer.StartServer();
            
            // Start two counter servers
            var cluster = new Dictionary<WorkerId, IPEndPoint>();

            var w0 = new WorkerId(0);
            cluster.Add(w0, new IPEndPoint(IPAddress.Parse("127.0.0.1"), 15722));
            var w0Server = new CounterServer("127.0.0.1", 15722, new WorkerId(0), "worker0/",
                new RespGraphDprFinder("127.0.0.1", 15721));
            var w0Thread = new Thread(w0Server.RunServer);
            w0Thread.Start();


            var w1 = new WorkerId(1);
            cluster.Add(w1, new IPEndPoint(IPAddress.Parse("127.0.0.1"), 15723));
            var w1Server = new CounterServer("127.0.0.1", 15723, new WorkerId(1), "worker1/",
                new RespGraphDprFinder("127.0.0.1", 15721));
            var w1Thread = new Thread(w1Server.RunServer);
            w1Thread.Start();


            // Start a client that performs some operations
            var client = new CounterClient(new RespGraphDprFinder("127.0.0.1", 15721), cluster);
            var op0 = client.Increment(w0, 42, out _);
            var op1 = client.Increment(w1, 2, out _);
            var op2 = client.Increment(w1, 7, out _);
            var op3 = client.Increment(w0, 10, out _);
            await client.Committed(op3);

            // Shutdown
            w0Server.StopServer();
            w0Thread.Join();
            
            w1Server.StopServer();
            w1Thread.Join();
        }
    }
}