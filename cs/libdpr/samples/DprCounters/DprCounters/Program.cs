using System.Collections.Generic;
using System.Net;
using System.Security.Cryptography;
using System.Threading;
using FASTER.core;
using FASTER.libdpr;

namespace DprCounters
{
    class Program
    {
        static void Main(string[] args)
        {
            // Use a simple pair of in-memory storage to back our DprFinder server for now. Start a local DPRFinder
            // server for the cluster
            var localDevice1 = new LocalMemoryDevice(1 << 20, 1 << 20, 1);
            var localDevice2 = new LocalMemoryDevice(1 << 20, 1 << 20, 1);
            var device = new PingPongDevice(localDevice1, localDevice2);
            using var dprFinderServer = new GraphDprFinderServer("localhost", 15721, new GraphDprFinderBackend(device));
            dprFinderServer.StartServer();
            
            // Start two counter servers
            var cluster = new Dictionary<Worker, IPEndPoint>();

            var w0 = new Worker(0);
            cluster.Add(w0, new IPEndPoint(IPAddress.Parse("localhost"), 15722));
            var w0Server = new CounterServer("localhost", 15722, new Worker(0), "worker0/",
                new GraphDprFinder("localhost", 15721));
            var w0Thread = new Thread(w0Server.RunServer);
            w0Thread.Start();

            var w1 = new Worker(1);
            cluster.Add(w1, new IPEndPoint(IPAddress.Parse("localhost"), 15723));
            var w1Server = new CounterServer("localhost", 15723, new Worker(1), "worker1/",
                new GraphDprFinder("localhost", 15721));
            var w1Thread = new Thread(w1Server.RunServer);
            w1Thread.Start();

            // Start a client that performs some operations
            var client = new CounterClient(new GraphDprFinder("localhost", 15721), cluster);
            var session = client.GetSession();
            var op0 = session.Increment(w0, 42, out _);
            var op1 = session.Increment(w1, 2, out _);
            var op2 = session.Increment(w1, 7, out _);
            var op3 = session.Increment(w0, 10, out _);
            while (!session.Committed(op3))
                client.RefreshDpr();


            // Shutdown
            w0Server.StopServer();
            w0Thread.Join();
            
            w1Server.StopServer();
            w1Thread.Join();
        }
    }
}