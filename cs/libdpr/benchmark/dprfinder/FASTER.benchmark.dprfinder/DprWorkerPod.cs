using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using DprMicrobench;
using FASTER.libdpr;


namespace FASTER.benchmark
{
    public class DprWorkerPod
    {
        internal int workerId;
        internal List<SimulatedDprWorker> simulatedWorkers;

        public DprWorkerPod(int workerId)
        {
            this.workerId = workerId;
            simulatedWorkers = new List<SimulatedDprWorker>();
        }

        private IDprFinder GetDprFinder(BenchmarkConfiguration config)
        {
            var coordinatorIp = DprCoordinator.clusterConfig.coordinatorIp;
            var coordinatorPort = DprCoordinator.clusterConfig.coordinatorPort;
            if (config.dprType.Equals("basic"))
                return new GraphDprFinder(coordinatorIp, coordinatorPort);
            if (config.dprType.Equals("enhanced"))
                return new EnhancedDprFinder(coordinatorIp, coordinatorPort);
            throw new Exception("Unrecognized argument");
        }

        private IWorkloadGenerator GetWorkloadGenerator(BenchmarkConfiguration config)
        {
            if (config.heavyHitterProb == 0.0)
            {
                return new UniformWorkloadGenerator(config.depProb);
            }
            return new SkewedWorkloadGenerator(config.depProb, config.heavyHitterProb, new Worker(0));
        }

        public void Run()
        {
            var info = DprCoordinator.clusterConfig.GetInfoForId(workerId);
            var addr = IPAddress.Parse(info.ip);
            var servSock = new Socket(addr.AddressFamily,
                SocketType.Stream, ProtocolType.Tcp);
            var local = new IPEndPoint(addr, info.port + 1);
            servSock.Bind(local);
            servSock.Listen(128);

            var clientSocket = servSock.Accept();
            var message = clientSocket.ReceiveBenchmarkMessage();
            Debug.Assert(message.type == 1);
            var config = (BenchmarkConfiguration) message.content;
            var threads = new List<Thread>();
            var startSignal = new ManualResetEventSlim();
            foreach (var worker in config.assignment[workerId])
            {
                var simulatedWorker = new SimulatedDprWorker(GetDprFinder(config),
                    GetWorkloadGenerator(config), config.workers, worker, config.delayProb);
                simulatedWorkers.Add(simulatedWorker);
                var thread = new Thread(() =>
                {
                    startSignal.Wait();
                    simulatedWorker.RunContinuously(config.runSeconds, config.averageMilli, config.delayMilli);
                });
                threads.Add(thread);
                thread.Start();
            }
            clientSocket.SendBenchmarkControlMessage("ready");

            var startTime = (DateTimeOffset) clientSocket.ReceiveBenchmarkMessage().content;
            SpinWait.SpinUntil(() => DateTimeOffset.UtcNow < startTime);
            startSignal.Set();
            foreach (var thread in threads)
                thread.Join();

            var result = new List<List<long>>();
            foreach (var worker in simulatedWorkers)
                result.Add(worker.ComputeVersionCommitLatencies());

            clientSocket.SendBenchmarkControlMessage(result);
            clientSocket.Close();
        }
    }
}