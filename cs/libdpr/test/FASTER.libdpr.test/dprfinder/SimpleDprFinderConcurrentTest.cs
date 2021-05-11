using System.Collections.Generic;
using System.Threading;
using FASTER.core;
using NUnit.Framework;

namespace FASTER.libdpr
{

    [TestFixture]
    public class SimpleDprFinderConcurrentTest
    {
        [Test]
        public void ConcurrentTestDprFinderSmallNoFailure()
        {
            var localDevice1 = new LocalMemoryDevice(1 << 20, 1 << 20, 1);
            var localDevice2 = new LocalMemoryDevice(1 << 20, 1 << 20, 1);
            var tested = new SimulatedDprFinder(localDevice1, localDevice2);
            var simulationEnd = new ManualResetEventSlim();
            var cluster = new List<SimulatedWorker>();
            for (var i = 0; i < 3; i++)
                cluster.Add(new SimulatedWorker(new Worker(i), cluster, tested.GetDprFinder, 0.5));

            tested.StartSimulation(0.0, simulationEnd);
            var threads = new List<Thread>();
            foreach (var worker in cluster)
            {
                var t = new Thread(() => worker.Simulate(1000));
                threads.Add(t);
                t.Start();
            }

            foreach (var t in threads)
                t.Join();

            tested.FinishSimulation();
        }
        
        [Test]
        public void ConcurrentTestDprFinderLargeNoFailure()
        {
            var localDevice1 = new LocalMemoryDevice(1 << 20, 1 << 20, 1);
            var localDevice2 = new LocalMemoryDevice(1 << 20, 1 << 20, 1);
            var tested = new SimulatedDprFinder(localDevice1, localDevice2);
            var simulationEnd = new ManualResetEventSlim();
            var cluster = new List<SimulatedWorker>();
            for (var i = 0; i < 100; i++)
                cluster.Add(new SimulatedWorker(new Worker(i), cluster, tested.GetDprFinder, 0.75));

            tested.StartSimulation(0.0, simulationEnd);
            var threads = new List<Thread>();
            foreach (var worker in cluster)
            {
                var t = new Thread(() => worker.Simulate(100000));
                threads.Add(t);
                t.Start();
            }

            foreach (var t in threads)
                t.Join();

            tested.FinishSimulation();
        }
        
        [Test]
        public void ConcurrentTestDprFinderFailure()
        {
            var localDevice1 = new LocalMemoryDevice(1 << 20, 1 << 20, 1);
            var localDevice2 = new LocalMemoryDevice(1 << 20, 1 << 20, 1);
            var tested = new SimulatedDprFinder(localDevice1, localDevice2);
            var simulationEnd = new ManualResetEventSlim();
            var cluster = new List<SimulatedWorker>();
            for (var i = 0; i < 10; i++)
                cluster.Add(new SimulatedWorker(new Worker(i), cluster, tested.GetDprFinder, 0.75));

            tested.StartSimulation(0.05, simulationEnd);
            var threads = new List<Thread>();
            foreach (var worker in cluster)
            {
                var t = new Thread(() => worker.Simulate(1000));
                threads.Add(t);
                t.Start();
            }

            foreach (var t in threads)
                t.Join();

            tested.FinishSimulation();
        }
    }
}