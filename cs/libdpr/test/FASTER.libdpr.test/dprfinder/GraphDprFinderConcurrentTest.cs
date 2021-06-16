using System.Collections.Generic;
using System.Threading;
using FASTER.core;
using NUnit.Framework;

namespace FASTER.libdpr
{

    [TestFixture]
    public class GraphDprFinderConcurrentTest
    {
        [Test]
        public void ConcurrentTestDprFinderSmallNoFailure()
        {
            var localDevice1 = new LocalMemoryDevice(1 << 20, 1 << 20, 1);
            var localDevice2 = new LocalMemoryDevice(1 << 20, 1 << 20, 1);
            var tested = new SimulatedDprFinder(localDevice1, localDevice2);
            var cluster = new List<SimulatedWorker>();
            for (var i = 0; i < 3; i++)
                cluster.Add(new SimulatedWorker(new Worker(i), cluster, tested.GetDprFinder, 0.5));
            tested.GetDprFinder().PersistState();
            tested.Simulate(0.0, 1000, cluster);
        }
        
        [Test]
        public void ConcurrentTestDprFinderLargeNoFailure()
        {
            var localDevice1 = new LocalMemoryDevice(1 << 20, 1 << 20, 1);
            var localDevice2 = new LocalMemoryDevice(1 << 20, 1 << 20, 1);
            var tested = new SimulatedDprFinder(localDevice1, localDevice2);
            var cluster = new List<SimulatedWorker>();
            for (var i = 0; i < 30; i++)
                cluster.Add(new SimulatedWorker(new Worker(i), cluster, tested.GetDprFinder, 0.75));
            tested.GetDprFinder().PersistState();
            tested.Simulate(0.0, 30000, cluster);
        }
        
        [Test]
        public void ConcurrentTestDprFinderFailure()
        {
            var localDevice1 = new LocalMemoryDevice(1 << 20, 1 << 20, 1);
            var localDevice2 = new LocalMemoryDevice(1 << 20, 1 << 20, 1);
            var tested = new SimulatedDprFinder(localDevice1, localDevice2);
            var cluster = new List<SimulatedWorker>();
            for (var i = 0; i < 10; i++)
                cluster.Add(new SimulatedWorker(new Worker(i), cluster, tested.GetDprFinder, 0.75));
            tested.GetDprFinder().PersistState();
            tested.Simulate(0.05, 1000, cluster);
        }
    }
}