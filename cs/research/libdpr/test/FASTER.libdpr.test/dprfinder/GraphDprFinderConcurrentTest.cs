using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using FASTER.core;
using NUnit.Framework;

namespace FASTER.libdpr
{
    internal class SimulatedWorker
    {
        private Worker me;
        private List<SimulatedWorker> cluster;
        private long worldLine, version, lastChecked;
        private Dictionary<WorkerVersion, List<WorkerVersion>> versions;
        private Func<GraphDprFinderBackend> backend;
        private double depProb;
        private Random random;
        private bool finished = false;

        public SimulatedWorker(Worker me, List<SimulatedWorker> cluster, Func<GraphDprFinderBackend> backend, double depProb)
        {
            this.me = me;
            worldLine = 1;
            version = 1;
            lastChecked = 0;
            versions = new Dictionary<WorkerVersion, List<WorkerVersion>>();
            this.backend = backend;
            this.depProb = depProb;
            this.cluster = cluster;
            random = new Random();
            backend().AddWorker(me);
        }

        private void SimulateOneVersion(bool generateDeps = true)
        {
            Thread.Sleep(random.Next(10, 20));
            var deps = new List<WorkerVersion>();
            var wv = new WorkerVersion(me, version);
            versions[wv] = deps;
            if (generateDeps)
            {
                for (var i = 0; i < cluster.Count; i++)
                {
                    var worker = cluster[random.Next(cluster.Count)]; 
                    var depVersion = Interlocked.Read(ref worker.version);
                    if (me.Equals(worker.me)) continue;
                    if (random.NextDouble() >= depProb || worker.finished)
                        continue;

                    if (depVersion > version)
                    {
                        // end version now
                        backend().NewCheckpoint(worldLine, wv, deps);
                        version = worker.version;
                        return;
                    }
                    deps.Add(new WorkerVersion(worker.me, depVersion));
                }
            }
            backend().NewCheckpoint(worldLine, wv, deps);
            Interlocked.Increment(ref version);
        }
        
        private void CheckInvariants()
        {
            var responseObject = backend().GetPrecomputedResponse();
            responseObject.rwLatch.EnterReadLock();
            var deserializedState = ClusterState.FromBuffer(responseObject.serializedResponse, 0, out var offset);
            if (BitConverter.ToInt32(responseObject.serializedResponse, offset) == -1)
            {
                responseObject.rwLatch.ExitReadLock();
                worldLine = deserializedState.currentWorldLine;
                version = deserializedState.worldLinePrefix[me];
                versions = versions.Where(kv => kv.Key.Version <= version && kv.Key.Version > lastChecked)
                    .ToDictionary(kv => kv.Key, kv => kv.Value);
                
                foreach (var (wv, deps) in versions)
                {
                    backend().NewCheckpoint(worldLine, wv, deps);
                }
                backend().MarkWorkerAccountedFor(me, lastChecked + 1);
                return;
            }

            var deserializedCut = new Dictionary<Worker, long>();
            RespUtil.ReadDictionaryFromBytes(responseObject.serializedResponse, offset, deserializedCut);
            responseObject.rwLatch.ExitReadLock();
            
            var persistedUntil = deserializedCut[me];
            // Guarantees should never regress, even if backend failed
            Assert.GreaterOrEqual(persistedUntil, lastChecked);
            // Check that all committed versions have persistent dependencies
            for (var v = lastChecked + 1; v <= persistedUntil; v++)
            {
                if (!versions.TryGetValue(new WorkerVersion(me, v), out var deps)) continue;
                foreach (var dep in deps)
                {
                    Assert.LessOrEqual(dep.Version,  cluster[(int) dep.Worker.guid].version);
                }
            }
            lastChecked = persistedUntil;
        }
        

        public void Simulate(ManualResetEventSlim termination)
        {
            while (!termination.IsSet)
            {
                SimulateOneVersion();
                CheckInvariants();
            }
            finished = true;
            var lastVersion = version;
            SimulateOneVersion(false);
            while (lastChecked < lastVersion)
                CheckInvariants();
        }
    }

    internal class SimulatedDprFinder
    {
        private IDevice frontDevice, backDevice;
        // Randomly reset to simulate DprFinder failure
        private volatile GraphDprFinderBackend backend;
        private Thread failOver, compute;

        public SimulatedDprFinder(IDevice frontDevice, IDevice backDevice)
        {
            this.frontDevice = frontDevice;
            this.backDevice = backDevice;
            backend = new GraphDprFinderBackend(new PingPongDevice(frontDevice, backDevice));
        }

        public GraphDprFinderBackend GetDprFinder() => backend;

        public void Simulate(double failureProb, int simulationTimeMilli, IEnumerable<SimulatedWorker> cluster)
        {
            var failOverTermination = new ManualResetEventSlim();
            var workerTermination = new ManualResetEventSlim();
            var backendTermination = new ManualResetEventSlim();
            failOver = new Thread(() =>
            {
                var rand = new Random();
                // failure simulator terminate before worker threads are joined so they can at least have one failure-free
                // version to ensure we make progress
                while (!failOverTermination.IsSet)
                {
                    Thread.Sleep(10);
                    if (rand.NextDouble() < failureProb)
                        backend = new GraphDprFinderBackend(new PingPongDevice(frontDevice, backDevice));
                }
            });
            compute = new Thread(() =>
            {
                while (!backendTermination.IsSet)
                    backend.Process();
            });
            compute.Start();
            failOver.Start();
            
            var threads = new List<Thread>();
            foreach (var worker in cluster)
            {
                var t = new Thread(() => worker.Simulate(workerTermination));
                threads.Add(t);
                t.Start();
            }

            Thread.Sleep(simulationTimeMilli);
            failOverTermination.Set();
            failOver.Join();
            
            workerTermination.Set();
            foreach (var t in threads)
                t.Join();

            backendTermination.Set();
            compute.Join();
            frontDevice.Dispose();
            backDevice.Dispose();
        }
    }

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
            tested.Simulate(0.05, 1000, cluster);
        }
    }
}