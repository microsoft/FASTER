using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using FASTER.core;
using NUnit.Framework;
using NUnit.Framework.Internal.Execution;

namespace FASTER.libdpr
{
    internal class SimulatedWorker
    {
        private Worker me;
        private List<SimulatedWorker> cluster;
        private long version, lastChecked;
        private Dictionary<WorkerVersion, List<WorkerVersion>> versions;
        private Func<GraphDprFinderBackend> backend;
        private double depProb;
        private Random random;
        private bool finished = false;

        public SimulatedWorker(Worker me, List<SimulatedWorker> cluster, Func<GraphDprFinderBackend> backend, double depProb)
        {
            this.me = me;
            version = 1;
            lastChecked = 0;
            versions = new Dictionary<WorkerVersion, List<WorkerVersion>>();
            this.backend = backend;
            this.depProb = depProb;
            this.cluster = cluster;
            random = new Random();
            backend().AddWorker(me, null);
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
                        backend().NewCheckpoint(wv, deps);
                        version = worker.version;
                        return;
                    }
                    deps.Add(new WorkerVersion(worker.me, depVersion));
                }
            }
            backend().NewCheckpoint(wv, deps);
            Interlocked.Increment(ref version);
        }
        
        private void CheckInvariants()
        {
            var (bytes, size) = backend().GetPersistentState();
            var deserializedState =
                size == 0 ? new GraphDprFinderBackend.State() : new GraphDprFinderBackend.State(bytes, 0);
            var persistedUntil = deserializedState.GetCurrentCut()[me];
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
        

        public void Simulate(long timeMilli)
        {
            var sw = Stopwatch.StartNew();
            while (sw.ElapsedMilliseconds < timeMilli)
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
        private ManualResetEventSlim termination;
        private IDevice frontDevice, backDevice;
        // Randomly reset to simulate DprFinder failure
        private volatile GraphDprFinderBackend backend;
        private Thread failOver, compute, persist;

        public SimulatedDprFinder(IDevice frontDevice, IDevice backDevice)
        {
            this.frontDevice = frontDevice;
            this.backDevice = backDevice;
            backend = new GraphDprFinderBackend(new PingPongDevice(frontDevice, backDevice));
        }

        public GraphDprFinderBackend GetDprFinder() => backend;

        public void FinishSimulation()
        {
            termination.Set();
            failOver.Join();
            compute.Join();
            persist.Join();
            frontDevice.Dispose();
            backDevice.Dispose();
        }

        public void StartSimulation(double failureProb)
        {
            termination = new ManualResetEventSlim();
            failOver = new Thread(() =>
            {
                var rand = new Random();   
                while (!termination.IsSet)
                {
                    Thread.Sleep(10);
                    if (rand.NextDouble() < failureProb)
                        backend = new GraphDprFinderBackend(new PingPongDevice(frontDevice, backDevice));
                }
            });
            compute = new Thread(() =>
            {
                while (!termination.IsSet)
                    backend.TryFindDprCut();
            });
        
            persist = new Thread(() =>
            {
                while (!termination.IsSet)
                    backend.PersistState();
            });

            persist.Start();
            compute.Start();
            failOver.Start();
        }
    }
}