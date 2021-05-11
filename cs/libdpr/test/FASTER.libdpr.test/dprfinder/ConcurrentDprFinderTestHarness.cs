using System;
using System.Collections.Generic;
using System.Diagnostics;
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
        private Func<SimpleDprFinderBackend> backend;
        private double depProb;
        private Random random;

        public SimulatedWorker(Worker me, List<SimulatedWorker> cluster, Func<SimpleDprFinderBackend> backend, double depProb)
        {
            this.me = me;
            version = 0;
            lastChecked = 0;
            versions = new Dictionary<WorkerVersion, List<WorkerVersion>>();
            this.backend = backend;
            this.depProb = depProb;
            this.cluster = cluster;
            random = new Random();
        }

        private void SimulateOnVersion(bool generateDeps = true)
        {
            Thread.Sleep(random.Next(10, 20));
            var workerVersion = new WorkerVersion(me, version + 1);
            var deps = new List<WorkerVersion>();
            if (generateDeps)
            {
                for (var i = 0; i < cluster.Count; i++)
                {
                    if (me.guid == i) continue;
                    if (random.NextDouble() < depProb)
                        deps.Add(new WorkerVersion(new Worker(i), cluster[i].version + 1));
                }
            }

            versions[workerVersion] = deps;
        }
        
        private void CheckInvariants()
        {
            var (bytes, size) = backend().GetPersistentState();
            var deserializedState =
                size == 0 ? new SimpleDprFinderBackend.State() : new SimpleDprFinderBackend.State(bytes, 0);
            var persistedUntil = deserializedState.GetCurrentCut()[me];
            // Guarantees should never regress, even if backend failed
            Assert.GreaterOrEqual(persistedUntil, lastChecked);
            // Check that all committed versions have persistent dependencies
            for (var v = lastChecked; v <= persistedUntil; v++)
            {
                foreach (var dep in versions[new WorkerVersion(me, v)])
                    Assert.GreaterOrEqual(dep.Version,  cluster[(int) dep.Worker.guid].version);
            }

            lastChecked = persistedUntil;
        }
        
        private void CommitVersion()
        {
            Interlocked.Increment(ref version);
            var wv = new WorkerVersion(me, version);
            backend().NewCheckpoint(wv, versions[wv]);
        }

        public void Simulate(long timeMilli)
        {
            var sw = Stopwatch.StartNew();
            while (sw.ElapsedMilliseconds < timeMilli)
            {
                SimulateOnVersion();
                CheckInvariants();
                CommitVersion();
            }
            SimulateOnVersion(false);
            CommitVersion();
            while (lastChecked != version)
                CheckInvariants();

        }
    }

    internal class SimulatedDprFinder
    {
        private ManualResetEventSlim terminate;
        private IDevice frontDevice, backDevice;
        // Randomly reset to simulate DprFinder failure
        private volatile SimpleDprFinderBackend backend;
        private Thread failOver, compute, persist;

        public SimulatedDprFinder(IDevice frontDevice, IDevice backDevice)
        {
            this.frontDevice = frontDevice;
            this.backDevice = backDevice;
        }

        public SimpleDprFinderBackend GetDprFinder() => backend;

        public void FinishSimulation()
        {
            failOver.Join();
            compute.Join();
            persist.Join();
        }

        public void StartSimulation(double failureProb, ManualResetEventSlim simulationEnd)
        {
            while (!simulationEnd.IsSet)
            {
                backend = new SimpleDprFinderBackend(new PingPongDevice(frontDevice, backDevice));
                terminate = new ManualResetEventSlim();
                failOver = new Thread(() =>
                {
                    var rand = new Random();   
                    while (!simulationEnd.IsSet)
                    {
                        Thread.Sleep(10);
                        if (rand.NextDouble() < failureProb)
                            terminate.Set();
                    }
                });
                compute = new Thread(() =>
                {
                    while (!terminate.IsSet && !simulationEnd.IsSet)
                    {
                        backend.TryFindDprCut();
                    }
                });
            
                persist = new Thread(() =>
                {
                    while (!terminate.IsSet && !simulationEnd.IsSet)
                    {
                        backend.PersistState();
                    }
                });

                persist.Start();
                compute.Start();
                failOver.Start();
            }
        }
    }
}