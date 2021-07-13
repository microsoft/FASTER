using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using FASTER.libdpr;

namespace DprMicrobench
{
    public class SimulatedDprWorker
    {
        private IDprFinder dprFinder;
        private IWorkloadGenerator generator;
        private IList<Worker> workers;
        private Worker me;
        private double delayProb;
        private Random random = new Random();

        private Dictionary<long, long> versionPersistent, versionRecoverable;
        private Stopwatch stopwatch;
        
        public SimulatedDprWorker(IDprFinder dprFinder, IWorkloadGenerator generator, 
            IList<Worker> workers, Worker me,
            double delayProb)
        {
            var v= dprFinder.NewWorker(me, null);
            Debug.Assert(v == 0);
            this.dprFinder = dprFinder;
            this.generator = generator;
            this.workers = workers;
            this.me = me;
            this.delayProb = delayProb;
        }

        public List<long> ComputeVersionCommitLatencies()
        {
            var result = new List<long>(versionRecoverable.Count);

            foreach (var entry in versionRecoverable)
            {
                if (!versionPersistent.TryGetValue(entry.Key, out var startTime)) continue;
                result.Add(entry.Value - startTime);
            }

            return result;
        }

        public void RunContinuously(int runSeconds, int averageMilli, int delayMilli)
        {
            versionPersistent = new Dictionary<long, long>(runSeconds * 1000 / averageMilli);
            versionRecoverable = new Dictionary<long, long>(runSeconds * 1000 / averageMilli);
            stopwatch = new Stopwatch();
            stopwatch.Start();
            var currentVersion = 1L;
            var safeVersion = 0L;
            while (stopwatch.ElapsedMilliseconds < runSeconds * 1000)
            {
                var elapsed = stopwatch.ElapsedMilliseconds;
                if (elapsed > runSeconds * 1000) break;
                
                dprFinder.Refresh();
                var currentSafeVersion = dprFinder.SafeVersion(me);
                for (var i = safeVersion + 1; i <= currentSafeVersion; i++)
                {
                    versionRecoverable.Add(i, elapsed);
                }

                safeVersion = currentSafeVersion;
                
                var expectedVersion = 1 + elapsed / averageMilli;
                if (expectedVersion > currentVersion)
                {
                    var deps = generator.GenerateDependenciesOneRun(workers, me, currentVersion);
                    // if (random.NextDouble() < delayProb)
                    //     Thread.Sleep(delayMilli);
                    elapsed = stopwatch.ElapsedMilliseconds;
                    versionPersistent.Add(currentVersion, elapsed);
                    dprFinder.ReportNewPersistentVersion(1, new WorkerVersion(me, currentVersion), deps);
                    currentVersion = expectedVersion;
                }
            }

            stopwatch.Stop();
        }
    }
}