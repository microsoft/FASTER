using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using NUnit.Framework.Internal.Commands;
using static System.Int64;

namespace FASTER.libdpr
{
    public class SimpleTestDprFinderBackend
    {
        private ConcurrentDictionary<Worker, long> versions = new ConcurrentDictionary<Worker, long>();

        public SimpleTestDprFinderBackend(int clusterSize)
        {
            for (var i = 0; i < clusterSize; i++)
                versions[new Worker(i)] = 0;
        }
        
        public void Update(Worker worker, long version) => versions[worker] = version;

        public (long, long) ComputeCut()
        {
            long max = MinValue, min = MaxValue;
            foreach (var version in versions)
            {
                max = Math.Max(max, version.Value);
                min = Math.Min(min, version.Value);
            }

            return ValueTuple.Create(min, max);
        }
    }
    // Simple single-threaded server
    public class SimpleTestDprFinder : IDprFinder
    {
        // cached local value
        private readonly Worker me;
        private long globalSafeVersionNum = 0, globalMaxVersionNum = 1;
        private long workerVersion = 0;
        private SimpleTestDprFinderBackend backend;

        public SimpleTestDprFinder(Worker me, SimpleTestDprFinderBackend backend)
        {
            this.me = me;
            this.backend = backend;
        }

        public void ReportRecovery(long worldLine, WorkerVersion latestRecoveredVersion)
        {
            throw new NotImplementedException();
        }


        public long SafeVersion(Worker worker)
        {
            return globalSafeVersionNum;
        }

        public long GlobalMaxVersion()
        {
            return globalMaxVersionNum;
        }

        public long SystemWorldLine()
        {
            return 0;
        }

        public IDprStateSnapshot GetStateSnapshot()
        {
            return new GlobalMinDprStateSnapshot(globalSafeVersionNum);
        }

        public void ReportNewPersistentVersion(WorkerVersion persisted, IEnumerable<WorkerVersion> deps)
        {
            workerVersion = persisted.Version;
            backend.Update(me, persisted.Version);
        }

        public void Refresh()
        {
            var (newMin, newMax) = backend.ComputeCut();
            globalMaxVersionNum = newMax;
            globalSafeVersionNum = newMin;
        }
    }
}