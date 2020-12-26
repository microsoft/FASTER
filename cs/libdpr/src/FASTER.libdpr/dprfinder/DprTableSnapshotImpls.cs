using System;
using System.Collections.Generic;


namespace FASTER.libdpr
{
    public class V1DprTableSnapshot: IDprTableSnapshot
    {
        private readonly long globalSafeVersion;

        public V1DprTableSnapshot(long globalSafeVersion)
        {
            this.globalSafeVersion = globalSafeVersion;
        }
        
        public long SafeVersion(Worker worker) => globalSafeVersion;
    }

    public class V2DprTableSnapshot: IDprTableSnapshot
    {
        private readonly Dictionary<Worker, long> cprTableSnapshot;
        private readonly long globalSafeVersion;

        public V2DprTableSnapshot(long globalSafeVersion, Dictionary<Worker, long> cprTableSnapshot)
        {
            this.cprTableSnapshot = cprTableSnapshot;
            this.globalSafeVersion = globalSafeVersion;
        }

        public long SafeVersion(Worker worker)
        {
            if (!cprTableSnapshot.TryGetValue(worker, out var tableValue))
                tableValue = 0;
            return Math.Max(globalSafeVersion, tableValue);
        }
    }

    public class V3DprTableSnapshot : IDprTableSnapshot
    {
        private readonly Dictionary<Worker, long> cprTableSnapshot;

        public V3DprTableSnapshot(Dictionary<Worker, long> cprTableSnapshot)
        {
            this.cprTableSnapshot = cprTableSnapshot;
        }

        public long SafeVersion(Worker worker)
        {
            return !cprTableSnapshot.TryGetValue(worker, out var safeVersion) ? 0 : safeVersion;
        }
        
    }
}