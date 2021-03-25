using System.Collections.Generic;

namespace FASTER.libdpr
{
    public class GlobalMinDprTableSnapshot: IDprTableSnapshot
    {
        private readonly long globalSafeVersion;

        public GlobalMinDprTableSnapshot(long globalSafeVersion)
        {
            this.globalSafeVersion = globalSafeVersion;
        }
        
        public long SafeVersion(Worker worker) => globalSafeVersion;
    }

    public class ExactDprTableSnapshot : IDprTableSnapshot
    {
        private readonly Dictionary<Worker, long> dprTableSnapshot;

        public ExactDprTableSnapshot(Dictionary<Worker, long> cprTableSnapshot)
        {
            this.dprTableSnapshot = cprTableSnapshot;
        }

        public long SafeVersion(Worker worker)
        {
            return !dprTableSnapshot.TryGetValue(worker, out var safeVersion) ? 0 : safeVersion;
        }
        
    }}