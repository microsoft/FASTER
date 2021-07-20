using System.Collections.Generic;

namespace FASTER.libdpr
{
    public class GlobalMinDprStateSnapshot : IDprStateSnapshot
    {
        private readonly long globalSafeVersion;

        public GlobalMinDprStateSnapshot(long globalSafeVersion)
        {
            this.globalSafeVersion = globalSafeVersion;
        }

        public long SafeVersion(Worker worker)
        {
            return globalSafeVersion;
        }
    }

    /// <summary>
    ///     A DprStateSnapshot backed by a dictionary mapping from worker to version
    /// </summary>
    public class DictionaryDprStateSnapshot : IDprStateSnapshot
    {
        private readonly Dictionary<Worker, long> dprTableSnapshot;

        /// <summary>
        ///     Constructs a new DprStateSnapshot backed by the given dictionary
        /// </summary>
        /// <param name="dprTableSnapshot"> dictionary that encodes the DPR state </param>
        public DictionaryDprStateSnapshot(Dictionary<Worker, long> dprTableSnapshot)
        {
            this.dprTableSnapshot = dprTableSnapshot;
        }

        /// <inheritdoc />
        public long SafeVersion(Worker worker)
        {
            if (dprTableSnapshot == null) return 0;

            lock (dprTableSnapshot)
            {
                return !dprTableSnapshot.TryGetValue(worker, out var safeVersion) ? 0 : safeVersion;
            }
        }
    }
}