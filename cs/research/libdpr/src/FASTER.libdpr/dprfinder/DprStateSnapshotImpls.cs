using System.Collections.Generic;

namespace FASTER.libdpr
{
    /// <summary>
    ///     A DprStateSnapshot backed by a dictionary mapping from worker to version
    /// </summary>
    public class DictionaryDprStateSnapshot : IDprStateSnapshot
    {
        private readonly Dictionary<WorkerId, long> dprTableSnapshot;
        private readonly long worldLine;

        /// <summary>
        ///     Constructs a new DprStateSnapshot backed by the given dictionary
        /// </summary>
        /// <param name="dprTableSnapshot"> dictionary that encodes the DPR state </param>
        public DictionaryDprStateSnapshot(Dictionary<WorkerId, long> dprTableSnapshot, long worldLine)
        {
            this.dprTableSnapshot = dprTableSnapshot;
            this.worldLine = worldLine;
        }

        /// <inheritdoc />
        public long SafeVersion(WorkerId workerId)
        {
            if (dprTableSnapshot == null) return 0;

            lock (dprTableSnapshot)
            {
                return !dprTableSnapshot.TryGetValue(workerId, out var safeVersion) ? 0 : safeVersion;
            }
        }
        
        /// <inheritdoc />
        public long SystemWorldLine() => worldLine;
    }
}