namespace FASTER.libdpr
{
    // TODO(Tianyu): Documentation
    public class GlobalMinDprStateSnapshot: IDprStateSnapshot
    {
        private readonly long globalSafeVersion;

        public GlobalMinDprStateSnapshot(long globalSafeVersion)
        {
            this.globalSafeVersion = globalSafeVersion;
        }
        
        public long SafeVersion(Worker worker) => globalSafeVersion;
    }

}