
namespace FASTER.libdpr
{
    /// <summary>
    /// A set of callback functions that libdpr expects the underlying implementation to invoke at certain points of
    /// the checkpoint/recovery process. 
    /// </summary>
    /// <typeparam name="TToken">Type of token that uniquely identifies a checkpoint</typeparam>
    public class DprWorkerCallbacks
    {
        private readonly DprWorkerState state;

        internal DprWorkerCallbacks(DprWorkerState state)
        {
            this.state = state;
        }

        /// <summary>
        /// Invoked when a version has been finalized --- i.e., no more operations can be a part of this version.
        /// </summary>
        /// <param name="version">Version number of the finished version</param>
        /// <param name="token">unique token that identifies the checkpoint associated with the version</param>
        public void OnVersionEnd(long oldVersion)
        {
            // Get or Add as some operations may have executed early in the next version, which would lead to a created
            // entry for the version without a token.
            var versionHandle = state.versions.GetOrAdd(oldVersion, v => new VersionHandle());
        }

        /// <summary>
        /// Invoked when a version is persistent.
        /// </summary>
        /// <param name="version">Version number of the finished version</param>
        /// <param name="token">unique token that identifies the checkpoint associated with the version</param>
        public void OnVersionPersistent(long version)
        {
            var versionObject = state.versions[version];
            var workerVersion = new WorkerVersion(state.me, version);
            // TODO(Tianyu): For performance, change IDprFinder code to only buffer this write and invoke expensive
            // writes on refreshes.
            state.dprFinder.ReportNewPersistentVersion(workerVersion, versionObject.deps);
        }

        /// <summary>
        /// Invoked when rollback is complete. There is no need for any identifier because libDPR always issues rollbacks
        /// one-at-a-time without only one outstanding rollback.
        /// </summary>
        public void OnRollbackComplete()
        {
            state.rollbackProgress.Set();
        }
    }
}