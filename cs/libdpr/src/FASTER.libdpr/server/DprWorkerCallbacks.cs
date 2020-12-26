
namespace FASTER.libdpr
{
    public class DprWorkerCallbacks<TToken>
    {
        private readonly DprWorkerState<TToken> state;

        internal DprWorkerCallbacks(DprWorkerState<TToken> state)
        {
            this.state = state;
        }

        /// <summary>
        /// Invoked when a version has been finalized --- i.e., no more operations can be a part of this version.
        /// </summary>
        /// <param name="version">Version number of the finished version</param>
        /// <param name="token">unique token that identifies the checkpoint associated with the version</param>
        public void OnVersionEnd(long version, TToken token)
        {
            // Get or Add as some operations may have executed early in the next version, which would lead to a created
            // entry for the version without a token.
            var versionHandle = state.versions.GetOrAdd(version, version => new VersionHandle<TToken>());
            versionHandle.token = token;
        }

        /// <summary>
        /// Invoked when a version is persistent.
        /// </summary>
        /// <param name="version">Version number of the finished version</param>
        /// <param name="token">unique token that identifies the checkpoint associated with the version</param>
        public void OnVersionPersistent(long version, TToken token)
        {
            var versionObject = state.versions[version];
            versionObject.token = token;
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
            state.epoch.BumpCurrentEpoch(() => state.nextWorldLine++);
        }
    }
}