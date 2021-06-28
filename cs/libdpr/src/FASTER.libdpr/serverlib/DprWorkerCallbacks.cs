
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
        /// Invoked when a version is persistent.
        /// </summary>
        /// <param name="version">Version number of the finished version</param>
        /// <param name="token">unique token that identifies the checkpoint associated with the version</param>
        public void OnVersionPersistent(long version)
        {
            var deps = state.versions[version];
            var workerVersion = new WorkerVersion(state.me, version);
            state.dprFinder.ReportNewPersistentVersion(workerVersion, deps);
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