
using System.Diagnostics;

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
        /// Invoked when a new version is created. This method should be invoked and allowed to finish before any
        /// operation is issued to the new version and before future version is created.
        /// </summary>
        /// <param name="version"></param>
        /// <param name="previousVersion"></param>
        public void BeforeNewVersion(long version, long previousVersion)
        {
            var worldLine = state.worldlineTracker.Enter();
            var deps = state.dependencySetPool.Checkout();
            if (previousVersion != 0)
                deps.Update(state.me, previousVersion);
            var success = state.versions.TryAdd(version, deps);
            Debug.Assert(success);
            state.worldlineTracker.Leave();
        }

        /// <summary>
        /// Invoked when a version is persistent.
        /// </summary>
        /// <param name="version">Version number of the finished version</param>
        /// <param name="token">unique token that identifies the checkpoint associated with the version</param>
        public void OnVersionPersistent(long version)
        {
            var worldLine = state.worldlineTracker.Enter();
            state.versions.TryRemove(version, out var deps);
            var workerVersion = new WorkerVersion(state.me, version);
            state.dprFinder.ReportNewPersistentVersion(worldLine, workerVersion, deps);
            state.dependencySetPool.Return(deps);
            state.worldlineTracker.Leave();
        }

        /// <summary>
        /// Invoked when rollback is complete. There is no need for any identifier because libDPR always issues rollbacks
        /// one-at-a-time without only one outstanding rollback.
        /// </summary>
        public void OnRollbackComplete()
        {
            // When rolling back, any currently unreported dependencies are lost anyways. Can safely discard.
            state.versions.Clear();
            state.rollbackProgress.Set();
        }
    }
}