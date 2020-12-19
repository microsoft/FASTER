using System.Collections.Concurrent;
using FASTER.core;
using FASTER.libdpr.versiontracking;

namespace FASTER.libdpr
{
    public class DprWorkerCallbacks<TToken>
    {
        private DprWorkerState<TToken> state;

        public void OnVersionEnd(long version, TToken token)
        {
            var versionHandle = state.versions.GetOrAdd(state.stateObject.Version(), version => new VersionHandle<TToken>());
            versionHandle.token = token;
        }

        public void OnVersionPersistent(long version, TToken token)
        {
            var versionObject = state.versions[version];
            versionObject.token = token;
            var workerVersion = new WorkerVersion(state.me, version);
            state.dprFinder.ReportNewPersistentVersion(workerVersion, versionObject.deps);
        }

        public void OnRollbackComplete()
        {
            state.epoch.BumpCurrentEpoch(() => state.nextWorldLine++);
        }
    }
}