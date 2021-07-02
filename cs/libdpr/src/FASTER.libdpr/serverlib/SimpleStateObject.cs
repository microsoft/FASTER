using System;
using System.Collections.Generic;

namespace FASTER.libdpr
{
    /// <summary>
    /// Abstracts a non-versioned state-store that performs checkpoints and rollbacks synchronously. This is a
    /// simpler API than IStateObject designed to work with a wider range of state-stores.
    ///
    /// In exchange of its simplicity, this class is pessimistic about concurrency and requires caller help to
    /// synchronize method calls properly. See VersionScheme().
    /// </summary>
    public abstract class SimpleStateObject : IStateObject
    {
        private DprWorkerCallbacks callbacks;
        private readonly SimpleVersionScheme versionScheme = new SimpleVersionScheme();

        /// <summary>
        /// Process batches under shared latch for correctness. Before starting to execute a batch, call Enter()
        /// to obtain the version all operations within the batch will execute in. After execution, call Leave()
        /// to stop protecting the version and allow checkpoints to continue. 
        /// </summary>
        /// <returns> VersionScheme used by this SimpleStateObject </returns>
        public SimpleVersionScheme VersionScheme() => versionScheme;
        
        /// <summary>
        /// Blockingly performs a checkpoint. Invokes the supplied callback when contents of checkpoint are on
        /// persistent storage and recoverable. This function is allowed to return as soon as checkpoint content
        /// is finalized, but before contents are persistent. libDPR will not interleave batch operation, other
        /// checkpoint requests, or restore requests with this function. 
        /// </summary>
        /// <param name="onPersist">Callback to invoke when checkpoint is recoverable</param>
        protected abstract void PerformCheckpoint(long version, ReadOnlySpan<byte> deps, Action onPersist);

        /// <summary>
        /// Blockingly recovers to a previous checkpoint as identified by the token. The function returns only after
        /// the state is restored for all future calls. libDPR will not interleave batch operation, other
        /// checkpoint requests, or restore requests with this function.
        /// </summary>
        /// <param name="token">Checkpoint to recover to</param>
        protected abstract void RestoreCheckpoint(long version);

        public abstract void PruneVersion(long version);

        public abstract IEnumerable<(long, byte[])> GetUnprunedVersions();

        /// <inheritdoc/>
        public void Register(DprWorkerCallbacks callbacks)
        {
            this.callbacks = callbacks;
        }

        /// <inheritdoc/>
        public long Version()
        {
            return versionScheme.Version();
        }
        
        /// <inheritdoc/>
        public void BeginCheckpoint(IStateObject.DepsProvider depsProvider, long targetVersion = -1)
        {
            versionScheme.TryAdvanceVersion(v =>
            {
                var deps = depsProvider(v);
                PerformCheckpoint(v, deps, () =>
                {
                    callbacks.OnVersionPersistent(v);
                });
            }, targetVersion);
            
        }
        
        /// <inheritdoc/>
        public void BeginRestore(long version)
        {
            versionScheme.TryAdvanceVersion(_ =>
            {
                RestoreCheckpoint(version);
                callbacks.OnRollbackComplete();
            });
        }
    }
}