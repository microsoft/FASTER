using System;

namespace FASTER.libdpr
{
    /// <summary>
    /// Abstracts a non-versioned state-store that performs checkpoints and rollbacks synchronously. This is a
    /// simpler API than IStateObject and can accomodate a wider range of state-stores.
    /// </summary>
    /// <typeparam name="TToken">Type of token that uniquely identifies a checkpoint</typeparam>
    public abstract class SimpleStateObject<TToken> : IStateObject<TToken>
    {
        private DprWorkerCallbacks<TToken> callbacks;
        private readonly SimpleVersionScheme versionScheme = new SimpleVersionScheme();

        /// <summary>
        /// Process batches under shared latch for correctness
        /// </summary>
        /// <returns></returns>
        public SimpleVersionScheme VersionScheme() => versionScheme;
        
        /// <summary>
        /// Blockingly performs a checkpoint. Invokes the supplied callback when contents of checkpoint are on
        /// persistent storage and recoverable. This function is allowed to return as soon as checkpoint content
        /// is finalized, but before contents are persistent. libDPR will not interleave batch operation, other
        /// checkpoint requests, or restore requests with this function. 
        /// </summary>
        /// <param name="onPersist">Callback to invoke when checkpoint is recoverable</param>
        protected abstract void PerformCheckpoint(Action<TToken> onPersist);

        /// <summary>
        /// Blockingly recovers to a previous checkpoint as identified by the token. The function returns only after
        /// the state is restored for all future calls. libDPR will not interleave batch operation, other
        /// checkpoint requests, or restore requests with this function.
        /// </summary>
        /// <param name="token">Checkpoint to recover to</param>
        protected abstract void RestoreCheckpoint(TToken token);
        
        /// <inheritdoc/>
        public void Register(DprWorkerCallbacks<TToken> callbacks)
        {
            this.callbacks = callbacks;
        }

        /// <inheritdoc/>
        public long Version()
        {
            return versionScheme.Version();
        }
        
        /// <inheritdoc/>
        public void BeginCheckpoint(long targetVersion = -1)
        {
            versionScheme.AdvanceVersion(v =>
            {
                PerformCheckpoint(token =>
                {
                    callbacks.OnVersionEnd(v, token);
                    callbacks.OnVersionPersistent(v, token);
                });
            }, targetVersion);
            
        }
        
        /// <inheritdoc/>
        public void BeginRestore(TToken token)
        {
            versionScheme.AdvanceVersion(v =>
            {
                RestoreCheckpoint(token);
                callbacks.OnRollbackComplete();
            });
        }
    }
}