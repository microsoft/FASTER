using System;
using System.Threading;

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
        private long versionCounter = 0;
        private ReaderWriterLockSlim opLatch = new ReaderWriterLockSlim();

        /// <summary>
        /// Process batches under shared latch for correcness
        /// </summary>
        /// <returns></returns>
        public ReaderWriterLockSlim OperationLatch() => opLatch;
        
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
            return versionCounter;
        }
        
        /// <inheritdoc/>
        public void BeginCheckpoint(long targetVersion = -1)
        {
            opLatch.EnterWriteLock();
            try
            {
                if (targetVersion <= versionCounter) return;

                versionCounter = targetVersion == -1 ? versionCounter + 1 : targetVersion;
                PerformCheckpoint(token =>
                {
                    callbacks.OnVersionEnd(versionCounter, token);
                    callbacks.OnVersionPersistent(versionCounter, token);
                });
            }
            finally
            {
                opLatch.ExitWriteLock();
            }
        }
        
        /// <inheritdoc/>
        public void BeginRestore(TToken token)
        {
            opLatch.EnterWriteLock();
            versionCounter++;
            RestoreCheckpoint(token);
            callbacks.OnRollbackComplete();
            opLatch.ExitWriteLock();
        }
    }
}