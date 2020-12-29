using System.Threading;

namespace FASTER.libdpr
{
    /// <summary>
    /// Adds versioning to a SimpleStateObject so it looks like a full StateObject. Do not expect this to be
    /// performant.
    /// </summary>
    /// <typeparam name="TUnderlying">Underlying SimpleStateObject type</typeparam>
    /// <typeparam name="TToken">Type of token that uniquely identifies a checkpoint</typeparam>
    public class SimpleStateObjectAdapter<TUnderlying, TToken> : IStateObject<TToken>
        where TUnderlying : ISimpleStateObject<TToken>
    {
        private TUnderlying underlying;
        private DprWorkerCallbacks<TToken> callbacks;
        private long versionCounter;
        private ReaderWriterLockSlim opLatch;

        /// <summary>
        /// Constructs a new SimpleStateObjectAdapter to wrap the given SimpleStateObject, using the supplied
        /// latch to exclude checkpoint-related operations from batch processing. This class assumes that batch
        /// processing happens under shared mode of the latch, whereas checkpointing happens under exclusive mode
        /// </summary>
        /// <param name="underlying">Underlying SimpleStateObject</param>
        /// <param name="opLatch">Latch to use for access coordination with batch operations</param>
        public SimpleStateObjectAdapter(TUnderlying underlying, ReaderWriterLockSlim opLatch)
        {
            this.underlying = underlying;
            this.opLatch = opLatch;
        }
        
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
                underlying.PerformCheckpoint(token =>
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
            underlying.RestoreCheckpoint(token);
            callbacks.OnRollbackComplete();
            opLatch.ExitWriteLock();
        }
    }
}