using System.Threading;

namespace FASTER.libdpr
{
    public class SimpleStateObjectAdapter<TUnderlying, TToken> : IStateObject<TToken>
        where TUnderlying : ISimpleStateObject<TToken>
    {
        private TUnderlying underlying;
        private DprWorkerCallbacks<TToken> callbacks;
        private long versionCounter;
        private ReaderWriterLockSlim opLatch;

        public SimpleStateObjectAdapter(TUnderlying underlying, ReaderWriterLockSlim opLatch)
        {
            this.underlying = underlying;
            this.opLatch = opLatch;
        }
        
        public void Register(DprWorkerCallbacks<TToken> callbacks)
        {
            this.callbacks = callbacks;
        }

        public long Version()
        {
            return versionCounter;
        }

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