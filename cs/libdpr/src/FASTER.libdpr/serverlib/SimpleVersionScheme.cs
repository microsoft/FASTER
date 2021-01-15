using System;
using System.Threading;
using FASTER.core;

namespace FASTER.libdpr
{
    public class SimpleVersionScheme
    {
        private ReaderWriterLockSlim versionLock = new ReaderWriterLockSlim();
        private long version = 1;
        // Negative denotes that some one is trying to advance a version
        private int count;

        public long Version()
        {
            return version;
        }

        public long Enter()
        {
            try
            {
                versionLock.EnterReadLock();
                Interlocked.Increment(ref count);
                return version;
            }
            finally
            {
                versionLock.ExitReadLock();
            }
        }

        public void Leave()
        {
            Interlocked.Decrement(ref count);
        }

        public bool AdvanceVersion(Action<long> criticalSection, long targetVersion = -1)
        {
            try
            {
                versionLock.EnterWriteLock();
                var original = version;
                if (targetVersion != -1 && targetVersion <= version) return false;
                version = targetVersion == -1 ? version + 1 : targetVersion;
                while (count != 0)
                    Thread.Yield();
                criticalSection(original);
                return true;
            }
            finally
            {
                versionLock.ExitWriteLock();
            }
        }
    }
}