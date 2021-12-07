using System;
using System.Threading;
using FASTER.core;

namespace FASTER.core
{
    /// <summary>
    ///     SimpleVersionScheme operates like a read-write latch that ensures protected code segments are not interleaved
    ///     with special "version change" code segments that run sequentially. It is more scalable than a typical reader-writer latch
    ///     by taking advantage of the epoch protection framework in the common case.
    /// </summary>
    public class SimpleVersionScheme
    {
        private LightEpoch epoch;
        private long version = 1;
        private ManualResetEventSlim versionChanged;

        /// <summary>
        ///     Creates a new SimpleVersionScheme
        /// </summary>
        public SimpleVersionScheme()
        {
            epoch = new LightEpoch();
        }

        /// <summary>
        ///     Returns the current version
        /// </summary>
        /// <returns>the current version</returns>
        public long Version()
        {
            return version;
        }

        /// <summary>
        ///     Enters a batch into the current version to protect later processing from concurrent checkpoints or recovery.
        ///     Method may block if there is a checkpoint or recovery underway. Once a batch enters the version, it must
        ///     eventually leave through the Leave() method so the system makes meaningful progress.
        /// </summary>
        /// <returns>current version number</returns>
        public long Enter()
        {
            epoch.Resume();
            // Temporarily block if a version change is under way --- depending on whether the thread observes
            // versionChanged, they are either in the current version or the next
            while (true)
            {
                var ev = versionChanged;
                if (ev == null) break;
                // Allow version change to complete by leaving this epoch. 
                epoch.Suspend();
                ev.Wait();
                epoch.Resume();
            }
            return version;
        }

        /// <summary>
        ///     Signals that a batch has been processed and should no longer be protected
        /// </summary>
        public void Leave()
        {
            epoch.Suspend();
        }

        /// <summary>
        ///     Attempts to advance the version to the target version, executing the given action in a critical section
        ///     where no batches are being processed before entering the next version. Each version will be advanced to
        ///     exactly once. This method may fail and return false if given target version is not larger than the current
        ///     version (possibly due to concurrent invocations to advance to the same version).
        ///     After the method returns, subsequent calls to Version() and Enter() will return at least the value of
        ///     targetVersion.
        /// </summary>
        /// <param name="criticalSection"> The logic to execute in a critical section </param>
        /// <param name="targetVersion"> The version to advance to, or -1 for the immediate next version</param>
        /// <returns> Whether the advance was successful </returns>
        public bool TryAdvanceVersion(Action<long, long> criticalSection, long targetVersion = -1)
        {
            var ev = new ManualResetEventSlim();
            // Compare and exchange to install our advance
            while (Interlocked.CompareExchange(ref versionChanged, ev, null) != null)
            {
            }
            
            if (targetVersion != -1 && targetVersion <= version)
            {
                versionChanged.Set();
                versionChanged = null;
                return false;
            }

            // Any thread that sees ev will be in v + 1, because the bump happens only after ev is set. 
            var original = version;
            epoch.Resume();
            epoch.BumpCurrentEpoch(() =>
            {
                version = targetVersion == -1 ? version + 1 : targetVersion;
                criticalSection(original, version);
                versionChanged.Set();
                versionChanged = null;
            });
            epoch.Suspend();
            return true;
        }
    }
}