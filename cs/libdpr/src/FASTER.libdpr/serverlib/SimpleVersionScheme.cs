using System;
using System.Threading;

namespace FASTER.libdpr
{
    /// <summary>
    /// SimpleVersionScheme is the default versioning mechanism used by SimpleStateObject. It uses a reader-writer latch
    /// to ensure that checkpoint/recovery happen without concurrent operations, which makes versioning straightforward.
    /// </summary>
    public class SimpleVersionScheme
    {
        private long version = 1; 
        // One count is reserved for the thread that actually advances the version, each batch also gets one
        private CountdownEvent count = new CountdownEvent(1);
        private ManualResetEventSlim versionChanged;

        /// <summary>
        /// Returns the current version
        /// </summary>
        /// <returns>the current version</returns>
        public long Version()
        {
            return version;
        }

        /// <summary>
        /// Enters a batch into the current version to protect later processing from concurrent checkpoints or recovery.
        ///
        /// Method may block if there is a checkpoint or recovery underway. Once a batch enters the version, it must
        /// eventually leave through the Leave() method so the system makes meaningful progress.
        /// </summary>
        /// <returns>current version number</returns>
        public long Enter()
        {
            while (true)
            {
                var ev = versionChanged;
                if (ev == null)
                {
                    if (count.TryAddCount())
                        // Because version is only changed after count == 0, which we know will never happen before we
                        // return at this point, it suffices to just read the field. 
                        return version;
                    // If the count ever reaches 0, version change may have already occured, and we need to
                    // back away to retry
                }
                else
                {
                    // Wait for version advance to complete and then try again.
                    versionChanged.Wait();
                }
            }
        }

        /// <summary>
        /// Signals that a batch has been processed and should no longer be protected
        /// </summary>
        public void Leave()
        {
            count.Signal();
        }

        /// <summary>
        /// Attempts to advance the version to the target version, executing the given action in a critical section
        /// where no batches are being processed before entering the next version. Each version will be advanced to
        /// exactly once. This method may fail and return false if given target version is not larger than the current
        /// version (possibly due to concurrent invocations to advance to the same version).
        ///
        /// After the method returns, subsequent calls to Version() and Enter() will return at least the value of
        /// targetVersion.
        /// </summary>
        /// <param name="criticalSection"> The logic to execute in a critical section </param>
        /// <param name="targetVersion"> The version to advance to, or -1 for the immediate next version</param>
        /// <returns> Whether the advance was successful </returns>
        public bool TryAdvanceVersion(Action<long, long> criticalSection, long targetVersion = -1)
        {
            if (targetVersion != -1 && targetVersion <= version) return false;

            var ev = new ManualResetEventSlim();
            // Compare and exchange to install our advance
            while (Interlocked.CompareExchange(ref versionChanged, ev, null) != null) {}
            
            // After success, we have exclusive access to update version
            var original = version;
            if (targetVersion != -1 && targetVersion <= version)
            {
                // In this case, advance request is not valid
                versionChanged = null;
                return false;
            }

            // One count is reserved for the thread that actually advances the version
            count.Signal();
            // Wait until all batches in the previous version has been processed
            count.Wait();
            
            version = targetVersion == -1 ? version + 1 : targetVersion;
            criticalSection(original, version);
            // Complete the version change
            count.Reset();
            ev.Set();
            versionChanged = null;
            return true;
        }
    }
}