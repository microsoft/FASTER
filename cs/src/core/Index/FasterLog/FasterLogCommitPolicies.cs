using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace FASTER.core
{
    /// <summary>
    /// FasterLogCommitPolicy defines the way FasterLog behaves on Commit(). In addition
    /// to choosing from a set of pre-defined ones, users can implement their own for custom behavior
    /// </summary>
    public interface IFasterLogCommitPolicy
    {
        /// <summary>
        /// Invoked when strategy object is attached to a FasterLog instance.
        /// </summary>
        /// <param name="log"> the log this commit strategy is attached to </param>
        public void OnAttached(FasterLog log);
        
        /// <summary>
        /// Admission control to decide whether a call to Commit() should successfully start or not.
        /// If false, commit logic will not execute. If true, a commit will be created to cover at least the tail given,
        /// although the underlying implementation may choose to compact multiple admitted Commit() invocations into
        /// one commit operation. It is the implementer's responsibility to log and retry any filtered Commit() when
        /// necessary (e.g., when there will not be any future Commit() invocations, but the last Commit() was filtered)
        /// </summary>
        /// <param name="currentTail"> if successful, this request will commit at least up to this tail</param>
        /// <param name="metadataChanged"> whether commit metadata (e.g., iterators) has changed </param>
        /// <returns></returns>
        public bool AdmitCommit(long currentTail, bool metadataChanged);

        /// <summary>
        /// Invoked when a commit is successfully created
        /// </summary>
        /// <param name="info"> commit content </param>
        public void OnCommitCreated(FasterLogRecoveryInfo info);

        /// <summary>
        /// Invoked after a commit is complete
        /// </summary>
        /// <param name="info"> commit content </param>
        public void OnCommitFinished(FasterLogRecoveryInfo info);

    }
    
    internal class DefaultCommitPolicy : IFasterLogCommitPolicy
    {

        /// <inheritdoc/>
        public void OnAttached(FasterLog log) {}

        /// <inheritdoc/>
        public bool AdmitCommit(long currentTail, bool metadataChanged) => true;
        
        /// <inheritdoc/>
        public void OnCommitCreated(FasterLogRecoveryInfo info) {}

        /// <inheritdoc/>
        public void OnCommitFinished(FasterLogRecoveryInfo info) {}
    }

    internal class MaxParallelCommitPolicy : IFasterLogCommitPolicy
    {
        private FasterLog log;
        private int commitInProgress, maxCommitInProgress;
        // If we filtered out some commit, make sure to remember to retry later 
        private bool shouldRetry;
        
        internal MaxParallelCommitPolicy(int maxCommitInProgress)
        {
            this.maxCommitInProgress = maxCommitInProgress;
        }
        
        /// <inheritdoc/>
        public void OnAttached(FasterLog log) => this.log = log;
        
        /// <inheritdoc/>
        public bool AdmitCommit(long currentTail, bool metadataChanged)
        {
            while (true)
            {
                var cip = commitInProgress;
                if (cip == maxCommitInProgress)
                {
                    shouldRetry = true;
                    return false;
                }

                if (Interlocked.CompareExchange(ref commitInProgress, cip + 1, cip) == cip) return true;
            }
        }
        
        /// <inheritdoc/>
        public void OnCommitCreated(FasterLogRecoveryInfo info)
        {
        }

        /// <inheritdoc/>
        public void OnCommitFinished(FasterLogRecoveryInfo info)
        {
            Interlocked.Decrement(ref commitInProgress);
            if (shouldRetry)
            {
                shouldRetry = false;
                log.Commit();
            }
        }
    }

    internal class RateLimitCommitPolicy : IFasterLogCommitPolicy
    {
        private FasterLog log;
        private Stopwatch stopwatch;
        private long lastAdmittedMilli, lastAdmittedAddress, thresholdMilli, thresholdRange;
        private int shouldRetry = 0;
        
        internal RateLimitCommitPolicy(long thresholdMilli, long thresholdRange)
        {
            this.thresholdMilli = thresholdMilli;
            this.thresholdRange = thresholdRange;
            stopwatch = Stopwatch.StartNew();
            lastAdmittedMilli = -thresholdMilli;
            lastAdmittedAddress = -thresholdRange;
        }
        
        /// <inheritdoc/>
        public void OnAttached(FasterLog log) => this.log = log;
        
        /// <inheritdoc/>
        public bool AdmitCommit(long currentTail, bool metadataChanged)
        {
            var now = stopwatch.ElapsedMilliseconds;
            while (true)
            {
                var lastSeenMilli = lastAdmittedMilli;
                var lastSeenAddress = lastAdmittedAddress;
                if (now - lastSeenMilli < thresholdMilli && currentTail - lastSeenAddress < thresholdRange)
                {
                    // Only allow spawning of task if no other task is already underway
                    if (Interlocked.CompareExchange(ref shouldRetry, 1, 0) == 0)
                    {
                        Task.Run(async () =>
                        {
                            await Task.Delay(TimeSpan.FromMilliseconds(thresholdMilli));
                            shouldRetry = 0;
                            log.Commit();
                        });
                    }
                    return false;
                }

                if (Interlocked.CompareExchange(ref lastAdmittedMilli, now, lastSeenMilli) == lastSeenMilli
                    && Interlocked.CompareExchange(ref lastAdmittedAddress, currentTail, lastSeenAddress) == lastSeenAddress)
                    return true;
            }
        }
        
        /// <inheritdoc/>
        public void OnCommitCreated(FasterLogRecoveryInfo info)
        {
        }

        /// <inheritdoc/>
        public void OnCommitFinished(FasterLogRecoveryInfo info)
        {
        }
    }

    public sealed partial class FasterLog : IDisposable
    {
        /// <summary>
        /// The default commit strategy ensures that each record is covered by at most one commit request (except when
        /// the metadata has changed). Redundant commit calls are dropped and corresponding commit invocation will
        /// return false.
        /// </summary>
        /// <returns> policy object </returns>
        public static IFasterLogCommitPolicy DefaultStrategy() => new DefaultCommitPolicy();

        /// <summary>
        /// Allows k (non-strong) commit requests to be in progress at any giving time. The k commits are guaranteed
        /// to be non-overlapping unless there are metadata changes. Additional commit requests will fail and
        /// automatically retried.
        /// </summary>
        /// <param name="k"> maximum number of commits that can be outstanding at a time </param>
        /// <param name="autoRetry">
        /// whether to automatically retry rejected commit requests later. If set to true, even when
        /// a commit() returns false due to being limited, the tail as of that commit will eventually be committed
        /// without the need to invoke commit() again.
        /// </param>
        /// <returns> policy object </returns>
        public static IFasterLogCommitPolicy MaxParallelCommitStrategy(int k) => new MaxParallelCommitPolicy(k);

        
        /// <summary>
        /// RateLimitCommitStrategy will only issue a request if it covers at least m bytes or if there has not been a
        /// commit request in n milliseconds. Additional commit requests will fail and automatically retried
        /// </summary>
        /// <param name="thresholdMilli">
        /// minimum time, in milliseconds, to be allowed between two commits, unless thresholdRange bytes will be committed
        /// </param>
        /// <param name="thresholdBytes">
        /// minimum range, in bytes, to be allowed between two commits, unless it has been thresholdMilli milliseconds
        /// </param>
        /// <returns> policy object </returns>
        public static IFasterLogCommitPolicy RateLimitCommitStrategy(long thresholdMilli, long thresholdBytes) =>
            new RateLimitCommitPolicy(thresholdMilli, thresholdBytes);
    }
}