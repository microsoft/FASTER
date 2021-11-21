using System.Diagnostics;
using System.Threading;

namespace FASTER.core
{
    /// <summary>
    /// FasterLogCommitStrategy defines the way FasterLog behaves on Commit() and CommitStrongly() calls. In addition
    /// to choosing from a set of pre-defined ones, users can implement their own for custom behavior.
    /// </summary>
    public interface IFasterLogCommitStrategy
    {
        /// <summary>
        /// Invoked when strategy object is attached to a FasterLog instance.
        /// </summary>
        /// <param name="log"> the log this commit strategy is attached to </param>
        public void OnAttached(FasterLog log);
        
        /// <summary>
        /// Admission control to decide whether a call to Commit() or CommitStrongly() should successfully start or not.
        /// If false, commit logic will not execute, and the user will get an explicit false return value on commit
        /// methods that support return values. If true, a commit will be created to cover at least the current tail
        /// given.
        /// </summary>
        /// <param name="strongCommit"> whether the request is a strong commit </param>
        /// <param name="currentTail"> if successful, this request will at least commit up to this tail</param>
        /// <param name="metadataChanged"> whether commit metadata (e.g., iterators) has changed </param>
        /// <returns></returns>
        public bool AdmitCommit(bool strongCommit, long currentTail, bool metadataChanged);

        
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

    /// <summary>
    /// The default commit strategy ensures that each record is covered by at most one commit request (except when the
    /// metadata has changed). Redundant commit calls are dropped.
    /// </summary>
    public class DefaultCommitStrategy : IFasterLogCommitStrategy
    {
        private long coveredUntilAddress = 0;        
        private FasterLog log;

        /// <inheritdoc/>
        public void OnAttached(FasterLog log) => this.log = log;

        /// <inheritdoc/>
        public bool AdmitCommit(bool strongCommit, long currentTail, bool metadataChanged)
        {            
            if (currentTail <= log.CommittedUntilAddress && !metadataChanged) return false;
            return Utility.MonotonicUpdate(ref coveredUntilAddress, currentTail, out _) || metadataChanged;
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

    /// <summary>
    /// MaxParallelCommitStrategy allows k commits to be outstanding at any giving time. The k commits are guaranteed
    /// to be non-overlapping with the exception to metadata changes. Additional commit requests will fail and the users
    /// need to try again at a later time.
    /// </summary>
    public class MaxParallelCommitStrategy : IFasterLogCommitStrategy
    {
        private FasterLog log;
        private int commitInProgress, maxCommitInProgress;

        /// <summary>
        /// Constructs a new MaxParallelCommitStrategy
        /// </summary>
        /// <param name="maxCommitInProgress"> maximum number of commits that can be outstanding at a time </param>
        public MaxParallelCommitStrategy(int maxCommitInProgress)
        {
            this.maxCommitInProgress = maxCommitInProgress;
        }
        
        /// <inheritdoc/>
        public void OnAttached(FasterLog log) => this.log = log;
        
        /// <inheritdoc/>
        public bool AdmitCommit(bool strongCommit, long currentTail, bool metadataChanged)
        {
            if (strongCommit) return true;
            if (currentTail <= log.CommittedUntilAddress && !metadataChanged) return false;

            while (true)
            {
                var cip = commitInProgress;
                if (cip == maxCommitInProgress) return false;
                if (Interlocked.CompareExchange(ref commitInProgress, cip, cip + 1) == cip) return true;
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
        }
    }

    /// <summary>
    /// MaxParallelCommitStrategyWithRetry allows k commits to be outstanding at any giving time. The k commits are
    /// guaranteed to be non-overlapping with the exception to metadata changes. Additional commit requests will fail,
    /// but the system will automatically retry commits to cover up to at least the address range requested of the
    /// failed requests.
    /// </summary>
    public class MaxParallelCommitStrategyWithRetry : IFasterLogCommitStrategy
    {
        private FasterLog log;
        private int commitInProgress, maxCommitInProgress;
        private bool shouldRetry;

        /// <summary>
        /// Constructs a new MaxParallelCommitStrategyWithRetry
        /// </summary>
        /// <param name="maxCommitInProgress"> maximum number of commits that can be outstanding at a time </param>
        public MaxParallelCommitStrategyWithRetry(int maxCommitInProgress)
        {
            this.maxCommitInProgress = maxCommitInProgress;
        }
        
        /// <inheritdoc/>
        public void OnAttached(FasterLog log) => this.log = log;
        
        /// <inheritdoc/>
        public bool AdmitCommit(bool strongCommit, long currentTail, bool metadataChanged)
        {
            if (strongCommit) return true;
            if (currentTail <= log.CommittedUntilAddress && !metadataChanged) return false;

            while (true)
            {
                var cip = commitInProgress;
                if (cip == maxCommitInProgress)
                {
                    shouldRetry = true;
                    return false;
                }
                if (Interlocked.CompareExchange(ref commitInProgress, cip, cip + 1) == cip) return true;
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

    /// <summary>
    /// RateLimitCommitStrategy will only issue a request if it covers at least m bytes or if there has not been a
    /// commit request in n milliseconds. Additional commit requests will fail and the users
    /// need to try again at a later time.
    /// </summary>
    public class RateLimitCommitStrategy : IFasterLogCommitStrategy
    {
        private FasterLog log;
        private Stopwatch stopwatch;
        private long lastAdmittedMilli, lastAdmittedAddress, thresholdMilli, thresholdRange;

        /// <summary>
        /// Constructs a new RateLimitCommitStrategy
        /// </summary>
        /// <param name="thresholdMilli"> minimum time, in milliseconds, to be allowed between two commits, unless thresholdRange bytes will be committed</param>
        /// <param name="thresholdRange"> minimum range, in bytes, to be allowed between two commits, unless it has been thresholdMilli milliseconds</param>
        public RateLimitCommitStrategy(long thresholdMilli, long thresholdRange)
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
        public bool AdmitCommit(bool strongCommit, long currentTail, bool metadataChanged)
        {
            if (currentTail <= log.CommittedUntilAddress && !metadataChanged) return false;
            var now = stopwatch.ElapsedMilliseconds;

            while (true)
            {
                var lastSeenMilli = lastAdmittedMilli;
                var lastSeenAddress = lastAdmittedAddress;
                if (now - lastSeenMilli < thresholdMilli && currentTail - lastSeenAddress < thresholdRange)
                    return false;
                // Can live lock?
                if (Interlocked.CompareExchange(ref lastAdmittedMilli, now, lastSeenMilli) == lastSeenMilli
                    && Interlocked.CompareExchange(ref lastAdmittedAddress, currentTail, lastSeenAddress) == lastSeenAddress)
                    return true;
            }
        }
        
        /// <inheritdoc/>
        public void OnCommitCreated(FasterLogRecoveryInfo info)
        {
            Utility.MonotonicUpdate(ref lastAdmittedAddress, info.UntilAddress, out _);
        }

        /// <inheritdoc/>
        public void OnCommitFinished(FasterLogRecoveryInfo info)
        {
        }
    }
}