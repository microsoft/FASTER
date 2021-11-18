using System.Diagnostics;
using System.Threading;

namespace FASTER.core
{
    public interface IFasterLogCommitStrategy
    {
        public void Attach(FasterLog log);
        
        public bool AdmitCommit(bool strongCommit, long currentTail, bool metadataChanged);

        
        public void OnCommitCreated(FasterLogRecoveryInfo info);

        public void OnCommitFinished(FasterLogRecoveryInfo info);

    }

    public class DefaultCommitStrategy : IFasterLogCommitStrategy
    {
        private long coveredUntilAddress = 0;        
        private FasterLog log;

        public void Attach(FasterLog log) => this.log = log;

        public bool AdmitCommit(bool strongCommit, long currentTail, bool metadataChanged)
        {            
            if (currentTail <= log.CommittedUntilAddress && !metadataChanged) return false;
            return Utility.MonotonicUpdate(ref coveredUntilAddress, currentTail, out _) || metadataChanged;
        }
        
        public void OnCommitCreated(FasterLogRecoveryInfo info)
        {
        }

        public void OnCommitFinished(FasterLogRecoveryInfo info)
        {
        }
    }

    public class MaxParallelCommitStrategy : IFasterLogCommitStrategy
    {
        private FasterLog log;
        private int commitInProgress, maxCommitInProgress;

        public MaxParallelCommitStrategy(int maxCommitInProgress)
        {
            this.maxCommitInProgress = maxCommitInProgress;
        }
        
        public void Attach(FasterLog log) => this.log = log;
        
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
        
        public void OnCommitCreated(FasterLogRecoveryInfo info)
        {
        }

        public void OnCommitFinished(FasterLogRecoveryInfo info)
        {
            Interlocked.Decrement(ref commitInProgress);
        }
    }

    public class MaxParallelCommitStrategyWithRetry : IFasterLogCommitStrategy
    {
        private FasterLog log;
        private int commitInProgress, maxCommitInProgress;
        private bool shouldRetry;

        public MaxParallelCommitStrategyWithRetry(int maxCommitInProgress)
        {
            this.maxCommitInProgress = maxCommitInProgress;
        }
        
        public void Attach(FasterLog log) => this.log = log;
        
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
        
        public void OnCommitCreated(FasterLogRecoveryInfo info)
        {
        }

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

    public class RateLimitCommitStrategy : IFasterLogCommitStrategy
    {
        private FasterLog log;
        private Stopwatch stopwatch;
        private long lastAdmittedMilli, lastAdmittedAddress, thresholdMilli, thresholdRange;

        public RateLimitCommitStrategy(long thresholdMilli, long thresholdRange)
        {
            this.thresholdMilli = thresholdMilli;
            this.thresholdRange = thresholdRange;
            stopwatch = Stopwatch.StartNew();
            lastAdmittedMilli = -thresholdMilli;
            lastAdmittedAddress = -thresholdRange;
        }
        
        public void Attach(FasterLog log) => this.log = log;
        
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
        
        public void OnCommitCreated(FasterLogRecoveryInfo info)
        {
            Utility.MonotonicUpdate(ref lastAdmittedAddress, info.UntilAddress, out _);
        }

        public void OnCommitFinished(FasterLogRecoveryInfo info)
        {
        }
    }
}