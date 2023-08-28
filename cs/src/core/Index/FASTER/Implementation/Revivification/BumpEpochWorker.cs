// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Threading;
using System.Threading.Tasks;
using static FASTER.core.Utility;

namespace FASTER.core
{
    internal static class BumpEpochWorker
    {
        // This is coordinated with BumpEpochWorker<Key, Value> constants to decrease the wait interval as the count of records grows
        // until we hit 16ms, which is the timer resolution on Windows. If we have more than MaxCountForBump record needing a bump, we'll
        // bump immediately. Otherwise we shift DefaultBumpIntervalMs by BumpMsShift multiples to determine sleep time (see ScanForBump()).
        internal const int DefaultBumpIntervalMs = 1024;
    }

    internal class BumpEpochWorker<Key, Value>
    {
        // These are coordinated with BumpEpochWorker.DefaultBumpIntervalMs; see its comments.
        const int MaxCountForBump = 32;
        const int BumpMsShift = 2;

        // State control variables.
        long state;
        const long ScanOrQuiescent = 0;     // Any threads in the worker are scanning or quiescent, so a thread may claim BumpOrSleep
        const long BumpOrSleep = 1;         // A thread is either bumping or sleeping before bumping, and after bumping will recheck for more work
        bool disposed;

        readonly FreeRecordPool<Key, Value> recordPool;

        readonly AutoResetEvent autoResetEvent = new(false);

        internal BumpEpochWorker(FreeRecordPool<Key, Value> recordPool) => this.recordPool = recordPool;

        private bool ClaimBumpOrSleepState() => state == ScanOrQuiescent && Interlocked.CompareExchange(ref this.state, BumpOrSleep, ScanOrQuiescent) == ScanOrQuiescent;

        internal void Start(bool fromAdd)
        {
            if (state == ScanOrQuiescent && (!fromAdd || ClaimBumpOrSleepState()))
                Task.Run(() => LaunchWorker(fromAdd));
            else
                this.autoResetEvent.Set();  // We must be in BumpOrSleep, so signal this
        }

        // Return whether another thread has been launched while we were scanning.
        internal bool YieldToAnotherThread() => this.state == BumpOrSleep;

        public long LaunchCount;

        private void LaunchWorker(bool fromAdd)
        {
            Interlocked.Increment(ref LaunchCount);
            ulong startMs;
            while (!disposed)
            {
                // Do the bump if we just added a free record. If this is the first time for this worker we may have only one record
                // (the one that triggered the worker run), or possibly more that happened at about the same time. Otherwise we've
                // looped up from below and already slept if needed. If not fromAdd, then we are here to update HasSafeRecords.
                if (fromAdd)
                { 
                    recordPool.fkv.epoch.BumpCurrentEpoch();
                    this.state = ScanOrQuiescent;   // Only set this if fromAdd, since we did not take BumpOrSleep state on entry in the non-fromAdd case
                }
                startMs = GetCurrentMilliseconds();

                // See if more entries were added following the bump.
                int waitMs;
                while (!ScanForBumpOrEmpty(startMs, fromAdd, out waitMs, out long lowestUnsafeEpoch))
                {
                    // No records needing Bump(), or another thread has taken BumpOrSleep state, or we're here from Take to update HasSafeRecords.
                    if (!fromAdd)
                        goto Done;

                    // If we can create some safe records by recalculating the safe epoch, redo the scan; otherwise break out of this loop to sleep and retry the bump.
                    if (lowestUnsafeEpoch > 0 && this.state == ScanOrQuiescent && recordPool.fkv.epoch.ComputeNewSafeToReclaimEpoch() >= lowestUnsafeEpoch)
                        continue;
                    break;
                }

                // We need another bump. If another thread has already claimed the BumpOrSleep state, exit.
                if (!ClaimBumpOrSleepState())
                    goto Done;

                // If we don't have many entries, sleep a bit so we don't thrash epoch increments.
                if (waitMs > 0)
                    this.autoResetEvent.WaitOne(waitMs);
            }
        Done:
            if (disposed)
                this.state = ScanOrQuiescent;
        }

        internal bool ScanForBumpOrEmpty(ulong startMs, bool fromAdd, out int waitMs, out long lowestUnsafeEpoch)
        {
            waitMs = BumpEpochWorker.DefaultBumpIntervalMs;
            lowestUnsafeEpoch = 0;
            if (!this.recordPool.ScanForBumpOrEmpty(MaxCountForBump, out int countNeedingBump, ref lowestUnsafeEpoch) || !fromAdd)
                return false;   // Pool is empty, no bump needed, or we are yielding to another thread

            if (countNeedingBump > 0)
            {
                waitMs = 0;
                if (countNeedingBump < MaxCountForBump)
                {
                    // Determine sleep interval based on count...                           // These are the current sleep times at count intervals, for illustration
                    if (countNeedingBump > MaxCountForBump / 2)                             // 16-31 records
                        waitMs = BumpEpochWorker.DefaultBumpIntervalMs >> BumpMsShift * 3;  // 16 ms
                    else if (countNeedingBump > MaxCountForBump / 4)                        // 8-15 records
                        waitMs = BumpEpochWorker.DefaultBumpIntervalMs >> BumpMsShift * 2;  // 64 ms
                    else if (countNeedingBump > MaxCountForBump / 8)                        // 4-7 records
                        waitMs = BumpEpochWorker.DefaultBumpIntervalMs >> BumpMsShift;      // 256 ms
                    else                                                                    // 1-4 records
                        waitMs = BumpEpochWorker.DefaultBumpIntervalMs;                     // 1024 ms

                    // If more time has already elapsed than we just decided to wait, we'll Bump immediately.
                    var elapsedMs = GetCurrentMilliseconds() - startMs;
                     if (elapsedMs >= (ulong)waitMs)
                        waitMs = 0;
                }
            }

            return this.state == ScanOrQuiescent;
        }

        internal void Dispose()
        {
            // Any in-progress thread will stop when it sees this, thinking another thread is taking over.
            this.disposed = true;          // This prevents a newly-launched thread from even starting
            var prevState = Interlocked.CompareExchange(ref this.state, BumpOrSleep, ScanOrQuiescent);
            this.autoResetEvent.Set();

            // If we were in BumpOrSleep before, wait until the worker thread wakes up and see the disposed state.
            if (prevState == BumpOrSleep)
            { 
                while (this.state == BumpOrSleep)
                    Thread.Yield();
            }
            this.autoResetEvent.Dispose();
        }

        /// <inheritdoc/>
        public override string ToString() => $"state: {(this.state == BumpOrSleep ? "BumpOrSleep" : "ScanOrQuiescent")}";
    }
}
