using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using FASTER.common;
using FASTER.core;

namespace FASTER.libdpr
{
    /// <summary>
    ///     The ClientVersionTracker helps convert DPR guarantees into FASTER-style CommitPoints using a user specified
    ///     sequence number scheme. Thread-safe.
    /// </summary>
    public class ClientVersionTracker
    {
        private CommitPoint commitPoint = new CommitPoint {UntilSerialNo = 0, ExcludedSerialNos = new List<long>()};

        // List of operations that don't have a version yet
        private readonly ConcurrentDictionary<long, byte> incompleteOperations = new ConcurrentDictionary<long, byte>();

        // Maintained to quickly start the CommitPoint computation. 
        private long largestCommittedSeqNum;

        // Reusable List objects to save memory allocations in case there are frequent checkpoints
        private readonly SimpleObjectPool<List<long>> listPool =
            new SimpleObjectPool<List<long>>(() => new List<long>());

        // Reusable data structure to temporarily hold worker versions to remove. Also serves as a latch to ensure
        // serial execution of any operation that removes versions from tracking (due to commits or rollbacks)
        private readonly List<WorkerVersion> toRemove = new List<WorkerVersion>();

        // Maps each WorkerVersion to a list of operations that executed in that WorkerVersion. We use fine-grained
        // locking on each entry list to ensure thread-safety. 
        private readonly ConcurrentDictionary<WorkerVersion, VersionList> versionMappings =
            new ConcurrentDictionary<WorkerVersion, VersionList>();

        /// <summary>
        ///     Adds the given serial number to tracking.
        /// </summary>
        /// <param name="serialNum"> serial number to add </param>
        public void Add(long serialNum)
        {
            incompleteOperations.TryAdd(serialNum, 0);
        }

        /// <summary>
        ///     Resolves the given serial number as part of the given version
        /// </summary>
        /// <param name="serialNum"> serial number to resolve </param>
        /// <param name="executedAt"> version to associate operation with </param>
        public void Resolve(long serialNum, WorkerVersion executedAt)
        {
            var list = versionMappings.GetOrAdd(executedAt, k =>
            {
                var l = listPool.Checkout();
                l.Clear();
                return new VersionList(l);
            });
            list.Add(serialNum);
            // Must remove from incomplete operations AFTER adding to completed operations, otherwise a race can cause
            // us to not mistakenly mark this operation as committed. If an operation is committed but we say it isn't,
            // that is ok. 
            incompleteOperations.TryRemove(serialNum, out _);
        }

        /// <summary>
        ///     Resolves the given serial numbers to all be part of the given version
        /// </summary>
        /// <param name="serialNums"> serial numbers to resolve </param>
        /// <param name="executedAt"> version to associate operations with </param>
        public void ResolveAll(IEnumerable<long> serialNums, WorkerVersion executedAt)
        {
            var list = versionMappings.GetOrAdd(executedAt, k =>
            {
                var l = listPool.Checkout();
                l.Clear();
                return new VersionList(l);
            });

            foreach (var serialNum in serialNums)
            {
                list.Add(serialNum);
                incompleteOperations.TryRemove(serialNum, out _);
            }
        }

        /// <summary>
        ///     Update internal state according to the given new committed cut
        /// </summary>
        /// <param name="dprState">new dpr state</param>
        public void HandleCommit(IDprStateSnapshot dprState)
        {
            lock (toRemove)
            {
                foreach (var entry in versionMappings)
                    if (dprState.SafeVersion(entry.Key.WorkerId) >= entry.Key.Version)
                        toRemove.Add(entry.Key);

                foreach (var wv in toRemove)
                {
                    // Safe to return list immediately, as a committed version cannot be added to 
                    versionMappings.TryRemove(wv, out var entry);
                    listPool.Return(entry.list);
                    largestCommittedSeqNum = Math.Max(largestCommittedSeqNum, entry.maxSeqNum);
                }

                if (toRemove.Count != 0)
                    ComputeCurrentCommitPoint();

                toRemove.Clear();
            }
        }

        /// <summary>
        ///     Given the current DPR state, rollback any uncommitted operations that are now lost and write changes
        ///     to the given CommitPoint to account for any lost operations
        /// </summary>
        /// <param name="dprState"> current DPR state </param>
        public void HandleRollback(IDprStateSnapshot dprState)
        {
            lock (toRemove)
            {
                incompleteOperations.Clear();

                foreach (var entry in versionMappings)
                    if (dprState.SafeVersion(entry.Key.WorkerId) < entry.Key.Version)
                    {
                        listPool.Return(entry.Value.list);
                        toRemove.Add(entry.Key);
                    }

                foreach (var wv in toRemove)
                    versionMappings.TryRemove(wv, out _);
                toRemove.Clear();
            }
        }

        // We may be able to rewrite the computed commit point in a more concise way, e.g. if the computed commit point
        // is until op 9, with exception of op 7, 8, 9, we can simplify this to until 6.
        private void AdjustCommitPoint(ref CommitPoint computed)
        {
            computed.ExcludedSerialNos.Sort();
            for (var i = computed.ExcludedSerialNos.Count - 1; i >= 0; i--)
            {
                if (computed.ExcludedSerialNos[i] != computed.UntilSerialNo - 1) return;
                computed.UntilSerialNo--;
                computed.ExcludedSerialNos.RemoveAt(i);
            }
        }

        private void ComputeCurrentCommitPoint()
        {
            // Reset commit point 
            commitPoint.ExcludedSerialNos.Clear();
            commitPoint.UntilSerialNo = largestCommittedSeqNum + 1;

            foreach (var entry in versionMappings)
                lock (entry.Value.list)
                {
                    foreach (var serialNum in entry.Value.list)
                        if (serialNum < commitPoint.UntilSerialNo)
                            commitPoint.ExcludedSerialNos.Add(serialNum);
                }

            // Make sure representation is most concise
            AdjustCommitPoint(ref commitPoint);
        }

        /// <summary>
        ///     Get the current commit point. This returned object cannot be safely called concurrently with HandleCommit().
        /// </summary>
        /// <returns> current commit point</returns>
        public CommitPoint GetCommitPoint()
        {
            return commitPoint;
        }

        private class VersionList
        {
            internal readonly List<long> list;
            internal long maxSeqNum;

            internal VersionList(List<long> list)
            {
                this.list = list;
                maxSeqNum = -1;
            }

            internal void Add(long serialNum)
            {
                lock (list)
                {
                    list.Add(serialNum);
                    maxSeqNum = Math.Max(serialNum, maxSeqNum);
                }
            }
        }
    }
}