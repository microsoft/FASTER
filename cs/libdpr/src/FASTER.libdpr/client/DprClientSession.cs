using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using FASTER.core;

namespace FASTER.libdpr
{
    public class DprClientSession
    {
        private Guid guid;
        private DprClient dprClient;
        private long seenDprViewNum = 0;
        private long seqNum = 0;
        private ClientVersionTracker versionTracker;
        private CommitPoint currentCommitPoint;
        
        private HashSet<WorkerVersion> deps;
        private long clientVersion = 1, clientWorldLine = 0;
        private ClientBatchTracker batchTracker;
        public DprClientSession(Guid guid, DprClient dprClient)
        {
            this.guid = guid;
            this.dprClient = dprClient;
            versionTracker = new ClientVersionTracker();
            currentCommitPoint = new CommitPoint();
            deps = new HashSet<WorkerVersion>();
            batchTracker = new ClientBatchTracker();
        }

        public CommitPoint GetCommitPoint()
        {
            if (Utility.MonotonicUpdate(ref seenDprViewNum, dprClient.GetDprViewNumber(), out _))
                ComputeCurrentCommitPoint();
            return currentCommitPoint;
        }
        
        // TODO(Tianyu): Note that this is not thread-safe
        public long IssueBatch(int batchSize, long workerId, ref DprBatchRequestHeader header)
        {
            // Wait for a batch slot to become available
            BatchInfo info;
            while (!batchTracker.TryGetBatchInfo(out info))
                // TODO(Tianyu): Is this ok?
                Thread.Yield();
            // Populate tracking information into the batch
            foreach (var dep in deps)
                info.deps.Add(dep);
            info.workerId = workerId;
            header.worldLine = clientWorldLine;
            info.startSeqNum = seqNum;
            seqNum += batchSize;
            info.endSeqNum = seqNum;
            
            // Populate header with relevant request information
            header.batchId = info.batchId;
            header.sessionId = guid;
            header.numDeps = info.deps.Count;
            header.worldLine = clientWorldLine;
            header.versionLowerBound = clientVersion;
            header.numMessages = batchSize;
            unsafe
            {
                fixed (byte* start = header.deps)
                {
                    for (var i = 0; i < info.deps.Count; i++)
                        Unsafe.AsRef<WorkerVersion>(start + sizeof(WorkerVersion) * i) = info.deps[i];
                }
            }
            return info.startSeqNum;
        }

        // TODO(Tianyu): Note that this is not thread-safe
        public unsafe void ResolveBatch(ref DprBatchResponseHeader reply)
        {
            var batchInfo = batchTracker.GetBatch(reply.batchId);
            

            // Remote machine would not have executed a batch if the world-lines are mismatched
            if (reply.worldLine != batchInfo.worldLine
                // These replies are from an earlier world-line that the client has already moved past. We would have
                // declared these ops lost, and the remote server would have rolled back. It is ok to simply
                // disregard these replies;
                || reply.worldLine < clientWorldLine)
            {
                // In this case, the remote machine has seen more failures than the client and the client needs to 
                // catch up 
                if (reply.worldLine > clientWorldLine)
                {
                    // TODO(Tianyu): Recovery codepath
                    // Wait for a refresh to make sure client is operating on latest DPR information
                    while (Utility.MonotonicUpdate(ref seenDprViewNum, dprClient.GetDprViewNumber(), out _))
                        Thread.Yield();
                    var result = new CommitPoint {UntilSerialNo = seqNum, ExcludedSerialNos = new List<long>()};
                    versionTracker.HandleRollback(dprClient.GetDprFinder().ReadSnapshot(), ref result);
                    batchTracker.HandleRollback(ref result);
                    AdjustCommitPoint(ref result);
                    throw new DprRollbackException(result);
                }
                // Otherwise, we would have declared these ops lost, and the remote server
                // would have rolled back. It is ok to simply disregard these replies
                batchTracker.FinishBatch(reply.batchId);
                return;
            }

            // Update versioning information
            long maxVersion = 0;

            fixed (byte* v = reply.versions)
            {
                var offset = 0;
                foreach (var version in new DprBatchVersionVector(v))
                {
                    // Not executed for non-dpr related reason.
                    if (version == 0) continue;
                    // Otherwise, update client version and add to tracking
                    maxVersion = Math.Max(maxVersion, version);
                    versionTracker.Add(batchInfo.startSeqNum + offset, new WorkerVersion(batchInfo.workerId, version));
                    offset++;
                }
                Utility.MonotonicUpdate(ref clientVersion, maxVersion, out _);
            }
            

            // Remove all deps to save space. Future ops will implicitly depend on these by transitivity
            foreach (var dep in batchInfo.deps)
                deps.Remove(dep);
            // Add largest worker-version as dependency for future ops. Must add after removal in case this op has
            // a version self-dependency (very likely) that would otherwise be erased
            deps.Add(new WorkerVersion(batchInfo.workerId, maxVersion));
            
            // Free up this batch's tracking space
            batchTracker.FinishBatch(reply.batchId);
        }

        // We may be able to rewrite the computed commit point in a more concise way, e.g. if the computed commit point
        // is until op 9, with exception of op 7, 8, 9, we can simplify this to until 6.
        private void AdjustCommitPoint(ref CommitPoint computed)
        {
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
            currentCommitPoint.ExcludedSerialNos.Clear();
            currentCommitPoint.UntilSerialNo = versionTracker.LargestSeqNum();
            
            // Add exceptions to all uncommitted, but finished ops
            var dprTable = dprClient.GetDprFinder().ReadSnapshot();
            versionTracker.ResolveOperations(dprTable);
            foreach (var (s, _) in versionTracker)
                currentCommitPoint.ExcludedSerialNos.Add(s);

            // Add any exceptions that have not finished (earlier committed operations may have completed out of order)
            foreach (var batch in batchTracker)
            {
                if (batch.startSeqNum >= currentCommitPoint.UntilSerialNo) continue;
                for (var i = batch.startSeqNum; i < currentCommitPoint.UntilSerialNo && i < batch.endSeqNum; i++)
                    currentCommitPoint.ExcludedSerialNos.Add(i);
            }
            
            // Make sure representation is most concise
            AdjustCommitPoint(ref currentCommitPoint);
        }
    }
}