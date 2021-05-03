using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using FASTER.core;

namespace FASTER.libdpr
{
    /// <summary>
    /// Corresponds to a client session that is a single logical thread of execution in the system.
    /// </summary>
    public class DprClientSession
    {
        private Guid guid;
        private DprClient dprClient;
        private long seenDprViewNum = 0;
        private long seqNum = 0;
        private ClientVersionTracker versionTracker;
        private CommitPoint currentCommitPoint;
        
        private LightDependencySet deps;
        private long clientVersion = 1, clientWorldLine = 0;
        private ClientBatchTracker batchTracker;

        private bool trackCommits;

        internal DprClientSession(Guid guid, DprClient dprClient, bool trackCommits)
        {
            this.guid = guid;
            this.dprClient = dprClient;
            versionTracker = new ClientVersionTracker();
            currentCommitPoint = new CommitPoint();
            currentCommitPoint.ExcludedSerialNos = new List<long>();
            deps = new LightDependencySet();
            batchTracker = new ClientBatchTracker();
            this.trackCommits = trackCommits;
        }

        // Should only be called on client threads, as it conveys worldline changes through exceptions.
        private void CheckWorldlineChange()
        {
            // Nothing to check if no dpr update
            if (seenDprViewNum == dprClient.GetDprViewNumber()) return;
            // No failure
            if (clientWorldLine == dprClient.GetDprFinder().SystemWorldLine()) return;
            clientWorldLine = dprClient.GetDprFinder().SystemWorldLine();
            var result = new CommitPoint {UntilSerialNo = seqNum, ExcludedSerialNos = new List<long>()};
            versionTracker.HandleRollback(dprClient.GetDprFinder().ReadSnapshot(), ref result);
            batchTracker.HandleRollback(ref result);
            AdjustCommitPoint(ref result);
            throw new DprRollbackException(result);
        }

        /// <summary>
        /// Returns the commit point of the session. All operations in the returned commit point is guaranteed to
        /// be recoverable, although the system may recover more. 
        /// </summary>
        /// <returns> CommitPoint of the session </returns>
        /// <exception cref="NotSupportedException"> If the sessions was created with commit tracking turned off</exception>
        public CommitPoint GetCommitPoint()
        {
            if (!trackCommits) throw new NotSupportedException();
            if (Utility.MonotonicUpdate(ref seenDprViewNum, dprClient.GetDprViewNumber(), out _))
                ComputeCurrentCommitPoint();
            return currentCommitPoint;
        }

        private unsafe void CopyDeps(ref DprBatchRequestHeader header)
        {
            lock (deps)
            {
                fixed (byte* d = header.deps)
                {
                    header.numDeps = 0;
                    foreach (var wv in deps)
                    {
                        Unsafe.AsRef<WorkerVersion>(d + header.numDeps * sizeof(WorkerVersion)) = wv;
                        header.numDeps++;
                    }
                }
            }
        }
        
        /// <summary>
        /// Add a batch of requests to DPR tracking, with the given number of requests issued to the given worker ID.
        /// This method should be called before the client sends a batch.
        ///
        /// Operations within the issued batch are numbered with ids in [return_value, return_value + batch_size) in
        /// order for tracking.
        ///
        /// Not thread-safe except with ResolveBatch.
        /// </summary>
        /// <param name="batchSize">size of the batch to issue</param>
        /// <param name="workerId"> destination of the batch</param>
        /// <param name="header"> header that encodes tracking information (to be forwarded to batch destination)</param>
        /// <returns> start id of the issued op for tracking purposes. </returns>
        public long IssueBatch(int batchSize, Worker workerId, out Span<byte> header)
        {
            CheckWorldlineChange();
            // Wait for a batch slot to become available
            BatchInfo info;
            // TODO(Tianyu): Probably rewrite with async
            while (!batchTracker.TryGetBatchInfo(out info))
                Thread.Yield();
            unsafe
            {
                fixed (byte* b = info.header)
                {
                    ref var dprHeader = ref Unsafe.AsRef<DprBatchRequestHeader>(b);
                    // Populate info with relevant request information
                    info.workerId = workerId;
                    info.startSeqNum = seqNum;
                    info.endSeqNum = seqNum + batchSize;
                    seqNum = info.endSeqNum;
                    
                    // Populate header with relevant request information
                    dprHeader.batchId = info.batchId;
                    dprHeader.sessionId = guid;
                    dprHeader.worldLine = clientWorldLine;
                    dprHeader.versionLowerBound = clientVersion;
                    dprHeader.numMessages = batchSize;
                    // Populate tracking information into the batch
                    CopyDeps(ref dprHeader);
                    header = new Span<byte>(info.header, 0, dprHeader.Size());
                }
            }

            return info.startSeqNum;
        }

        /// <summary>
        /// Consumes a DPR batch reply and update tracking information. This method should be called before exposing
        /// batch result; if the method returns false, the results are rolled back and should be discarded.
        ///
        /// Thread-safe to invoke with other methods of the object.
        /// </summary>
        /// <param name="reply">The received reply</param>
        /// <returns>whether it is safe to proceed with consuming the operation result</returns>
        public unsafe bool ResolveBatch(ref DprBatchResponseHeader reply)
        {
            var batchInfo = batchTracker.GetBatch(reply.batchId);

            fixed (byte* b = batchInfo.header)
            {
                ref var request = ref Unsafe.AsRef<DprBatchRequestHeader>(b);
                // Remote machine would not have executed a batch if the world-lines are mismatched
                if (reply.worldLine != request.worldLine
                    // These replies are from an earlier world-line that the client has already moved past. We would have
                    // declared these ops lost, and the remote server would have rolled back. It is ok to simply
                    // disregard these replies;
                    || reply.worldLine < clientWorldLine)
                {
                    // In this case, the remote machine has seen more failures than the client and the client needs to 
                    // catch up 
                    if (reply.worldLine > clientWorldLine)
                    {
                        // Do nothing and client thread will find out about the failure at its own pace
                        return false;
                    }
                    // Otherwise, we would have declared these ops lost, and the remote server
                    // would have rolled back. It is ok to simply disregard these replies
                    batchTracker.FinishBatch(reply.batchId);
                    return false;
                }
                
                // Update versioning information
                long maxVersion = 0;

                // TODO(Tianyu): Reason harder about concurrency behavior here and document accordingly
                lock (versionTracker)
                {
                    fixed (byte* v = reply.versions)
                    {
                        var offset = 0;
                        foreach (var version in new DprBatchVersionVector(v))
                        {
                            // Not executed for non-dpr related reason.
                            if (version == 0) continue;
                            // Otherwise, update client version and add to tracking
                            maxVersion = Math.Max(maxVersion, version);
                            if (trackCommits)
                            {
                                versionTracker.Add(batchInfo.startSeqNum + offset,
                                    new WorkerVersion(batchInfo.workerId, version));
                            }
                            offset++;
                        }

                        Utility.MonotonicUpdate(ref clientVersion, maxVersion, out _);
                    }
                }


                lock (deps)
                {
                    fixed (byte* d = request.deps)
                    {

                        // Remove all deps to save space. Future ops will implicitly depend on these by transitivity
                        for (var i = 0; i < request.numDeps; i++)
                        {
                            ref var wv = ref Unsafe.AsRef<WorkerVersion>(d + sizeof(WorkerVersion) * i);
                            var succeed = deps.TryRemove(wv.Worker, wv.Version);
                            // Because client sessions are single threaded this call must succeed. 
                            Debug.Assert(succeed);
                        }

                        // Add largest worker-version as dependency for future ops. Must add after removal in case this op has
                        // a version self-dependency (very likely) that would otherwise be erased
                        deps.Update(batchInfo.workerId, maxVersion);
                    }
                }
                // Free up this batch's tracking space
                batchTracker.FinishBatch(reply.batchId);
            }

            return true;
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
            // TODO(Tianyu): This calculation is inefficient --- can just start from the largest seq num of committed versions
            currentCommitPoint.UntilSerialNo = versionTracker.LargestSeqNum() + 1;

            lock (versionTracker)
            {
                // Add exceptions to all uncommitted, but finished ops
                var dprTable = dprClient.GetDprFinder().ReadSnapshot();
                versionTracker.ResolveOperations(dprTable);
                foreach (var (s, _) in versionTracker)
                    currentCommitPoint.ExcludedSerialNos.Add(s);
            }

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