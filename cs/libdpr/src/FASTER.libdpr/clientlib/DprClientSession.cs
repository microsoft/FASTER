using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using FASTER.core;

namespace FASTER.libdpr
{
    // TODO(Tianyu): Document
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
        public DprClientSession(Guid guid, DprClient dprClient, bool trackCommits)
        {
            this.guid = guid;
            this.dprClient = dprClient;
            versionTracker = new ClientVersionTracker();
            currentCommitPoint = new CommitPoint();
            deps = new LightDependencySet();
            batchTracker = new ClientBatchTracker();
            this.trackCommits = trackCommits;
        }

        // Should only be called on client threads, as it conveys worldline changes through exceptions.
        private void CheckWorldlineChange()
        {
            // Nothing to check if no dpr update
            if (!Utility.MonotonicUpdate(ref seenDprViewNum, dprClient.GetDprViewNumber(), out _)) return;
            // No failure
            if (clientWorldLine == dprClient.GetDprFinder().SystemWorldLine()) return;
            clientWorldLine = dprClient.GetDprFinder().SystemWorldLine();
            var result = new CommitPoint {UntilSerialNo = seqNum, ExcludedSerialNos = new List<long>()};
            versionTracker.HandleRollback(dprClient.GetDprFinder().ReadSnapshot(), ref result);
            batchTracker.HandleRollback(ref result);
            AdjustCommitPoint(ref result);
            throw new DprRollbackException(result);
        }

        public CommitPoint GetCommitPoint()
        {
            if (!trackCommits) throw new NotSupportedException();
            CheckWorldlineChange();
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
        
        // Should be called single-threadedly on client session thread with GetCommitPoint. Can be concurrent
        // with ResolveBatch calls.
        /// <summary>
        ///
        /// </summary>
        /// <param name="batchSize"></param>
        /// <param name="workerId"></param>
        /// <param name="header"></param>
        /// <returns></returns>
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