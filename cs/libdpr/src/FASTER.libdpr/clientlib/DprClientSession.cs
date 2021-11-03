using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;

namespace FASTER.libdpr
{
    /// <summary>
    ///     Corresponds to a client session that is a single logical thread of execution in the system.
    /// </summary>
    public class DprClientSession
    {
        private readonly ClientBatchTracker batchTracker;
        private long clientVersion = 1, clientWorldLine = 1;

        private readonly LightDependencySet deps;
        private readonly DprClient dprClient;
        private readonly Guid guid;
        private long seenDprViewNum;

        internal DprClientSession(Guid guid, DprClient dprClient)
        {
            this.guid = guid;
            this.dprClient = dprClient;
            deps = new LightDependencySet();
            batchTracker = new ClientBatchTracker();
        }

        /// <summary>
        ///     Get the current DPR cut. Returns true if it was different from the response given last time. 
        /// </summary>
        /// <param name="snapshot"> the new cut </param>
        /// <returns> whether the cut was different from last time </returns>
        public bool TryGetCurrentCut(out IDprStateSnapshot snapshot)
        {
            snapshot = dprClient.GetDprFinder().GetStateSnapshot();
            var clientViewNumber = dprClient.GetDprViewNumber();
            if (seenDprViewNum >= clientViewNumber) return false;
            seenDprViewNum = clientViewNumber;
            return true;
        }

        // Should only be called on client threads, as it conveys worldline changes through exceptions.
        private void CheckWorldlineChange()
        {
            // No failure
            if (clientWorldLine == dprClient.GetDprFinder().SystemWorldLine()) return;
            clientWorldLine = dprClient.GetDprFinder().SystemWorldLine();
            var rollbackPoint = dprClient.GetDprFinder().GetStateSnapshot();
            batchTracker.HandleRollback();
            throw new DprRollbackException(rollbackPoint);
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
        ///     Add a batch of requests to DPR tracking, with the given number of requests issued to the given worker ID.
        ///     This method should be called before the client sends a batch.
        ///     Operations within the issued batch are numbered with ids in [return_value, return_value + batch_size) in
        ///     order for tracking.
        ///     Not thread-safe except with ResolveBatch.
        /// </summary>
        /// <param name="batchSize">size of the batch to issue</param>
        /// <param name="workerId"> destination of the batch</param>
        /// <param name="header"> header that encodes tracking information (to be forwarded to batch destination)</param>
        public void IssueBatch(int batchSize, Worker workerId, out Span<byte> header)
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
        }

        /// <summary>
        ///     Consumes a DPR batch reply and update tracking information. This method should be called before exposing
        ///     batch result; if the method returns false, the results are rolled back and should be discarded. Returns a
        ///     versionVector that allows callers to inspect the version each operation executed at, in the same order
        ///     operations appeared in the batch.
        ///     Thread-safe to invoke with other methods of the object.
        /// </summary>
        /// <param name="reply">The received reply</param>
        /// <param name="versionVector">
        ///     An IEnumerable holding the version each operation in the batch executed at. Operations are identified by
        ///     the offset they appeared in the original batch. This object shares scope with the reply Memory and will no
        ///     longer be safe to access if reply is moved, modified, or deallocated.
        /// </param>
        /// <returns>whether it is safe to proceed with consuming the operation result</returns>
        public unsafe bool ResolveBatch(ReadOnlySpan<byte> reply, out DprBatchVersionVector versionVector)
        {
            versionVector = new DprBatchVersionVector(Span<byte>.Empty);

            fixed (byte* h = reply)
            {
                ref var responseHeader = ref Unsafe.AsRef<DprBatchResponseHeader>(h);
                var batchInfo = batchTracker.GetBatch(responseHeader.batchId);

                fixed (byte* b = batchInfo.header)
                {
                    ref var request = ref Unsafe.AsRef<DprBatchRequestHeader>(b);
                    // Remote machine would not have executed a batch if the world-lines are mismatched
                    if (responseHeader.worldLine != request.worldLine
                        // These replies are from an earlier world-line that the client has already moved past. We would have
                        // declared these ops lost, and the remote server would have rolled back. It is ok to simply
                        // disregard these replies;
                        || responseHeader.worldLine < clientWorldLine)
                    {
                        // In this case, the remote machine has seen more failures than the client and the client needs to 
                        // catch up 
                        if (responseHeader.worldLine < 0)
                            // TODO(Tianyu): Trigger recovery eagerly if possible
                            // Do nothing for now and let client thread will find out about the failure at its own pace
                            return false;

                        // Otherwise, we would have declared these ops lost, and the remote server
                        // would have rolled back. It is ok to simply disregard these replies
                        batchTracker.FinishBatch(responseHeader.batchId);
                        return false;
                    }

                    // Update versioning information
                    // TODO(Tianyu): Not necessary to iterate through all of the vector, can probably do an optimization
                    // to compute the max on the sender side to avoid iteration. 
                    long maxVersion = 0;
                    versionVector = new DprBatchVersionVector(reply);
                    for (var i = 0; i < versionVector.Count; i++)
                        maxVersion = Math.Max(maxVersion, versionVector[i]);
                    core.Utility.MonotonicUpdate(ref clientVersion, maxVersion, out _);
                    versionVector = new DprBatchVersionVector(reply);

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
                    batchTracker.FinishBatch(responseHeader.batchId);
                }
            }

            return true;
        }

        /// <summary>
        ///      Consumes a DPR subscription batch reply and update tracking information. This method should be called
        ///      before exposing the batch; if the method returns false, the results are rolled back and should be
        ///      discarded. Returns a versionVector that allows callers to inspect the version each operation executed
        ///      at, in the same order operations appeared in the batch.
        ///     Thread-safe to invoke with other methods of the object.
        /// </summary>
        /// <param name="src"> Id of the work this batch is from </param>
        /// <param name="header">The received subscription batch header</param>
        /// <param name="versionVector">
        ///     An IEnumerable holding the version each operation in the batch executed at. Operations are identified by
        ///     the offset they appeared in the original batch. This object shares scope with the reply Memory and will no
        ///     longer be safe to access if reply is moved, modified, or deallocated.
        /// </param>
        /// <returns>whether it is safe to proceed with consuming the operation result</returns>
        public unsafe bool ResolveSubscriptionBatch(Worker src, ReadOnlySpan<byte> header,
            out DprBatchVersionVector versionVector)
        {
            CheckWorldlineChange();
            // Wait for a batch slot to become available
            BatchInfo info;
            // TODO(Tianyu): Probably rewrite with async
            while (!batchTracker.TryGetBatchInfo(out info))
                Thread.Yield();
            info.workerId = src;

            fixed (byte* h = header)
            {
                ref var responseHeader = ref Unsafe.AsRef<DprBatchResponseHeader>(h);
                responseHeader.batchId = info.batchId;
            }

            return ResolveBatch(header, out versionVector);
        }
    }
}