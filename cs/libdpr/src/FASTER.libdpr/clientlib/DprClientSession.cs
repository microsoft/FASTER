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
            CheckWorldlineChange();
            snapshot = dprClient.GetDprFinder().GetStateSnapshot();
            var clientViewNumber = dprClient.GetDprViewNumber();
            if (seenDprViewNum >= clientViewNumber) return false;
            seenDprViewNum = clientViewNumber;
            return true;
        }

        // Should only be called on client threads, as it conveys worldline changes through exceptions.
        private void CheckWorldlineChange()
        {
            var systemWorldLine = dprClient.GetDprFinder().SystemWorldLine();
            // No failure
            if (clientWorldLine == systemWorldLine) return;
            Debug.Assert(clientWorldLine < systemWorldLine);
            clientWorldLine = dprClient.GetDprFinder().SystemWorldLine();
            var rollbackPoint = dprClient.GetDprFinder().GetStateSnapshot();
            batchTracker.HandleRollback();
            throw new DprRollbackException(rollbackPoint);
        }

        private unsafe long CopyDeps(ref DprBatchHeader header)
        {
            lock (deps)
            {
                fixed (byte* d = header.data)
                {
                    header.numAdditionalDeps = 0;
                    var copyHead = d + header.AdditionalDepsOffset;
                    foreach (var wv in deps)
                    {
                        Unsafe.AsRef<WorkerVersion>(copyHead) = wv;
                        header.numAdditionalDeps++;
                        copyHead += sizeof(WorkerVersion);
                    }
                    return copyHead - d;
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
        /// <returns> header that encodes tracking information (to be forwarded to batch destination)</returns>
        public ReadOnlySpan<byte> IssueBatch()
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
                    ref var dprHeader = ref Unsafe.AsRef<DprBatchHeader>(b);

                    // Populate header with relevant request information
                    dprHeader.srcWorkerId = Worker.INVALID;
                    dprHeader.worldLine = clientWorldLine;
                    dprHeader.version = clientVersion;
                    dprHeader.batchId = info.batchId;
                    // Populate tracking information into the batch
                    var len = CopyDeps(ref dprHeader);
                    return new Span<byte>(info.header, 0, (int) len);
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
        ///     An vector holding the version each operation in the batch executed at, if the sender of the batch
        ///     populated if (see DprBatchVersionTracker in DprServer). Operations are identified by  the offset they
        ///     appeared in the original batch. This object shares scope with the reply Memory and will no
        ///     longer be safe to access if reply is modified or deallocated.
        /// </param>
        /// <returns>whether it is safe to proceed with consuming the operation result</returns>
        public unsafe bool ResolveBatch(ReadOnlySpan<byte> reply, out DprBatchVersionVector versionVector)
        {
            versionVector = new DprBatchVersionVector(Span<byte>.Empty);

            fixed (byte* h = reply)
            {
                ref var responseHeader = ref Unsafe.AsRef<DprBatchHeader>(h);
                // If there is a world-line mismatch, need to throw away this batch
                if (responseHeader.worldLine != clientWorldLine)
                {
                    if (responseHeader.batchId != ClientBatchTracker.INVALID_BATCH_ID)
                        batchTracker.FinishBatch(responseHeader.batchId);
                    return false;
                }

                // Update versioning information
                core.Utility.MonotonicUpdate(ref clientVersion, responseHeader.version, out _);
                versionVector = new DprBatchVersionVector(reply);

                if (responseHeader.batchId != ClientBatchTracker.INVALID_BATCH_ID)
                {
                    var batchInfo = batchTracker.GetBatch(responseHeader.batchId);
                    fixed (byte* b = batchInfo.header)
                    {
                        ref var request = ref Unsafe.AsRef<DprBatchHeader>(b);
                        lock (deps)
                        {
                            fixed (byte* d = request.data)
                            {
                                var head = d + request.AdditionalDepsOffset;
                                // Remove all deps to save space. Future ops will implicitly depend on these by transitivity
                                for (var i = 0; i < request.numAdditionalDeps; i++)
                                {
                                    ref var wv = ref Unsafe.AsRef<WorkerVersion>(head);
                                    var succeed = deps.TryRemove(wv.Worker, wv.Version);
                                    // Because client sessions are single threaded this call must succeed. 
                                    Debug.Assert(succeed);
                                    head += sizeof(WorkerVersion);
                                }

                                // Add largest worker-version as dependency for future ops. Must add after removal in case this op has
                                // a version self-dependency (very likely) that would otherwise be erased
                                deps.Update(responseHeader.srcWorkerId, responseHeader.version);
                            }
                        }
                        // Free up this batch's tracking space
                        batchTracker.FinishBatch(responseHeader.batchId);
                    }
                }
            }

            return true;
        }
    }
}