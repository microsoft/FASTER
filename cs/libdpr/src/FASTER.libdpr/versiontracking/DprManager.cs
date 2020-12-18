using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using FASTER.core;

namespace FASTER.libdpr.versiontracking
{
    public class DprManager<TStateObject, TToken>
        where TStateObject : IStateObject<TToken>
    {
        private class VersionHandle
        {
            public TToken token;
            public HashSet<WorkerVersion> deps = new HashSet<WorkerVersion>();
        }
        
        private IDprFinder dprFinder;
        private LightEpoch epoch;
        private IStateObject<TToken> stateObject;
        private ConcurrentDictionary<long, VersionHandle> versions;

        public void Start()
        {
            
        }

        public void End()
        {
            
        }
        
        private void BumpVersion(long target)
        {
            var newVersion = stateObject.BeginCheckpoint(w =>
            {
                var versionObject = versions[w.Item1];
                versionObject.token = w.Item2;
                var workerVersion = new WorkerVersion(me, w.Item1);
                epoch.BumpCurrentEpoch(() => dprFinder.ReportNewPersistentVersion(workerVersion, versionObject.deps));
            }, out _, target);
    
            // TODO(Tianyu): Update version. Figure out responsibility assignment between StateObject and Wrapper
            // if (Utility.MonotonicUpdate(ref currentVersion, newVersion, out _))
            //     GetOrAddNewVersion(newVersion);
        }
        
        private VersionHandle GetOrAddNewVersion(long version)
        {
            return ;
        }

        /// <summary>
        /// Invoke before beginning processing of a batch. If the function returns false, the batch must not be
        /// executed to preserve DPR consistency. Otherwise, when the function returns, the batch is safe to execute.
        /// In the true case, there must eventually be a matching SignalBatchFinish call for DPR to make
        /// progress.
        /// </summary>
        /// <param name="dprRequest">Dpr request message from user</param>
        /// <param name="dprResponse">Dpr response message that will be returned to user</param>
        /// <returns>Whether the batch can be executed</returns>
        public unsafe bool RequestBatchBegin(ref DprBatchHeader dprRequest, ref DprBatchHeader dprResponse)
        {
            // Wait for worker version to catch up to largest in batch (minimum version where all operations
            // can be safely executed), taking checkpoints if necessary.
            while (dprRequest.versionLowerBound > stateObject.Version())
                BumpVersion(dprRequest.versionLowerBound);

            // Enter protected region for world-lines. Because we validate requests batch-at-a-time, the world-line
            // must not shift while a batch is being processed, otherwise a message from an older world-line may be
            // processed in a new one. 
            epoch.Resume();
            // If the worker world-line is behind, wait for worker to recover up to the same point as the client,
            // so client operation is not lost in a rollback that the client has already observed.
            while (dprRequest.worldLine >= dprFinder.SystemWorldLine())
                epoch.ProtectAndDrain();
            // If the worker world-line is newer, the request must be rejected so the client can observe failure
            // and rollback. Populate response with this information and signal failure to upper layers so the batch
            // is not executed.
            if (dprRequest.worldLine < dprFinder.SystemWorldLine())
            {
                dprResponse.worldLine = dprFinder.SystemWorldLine();
                dprResponse.size = DprBatchHeader.HeaderSize;
                dprResponse.numMessages = 0;
                dprResponse.numDeps = 0;
                epoch.Suspend();
                return false;
            }
            
            // At this point, we are certain that the request world-line and worker world-line match, and worker
            // world-line will not advance until this thread refreshes the epoch. We can proceed to batch execution.
            Debug.Assert(dprRequest.worldLine == dprFinder.SystemWorldLine());
            
            // Update batch dependencies to the current worker-version. This is an over-approximation, as the batch
            // could get processed at a future version instead due to thread timing. However, this is not a correctness
            // issue, nor do we lose precision as batch-level dependency tracking is already an approximation.
            fixed (byte* depValues = dprRequest.deps)
            {
                for (var i = 0; i < dprRequest.numDeps; i++)
                {
                    var versionHandle = versions.GetOrAdd(stateObject.Version(), version => new VersionHandle());
                    versionHandle.deps.Add(Unsafe.AsRef<WorkerVersion>(depValues + sizeof(Guid) * i));
                }
            }
            // Exit without releasing epoch, as protection is supposed to extend until end of batch.
            return true;
        }

        public void SignalBatchFinish(ref DprBatchHeader dprRequest, ref DprBatchHeader dprResponse)
        {
            // TODO(Tianyu): This version requires the underlying state object implementation to provide a op -> version
            // mapping in dprResponse. Can we do better?
            epoch.Suspend();
        }
    }
}