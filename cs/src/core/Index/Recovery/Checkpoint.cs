// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma warning disable 0162

//#define WAIT_FOR_INDEX_CHECKPOINT

using System;
using System.Threading;
using System.Threading.Tasks;

namespace FASTER.core
{
    /// <summary>
    /// Linked list (chain) of checkpoint info
    /// </summary>
    public struct LinkedCheckpointInfo
    {
        /// <summary>
        /// Next task in checkpoint chain
        /// </summary>
        public Task<LinkedCheckpointInfo> NextTask;
    }
    
    internal static class EpochPhaseIdx
    {
        public const int PrepareForIndexCheckpt = 0;
        public const int Prepare = 1;
        public const int InProgress = 2;
        public const int WaitPending = 3;
        public const int WaitFlush = 4;
        public const int CheckpointCompletionCallback = 5;
    }

    public partial class FasterKV<Key, Value, Input, Output, Context>
        where Key : new()
        where Value : new()
    {
        
        internal TaskCompletionSource<LinkedCheckpointInfo> checkpointTcs
            = new TaskCompletionSource<LinkedCheckpointInfo>(TaskCreationOptions.RunContinuationsAsynchronously);
            
        internal Guid _indexCheckpointToken;
        internal Guid _hybridLogCheckpointToken;
        internal HybridLogCheckpointInfo _hybridLogCheckpoint;

        internal Task<LinkedCheckpointInfo> CheckpointTask => checkpointTcs.Task;

        internal void AcquireSharedLatchesForAllPendingRequests(FasterExecutionContext ctx)
        {
            foreach (var _ctx in ctx.retryRequests)
            {
                AcquireSharedLatch(_ctx.key.Get());
            }

            foreach (var _ctx in ctx.ioPendingRequests.Values)
            {
                AcquireSharedLatch(_ctx.key.Get());
            }
        }
        
        internal void WriteHybridLogMetaInfo()
        {
            checkpointManager.CommitLogCheckpoint(_hybridLogCheckpointToken, _hybridLogCheckpoint.info.ToByteArray());
        }

        internal void WriteIndexMetaInfo()
        {
            checkpointManager.CommitIndexCheckpoint(_indexCheckpointToken, _indexCheckpoint.info.ToByteArray());
        }

        internal bool ObtainCurrentTailAddress(ref long location)
        {
            var tailAddress = hlog.GetTailAddress();
            return Interlocked.CompareExchange(ref location, tailAddress, 0) == 0;
        }

        internal void InitializeIndexCheckpoint(Guid indexToken)
        {
            _indexCheckpoint.Initialize(indexToken, state[resizeInfo.version].size, checkpointManager);
        }

        internal void InitializeHybridLogCheckpoint(Guid hybridLogToken, int version)
        {
            _hybridLogCheckpoint.Initialize(hybridLogToken, version, checkpointManager);
        }

        // #endregion
    }
}