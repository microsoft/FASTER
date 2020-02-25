// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma warning disable 0162

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace FASTER.core
{
    public partial class FasterKV<Key, Value, Input, Output, Context, Functions> : FasterBase, IFasterKV<Key, Value, Input, Output, Context, Functions>
        where Key : new()
        where Value : new()
        where Functions : IFunctions<Key, Value, Input, Output, Context>
    {
        private readonly Functions functions;
        private readonly AllocatorBase<Key, Value> hlog;
        private readonly AllocatorBase<Key, Value> readcache;
        private readonly IFasterEqualityComparer<Key> comparer;

        private readonly bool UseReadCache = false;
        private readonly bool CopyReadsToTail = false;
        private readonly bool FoldOverSnapshot = false;
        private readonly int sectorSize;
        private readonly bool WriteDefaultOnDelete = false;
        private bool RelaxedCPR = false;

        /// <summary>
        /// Use relaxed version of CPR, where ops pending I/O
        /// are not part of CPR checkpoint. This mode allows
        /// us to eliminate the WAIT_PENDING phase, and allows
        /// sessions to be suspended. Do not modify during checkpointing.
        /// </summary>
        internal void UseRelaxedCPR() => RelaxedCPR = true;

        /// <summary>
        /// Number of used entries in hash index
        /// </summary>
        public long EntryCount => GetEntryCount();

        /// <summary>
        /// Size of index in #cache lines (64 bytes each)
        /// </summary>
        public long IndexSize => state[resizeInfo.version].size;

        /// <summary>
        /// Comparer used by FASTER
        /// </summary>
        public IFasterEqualityComparer<Key> Comparer => comparer;

        /// <summary>
        /// Hybrid log used by this FASTER instance
        /// </summary>
        public LogAccessor<Key, Value, Input, Output, Context, Functions> Log { get; }

        /// <summary>
        /// Read cache used by this FASTER instance
        /// </summary>
        public LogAccessor<Key, Value, Input, Output, Context, Functions> ReadCache { get; }

        private enum CheckpointType
        {
            INDEX_ONLY,
            HYBRID_LOG_ONLY,
            FULL
        }

        private CheckpointType _checkpointType;
        private Guid _indexCheckpointToken;
        private Guid _hybridLogCheckpointToken;
        private SystemState _systemState;
        private HybridLogCheckpointInfo _hybridLogCheckpoint;
        private ConcurrentDictionary<string, CommitPoint> _recoveredSessions;

        /// <summary>
        /// Create FASTER instance
        /// </summary>
        /// <param name="size">Size of core index (#cache lines)</param>
        /// <param name="comparer">FASTER equality comparer for key</param>
        /// <param name="variableLengthStructSettings"></param>
        /// <param name="functions">Callback functions</param>
        /// <param name="logSettings">Log settings</param>
        /// <param name="checkpointSettings">Checkpoint settings</param>
        /// <param name="serializerSettings">Serializer settings</param>
        public FasterKV(long size, Functions functions, LogSettings logSettings, CheckpointSettings checkpointSettings = null, SerializerSettings<Key, Value> serializerSettings = null, IFasterEqualityComparer<Key> comparer = null, VariableLengthStructSettings<Key, Value> variableLengthStructSettings = null)
        {
            if (comparer != null)
                this.comparer = comparer;
            else
            {
                if (typeof(IFasterEqualityComparer<Key>).IsAssignableFrom(typeof(Key)))
                {
                    this.comparer = new Key() as IFasterEqualityComparer<Key>;
                }
                else
                {
                    Console.WriteLine("***WARNING*** Creating default FASTER key equality comparer based on potentially slow EqualityComparer<Key>.Default. To avoid this, provide a comparer (IFasterEqualityComparer<Key>) as an argument to FASTER's constructor, or make Key implement the interface IFasterEqualityComparer<Key>");
                    this.comparer = FasterEqualityComparer<Key>.Default;
                }
            }

            if (checkpointSettings == null)
                checkpointSettings = new CheckpointSettings();

            if (checkpointSettings.CheckpointDir != null && checkpointSettings.CheckpointManager != null)
                throw new FasterException("Specify either CheckpointManager or CheckpointDir for CheckpointSettings, not both");

            checkpointManager = checkpointSettings.CheckpointManager ?? new LocalCheckpointManager(checkpointSettings.CheckpointDir ?? "");

            FoldOverSnapshot = checkpointSettings.CheckPointType == core.CheckpointType.FoldOver;
            CopyReadsToTail = logSettings.CopyReadsToTail;
            this.functions = functions;

            if (logSettings.ReadCacheSettings != null)
            {
                CopyReadsToTail = false;
                UseReadCache = true;
            }

            if (Utility.IsBlittable<Key>() && Utility.IsBlittable<Value>())
            {
                if (variableLengthStructSettings != null)
                {
                    hlog = new VariableLengthBlittableAllocator<Key, Value>(logSettings, variableLengthStructSettings, this.comparer, null, epoch);
                    Log = new LogAccessor<Key, Value, Input, Output, Context, Functions>(this, hlog);
                    if (UseReadCache)
                    {
                        readcache = new VariableLengthBlittableAllocator<Key, Value>(
                            new LogSettings
                            {
                                PageSizeBits = logSettings.ReadCacheSettings.PageSizeBits,
                                MemorySizeBits = logSettings.ReadCacheSettings.MemorySizeBits,
                                SegmentSizeBits = logSettings.ReadCacheSettings.MemorySizeBits,
                                MutableFraction = 1 - logSettings.ReadCacheSettings.SecondChanceFraction
                            }, variableLengthStructSettings, this.comparer, ReadCacheEvict, epoch);
                        readcache.Initialize();
                        ReadCache = new LogAccessor<Key, Value, Input, Output, Context, Functions>(this, readcache);
                    }
                }
                else
                {
                    hlog = new BlittableAllocator<Key, Value>(logSettings, this.comparer, null, epoch);
                    Log = new LogAccessor<Key, Value, Input, Output, Context, Functions>(this, hlog);
                    if (UseReadCache)
                    {
                        readcache = new BlittableAllocator<Key, Value>(
                            new LogSettings
                            {
                                PageSizeBits = logSettings.ReadCacheSettings.PageSizeBits,
                                MemorySizeBits = logSettings.ReadCacheSettings.MemorySizeBits,
                                SegmentSizeBits = logSettings.ReadCacheSettings.MemorySizeBits,
                                MutableFraction = 1 - logSettings.ReadCacheSettings.SecondChanceFraction
                            }, this.comparer, ReadCacheEvict, epoch);
                        readcache.Initialize();
                        ReadCache = new LogAccessor<Key, Value, Input, Output, Context, Functions>(this, readcache);
                    }
                }
            }
            else
            {
                WriteDefaultOnDelete = true;

                hlog = new GenericAllocator<Key, Value>(logSettings, serializerSettings, this.comparer, null, epoch);
                Log = new LogAccessor<Key, Value, Input, Output, Context, Functions>(this, hlog);
                if (UseReadCache)
                {
                    readcache = new GenericAllocator<Key, Value>(
                        new LogSettings
                        {
                            PageSizeBits = logSettings.ReadCacheSettings.PageSizeBits,
                            MemorySizeBits = logSettings.ReadCacheSettings.MemorySizeBits,
                            SegmentSizeBits = logSettings.ReadCacheSettings.MemorySizeBits,
                            MutableFraction = 1 - logSettings.ReadCacheSettings.SecondChanceFraction
                        }, serializerSettings, this.comparer, ReadCacheEvict, epoch);
                    readcache.Initialize();
                    ReadCache = new LogAccessor<Key, Value, Input, Output, Context, Functions>(this, readcache);
                }
            }

            hlog.Initialize();

            sectorSize = (int)logSettings.LogDevice.SectorSize;
            Initialize(size, sectorSize);

            _systemState = default;
            _systemState.phase = Phase.REST;
            _systemState.version = 1;
            _checkpointType = CheckpointType.HYBRID_LOG_ONLY;
        }

        /// <summary>
        /// Initiate full checkpoint
        /// </summary>
        /// <param name="token">Checkpoint token</param>
        /// <returns>
        /// Whether we successfully initiated the checkpoint (initiation may
        /// fail if we are already taking a checkpoint or performing some other
        /// operation such as growing the index).
        /// </returns>
        public bool TakeFullCheckpoint(out Guid token)
        {
            if (InternalTakeCheckpoint(CheckpointType.FULL))
            {
                token = _indexCheckpointToken;
                return true;
            }
            else
            {
                token = default;
                return false;
            }
        }

        /// <summary>
        /// Initiate index checkpoint
        /// </summary>
        /// <param name="token">Checkpoint token</param>
        /// <returns>Whether we could initiate the checkpoint</returns>
        public bool TakeIndexCheckpoint(out Guid token)
        {
            if (InternalTakeCheckpoint(CheckpointType.INDEX_ONLY))
            {
                token = _indexCheckpointToken;
                return true;
            }
            else
            {
                token = default;
                return false;
            }
        }

        /// <summary>
        /// Take hybrid log checkpoint
        /// </summary>
        /// <param name="token">Checkpoint token</param>
        /// <returns>Whether we could initiate the checkpoint</returns>
        public bool TakeHybridLogCheckpoint(out Guid token)
        {
            if (InternalTakeCheckpoint(CheckpointType.HYBRID_LOG_ONLY))
            {
                token = _hybridLogCheckpointToken;
                return true;
            }
            else
            {
                token = default;
                return false;
            }
        }

        /// <summary>
        /// Recover from the latest checkpoints
        /// </summary>
        public void Recover()
        {
            InternalRecoverFromLatestCheckpoints();
        }

        /// <summary>
        /// Recover
        /// </summary>
        /// <param name="fullCheckpointToken"></param>
        public void Recover(Guid fullCheckpointToken)
        {
            InternalRecover(fullCheckpointToken, fullCheckpointToken);
        }

        /// <summary>
        /// Recover
        /// </summary>
        /// <param name="indexCheckpointToken"></param>
        /// <param name="hybridLogCheckpointToken"></param>
        public void Recover(Guid indexCheckpointToken, Guid hybridLogCheckpointToken)
        {
            InternalRecover(indexCheckpointToken, hybridLogCheckpointToken);
        }

        /// <summary>
        /// Wait for ongoing checkpoint to complete
        /// </summary>
        /// <returns></returns>
        public async ValueTask CompleteCheckpointAsync(CancellationToken token = default)
        {
            if (LightEpoch.AnyInstanceProtected())
                throw new FasterException("Cannot use CompleteCheckpointAsync when using legacy or non-async sessions");

            token.ThrowIfCancellationRequested();

            while (true)
            {
                var systemState = _systemState;
                if (systemState.phase == Phase.REST || systemState.phase == Phase.PREPARE_GROW || systemState.phase == Phase.IN_PROGRESS_GROW)
                    return;

                await HandleCheckpointingPhasesAsync(null, null);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status ContextRead(ref Key key, ref Input input, ref Output output, Context context, long serialNo, FasterExecutionContext sessionCtx)
        {
            var pcontext = default(PendingContext);
            var internalStatus = InternalRead(ref key, ref input, ref output, ref context, ref pcontext, sessionCtx, serialNo);
            Status status;
            if (internalStatus == OperationStatus.SUCCESS || internalStatus == OperationStatus.NOTFOUND)
            {
                status = (Status)internalStatus;
            }
            else
            {
                status = HandleOperationStatus(sessionCtx, sessionCtx, pcontext, internalStatus);
            }
            sessionCtx.serialNum = serialNo;
            return status;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal ValueTask<(Status, Output)> ReadAsync(
            ClientSession<Key, Value, Input, Output, Context, Functions> clientSession,
            ref Key key, ref Input input, Context context = default, CancellationToken token = default)
        {
            var pcontext = default(PendingContext);
            Output output = default;

            clientSession.ctx.serialNum++;

            OperationStatus internalStatus;

            if (clientSession.SupportAsync) clientSession.UnsafeResumeThread();
            try
            {
                do
                {
                    internalStatus = InternalRead(ref key, ref input, ref output,
                    ref context, ref pcontext, clientSession.ctx, clientSession.ctx.serialNum);
                }
                while (internalStatus == OperationStatus.CPR_SHIFT_DETECTED);
            }
            finally
            {
                if (clientSession.SupportAsync) clientSession.UnsafeSuspendThread();
            }

            switch (internalStatus)
            {
                case OperationStatus.SUCCESS:
                case OperationStatus.NOTFOUND:

                    return new ValueTask<(Status, Output)>(((Status)internalStatus, output));

                case OperationStatus.RECORD_ON_DISK:
                    return SlowReadAsync(this, clientSession, pcontext, token);
                    
                
                default:
                    throw new Exception($"Unexpected {nameof(OperationStatus)} while reading => {internalStatus}");

            }

            static async ValueTask<(Status, Output)> SlowReadAsync(
                FasterKV<Key, Value, Input, Output, Context, Functions> @this,
                ClientSession<Key, Value, Input, Output, Context, Functions> clientSession,
                PendingContext pendingContext, CancellationToken token = default)
            {

                var diskRequest = @this.ScheduleGetFromDisk(clientSession.ctx, pendingContext);

                token.ThrowIfCancellationRequested();

                if (@this.epoch.ThisInstanceProtected())
                    throw new NotSupportedException("Async operations not supported over protected epoch");

                await diskRequest.ValueTaskOfT;

                lock (clientSession.ctx)
                {

                    if (clientSession.SupportAsync) clientSession.UnsafeResumeThread();
                    try
                    {
                        #region RelaxedCPR
                        if (!@this.RelaxedCPR)
                        {
                            if (clientSession.ctx.phase == Phase.IN_PROGRESS
                                || clientSession.ctx.phase == Phase.WAIT_PENDING)
                            {
                                return @this.InternalCompleteIOPendingReadRequestsAsync(
                                    clientSession.ctx.prevCtx, clientSession.ctx, diskRequest, pendingContext);
                            }
                        }
                        #endregion

                        return @this.InternalCompleteIOPendingReadRequestsAsync(
                            clientSession.ctx, clientSession.ctx, diskRequest, pendingContext);
                    }
                    finally
                    {
                        if (clientSession.SupportAsync) clientSession.UnsafeSuspendThread();
                    }
                }

            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status ContextUpsert(ref Key key, ref Value value, Context context, long serialNo, FasterExecutionContext sessionCtx)
        {
            var pcontext = default(PendingContext);
            var internalStatus = InternalUpsert(ref key, ref value, ref context, ref pcontext, sessionCtx, serialNo);
            Status status;

            if (internalStatus == OperationStatus.SUCCESS || internalStatus == OperationStatus.NOTFOUND)
            {
                status = (Status)internalStatus;
            }
            else
            {
                status = HandleOperationStatus(sessionCtx, sessionCtx, pcontext, internalStatus);
            }
            sessionCtx.serialNum = serialNo;
            return status;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status ContextRMW(ref Key key, ref Input input, Context context, long serialNo, FasterExecutionContext sessionCtx)
        {
            var pcontext = default(PendingContext);
            var internalStatus = InternalRMW(ref key, ref input, ref context, ref pcontext, sessionCtx, serialNo);
            Status status;
            if (internalStatus == OperationStatus.SUCCESS || internalStatus == OperationStatus.NOTFOUND)
            {
                status = (Status)internalStatus;
            }
            else
            {
                status = HandleOperationStatus(sessionCtx, sessionCtx, pcontext, internalStatus);
            }
            sessionCtx.serialNum = serialNo;
            return status;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status ContextDelete(ref Key key, Context context, long serialNo, FasterExecutionContext sessionCtx)
        {
            var pcontext = default(PendingContext);
            var internalStatus = InternalDelete(ref key, ref context, ref pcontext, sessionCtx, serialNo);
            var status = default(Status);
            if (internalStatus == OperationStatus.SUCCESS || internalStatus == OperationStatus.NOTFOUND)
            {
                status = (Status)internalStatus;
            }
            sessionCtx.serialNum = serialNo;
            return status;
        }


        /// <summary>
        /// Grow the hash index
        /// </summary>
        /// <returns>Whether the request succeeded</returns>
        public bool GrowIndex()
        {
            return InternalGrowIndex();
        }

        /// <summary>
        /// Dispose FASTER instance
        /// </summary>
        public void Dispose()
        {
            base.Free();
            LegacyDispose();
            hlog.Dispose();
            readcache?.Dispose();
        }
    }
}
