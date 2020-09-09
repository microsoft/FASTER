// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.
#pragma warning disable 0162

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace FASTER.core
{
    public partial class FasterKV<Key, Value> : FasterBase,
        IFasterKV<Key, Value>
    {
        internal readonly AllocatorBase<Key, Value> hlog;
        private readonly AllocatorBase<Key, Value> readcache;
        private readonly IFasterEqualityComparer<Key> comparer;

        internal readonly bool UseReadCache;
        private readonly bool CopyReadsToTail;
        private readonly bool FoldOverSnapshot;
        internal readonly int sectorSize;
        private readonly bool WriteDefaultOnDelete;
        internal bool RelaxedCPR;

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
        public LogAccessor<Key, Value> Log { get; }

        /// <summary>
        /// Read cache used by this FASTER instance
        /// </summary>
        public LogAccessor<Key, Value> ReadCache { get; }

        internal ConcurrentDictionary<string, CommitPoint> _recoveredSessions;

        /// <summary>
        /// Create FASTER instance
        /// </summary>
        /// <param name="size">Size of core index (#cache lines)</param>
        /// <param name="comparer">FASTER equality comparer for key</param>
        /// <param name="variableLengthStructSettings"></param>
        /// <param name="logSettings">Log settings</param>
        /// <param name="checkpointSettings">Checkpoint settings</param>
        /// <param name="serializerSettings">Serializer settings</param>
        public FasterKV(long size, LogSettings logSettings,
            CheckpointSettings checkpointSettings = null, SerializerSettings<Key, Value> serializerSettings = null,
            IFasterEqualityComparer<Key> comparer = null,
            VariableLengthStructSettings<Key, Value> variableLengthStructSettings = null)
        {
            if (comparer != null)
                this.comparer = comparer;
            else
            {
                if (typeof(IFasterEqualityComparer<Key>).IsAssignableFrom(typeof(Key)))
                {
                    if (default(Key) != null)
                    {
                        this.comparer = default(Key) as IFasterEqualityComparer<Key>;
                    }
                    else if (typeof(Key).GetConstructor(Type.EmptyTypes) != null)
                    {
                        this.comparer = Activator.CreateInstance(typeof(Key)) as IFasterEqualityComparer<Key>;
                    }
                }
                else
                {
                    this.comparer = FasterEqualityComparer.Get<Key>();
                }
            }

            if (checkpointSettings == null)
                checkpointSettings = new CheckpointSettings();

            if (checkpointSettings.CheckpointDir != null && checkpointSettings.CheckpointManager != null)
                throw new FasterException(
                    "Specify either CheckpointManager or CheckpointDir for CheckpointSettings, not both");

            bool oldCheckpointManager = false;

            if (oldCheckpointManager)
            {
                checkpointManager = checkpointSettings.CheckpointManager ??
                                new LocalCheckpointManager(checkpointSettings.CheckpointDir ?? "");
            }
            else
            {
                checkpointManager = checkpointSettings.CheckpointManager ??
                    new DeviceLogCommitCheckpointManager
                    (new LocalStorageNamedDeviceFactory(),
                        new DefaultCheckpointNamingScheme(
                          new DirectoryInfo(checkpointSettings.CheckpointDir ?? ".").FullName));
            }

            if (checkpointSettings.CheckpointManager == null)
                disposeCheckpointManager = true;

            FoldOverSnapshot = checkpointSettings.CheckPointType == core.CheckpointType.FoldOver;
            CopyReadsToTail = logSettings.CopyReadsToTail;

            if (logSettings.ReadCacheSettings != null)
            {
                CopyReadsToTail = false;
                UseReadCache = true;
            }

            if (Utility.IsBlittable<Key>() && Utility.IsBlittable<Value>())
            {
                if (variableLengthStructSettings != null)
                {
                    hlog = new VariableLengthBlittableAllocator<Key, Value>(logSettings, variableLengthStructSettings,
                        this.comparer, null, epoch);
                    Log = new LogAccessor<Key, Value>(this, hlog);
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
                        ReadCache = new LogAccessor<Key, Value>(this, readcache);
                    }
                }
                else
                {
                    hlog = new BlittableAllocator<Key, Value>(logSettings, this.comparer, null, epoch);
                    Log = new LogAccessor<Key, Value>(this, hlog);
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
                        ReadCache = new LogAccessor<Key, Value>(this, readcache);
                    }
                }
            }
            else
            {
                WriteDefaultOnDelete = true;

                hlog = new GenericAllocator<Key, Value>(logSettings, serializerSettings, this.comparer, null, epoch);
                Log = new LogAccessor<Key, Value>(this, hlog);
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
                    ReadCache = new LogAccessor<Key, Value>(this, readcache);
                }
            }

            hlog.Initialize();

            sectorSize = (int)logSettings.LogDevice.SectorSize;
            Initialize(size, sectorSize);

            systemState = default;
            systemState.phase = Phase.REST;
            systemState.version = 1;
        }

        /// <summary>
        /// Initiate full checkpoint
        /// </summary>
        /// <param name="token">Checkpoint token</param>
        /// <returns>
        /// Whether we successfully initiated the checkpoint (initiation may
        /// fail if we are already taking a checkpoint or performing some other
        /// operation such as growing the index). Use CompleteCheckpointAsync to wait completion.
        /// </returns>
        public bool TakeFullCheckpoint(out Guid token)
        {
            ISynchronizationTask backend;
            if (FoldOverSnapshot)
                backend = new FoldOverCheckpointTask();
            else
                backend = new SnapshotCheckpointTask();

            var result = StartStateMachine(new FullCheckpointStateMachine(backend, -1));
            token = _hybridLogCheckpointToken;
            return result;
        }

        /// <summary>
        /// Initiate full checkpoint
        /// </summary>
        /// <param name="token">Checkpoint token</param>
        /// <param name="checkpointType">Checkpoint type</param>
        /// <returns>
        /// Whether we successfully initiated the checkpoint (initiation may
        /// fail if we are already taking a checkpoint or performing some other
        /// operation such as growing the index). Use CompleteCheckpointAsync to wait completion.
        /// </returns>
        public bool TakeFullCheckpoint(out Guid token, CheckpointType checkpointType)
        {
            ISynchronizationTask backend;
            if (checkpointType == CheckpointType.FoldOver)
                backend = new FoldOverCheckpointTask();
            else if (checkpointType == CheckpointType.Snapshot)
                backend = new SnapshotCheckpointTask();
            else
                throw new FasterException("Unsupported full checkpoint type");

            var result = StartStateMachine(new FullCheckpointStateMachine(backend, -1));
            if (result)
                token = _hybridLogCheckpointToken;
            else
                token = default;
            return result;
        }

        /// <summary>
        /// Take full (index + log) checkpoint
        /// </summary>
        /// <param name="checkpointType">Checkpoint type</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>
        /// (bool success, Guid token)
        /// success: Whether we successfully initiated the checkpoint (initiation may
        /// fail if we are already taking a checkpoint or performing some other
        /// operation such as growing the index).
        /// token: Token for taken checkpoint
        /// Await task to complete checkpoint, if initiated successfully
        /// </returns>
        public async ValueTask<(bool success, Guid token)> TakeFullCheckpointAsync(CheckpointType checkpointType, CancellationToken cancellationToken = default)
        {
            var success = TakeFullCheckpoint(out Guid token, checkpointType);

            if (success)
                await CompleteCheckpointAsync(cancellationToken);

            return (success, token);
        }

        /// <summary>
        /// Initiate index-only checkpoint
        /// </summary>
        /// <param name="token">Checkpoint token</param>
        /// <returns>Whether we could initiate the checkpoint. Use CompleteCheckpointAsync to wait completion.</returns>
        public bool TakeIndexCheckpoint(out Guid token)
        {
            var result = StartStateMachine(new IndexSnapshotStateMachine());
            token = _indexCheckpointToken;
            return result;
        }

        /// <summary>
        /// Take index-only checkpoint
        /// </summary>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>
        /// (bool success, Guid token)
        /// success: Whether we successfully initiated the checkpoint (initiation may
        /// fail if we are already taking a checkpoint or performing some other
        /// operation such as growing the index).
        /// token: Token for taken checkpoint
        /// Await task to complete checkpoint, if initiated successfully
        /// </returns>
        public async ValueTask<(bool success, Guid token)> TakeIndexCheckpointAsync(CancellationToken cancellationToken = default)
        {
            var success = TakeIndexCheckpoint(out Guid token);

            if (success)
                await CompleteCheckpointAsync(cancellationToken);

            return (success, token);
        }

        /// <summary>
        /// Initiate log-only checkpoint
        /// </summary>
        /// <param name="token">Checkpoint token</param>
        /// <returns>Whether we could initiate the checkpoint. Use CompleteCheckpointAsync to wait completion.</returns>
        public bool TakeHybridLogCheckpoint(out Guid token)
        {
            ISynchronizationTask backend;
            if (FoldOverSnapshot)
                backend = new FoldOverCheckpointTask();
            else
                backend = new SnapshotCheckpointTask();

            var result = StartStateMachine(new HybridLogCheckpointStateMachine(backend, -1));
            token = _hybridLogCheckpointToken;
            return result;
        }

        /// <summary>
        /// Initiate log-only checkpoint
        /// </summary>
        /// <param name="token">Checkpoint token</param>
        /// <param name="checkpointType">Checkpoint type</param>
        /// <returns>Whether we could initiate the checkpoint. Use CompleteCheckpointAsync to wait completion.</returns>
        public bool TakeHybridLogCheckpoint(out Guid token, CheckpointType checkpointType)
        {
            ISynchronizationTask backend;
            if (checkpointType == CheckpointType.FoldOver)
                backend = new FoldOverCheckpointTask();
            else if (checkpointType == CheckpointType.Snapshot)
                backend = new SnapshotCheckpointTask();
            else
                throw new FasterException("Unsupported checkpoint type");

            var result = StartStateMachine(new HybridLogCheckpointStateMachine(backend, -1));
            token = _hybridLogCheckpointToken;
            return result;
        }

        /// <summary>
        /// Take log-only checkpoint
        /// </summary>
        /// <param name="checkpointType">Checkpoint type</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>
        /// (bool success, Guid token)
        /// success: Whether we successfully initiated the checkpoint (initiation may
        /// fail if we are already taking a checkpoint or performing some other
        /// operation such as growing the index).
        /// token: Token for taken checkpoint
        /// Await task to complete checkpoint, if initiated successfully
        /// </returns>
        public async ValueTask<(bool success, Guid token)> TakeHybridLogCheckpointAsync(CheckpointType checkpointType, CancellationToken cancellationToken = default)
        {
            var success = TakeHybridLogCheckpoint(out Guid token, checkpointType);

            if (success)
                await CompleteCheckpointAsync(cancellationToken);

            return (success, token);
        }

        /// <summary>
        /// Recover from the latest checkpoint (blocking operation)
        /// </summary>
        /// <param name="numPagesToPreload"></param>
        public void Recover(int numPagesToPreload = -1)
        {
            InternalRecoverFromLatestCheckpoints(numPagesToPreload);
        }

        /// <summary>
        /// Recover from specific token (blocking operation)
        /// </summary>
        /// <param name="fullCheckpointToken">Token</param>
        /// <param name="numPagesToPreload">Number of pages to preload into memory after recovery</param>
        public void Recover(Guid fullCheckpointToken, int numPagesToPreload = -1)
        {
            InternalRecover(fullCheckpointToken, fullCheckpointToken, numPagesToPreload);
        }

        /// <summary>
        /// Recover from specific index and log token (blocking operation)
        /// </summary>
        /// <param name="indexCheckpointToken"></param>
        /// <param name="hybridLogCheckpointToken"></param>
        /// <param name="numPagesToPreload">Number of pages to preload into memory after recovery</param>
        public void Recover(Guid indexCheckpointToken, Guid hybridLogCheckpointToken, int numPagesToPreload = -1)
        {
            InternalRecover(indexCheckpointToken, hybridLogCheckpointToken, numPagesToPreload);
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
                var systemState = this.systemState;
                if (systemState.phase == Phase.REST || systemState.phase == Phase.PREPARE_GROW ||
                    systemState.phase == Phase.IN_PROGRESS_GROW)
                    return;

                List<ValueTask> valueTasks = new List<ValueTask>();
                
                ThreadStateMachineStep<Empty, Empty, Empty, NullFasterSession>(null, NullFasterSession.Instance, valueTasks, token);

                if (valueTasks.Count == 0)
                    break;

                foreach (var task in valueTasks)
                {
                    if (!task.IsCompleted)
                        await task;
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status ContextRead<Input, Output, Context, FasterSession>(ref Key key, ref Input input, ref Output output, Context context, FasterSession fasterSession, long serialNo,
            FasterExecutionContext<Input, Output, Context> sessionCtx)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            var pcontext = default(PendingContext<Input, Output, Context>);
            var internalStatus = InternalRead(ref key, ref input, ref output, ref context, ref pcontext, fasterSession, sessionCtx, serialNo);
            Debug.Assert(internalStatus != OperationStatus.RETRY_NOW);

            Status status;
            if (internalStatus == OperationStatus.SUCCESS || internalStatus == OperationStatus.NOTFOUND)
            {
                status = (Status)internalStatus;
            }
            else
            {
                status = HandleOperationStatus(sessionCtx, sessionCtx, pcontext, fasterSession, internalStatus);
            }

            sessionCtx.serialNum = serialNo;
            return status;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status ContextUpsert<Input, Output, Context, FasterSession>(ref Key key, ref Value value, Context context, FasterSession fasterSession, long serialNo,
            FasterExecutionContext<Input, Output, Context> sessionCtx)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            var pcontext = default(PendingContext<Input, Output, Context>);
            OperationStatus internalStatus;

            do
                internalStatus = InternalUpsert(ref key, ref value, ref context, ref pcontext, fasterSession, sessionCtx, serialNo);
            while (internalStatus == OperationStatus.RETRY_NOW);

            Status status;

            if (internalStatus == OperationStatus.SUCCESS || internalStatus == OperationStatus.NOTFOUND)
            {
                status = (Status)internalStatus;
            }
            else
            {
                status = HandleOperationStatus(sessionCtx, sessionCtx, pcontext, fasterSession, internalStatus);
            }

            sessionCtx.serialNum = serialNo;
            return status;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status ContextRMW<Input, Output, Context, FasterSession>(ref Key key, ref Input input, Context context, FasterSession fasterSession, long serialNo,
            FasterExecutionContext<Input, Output, Context> sessionCtx)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            var pcontext = default(PendingContext<Input, Output, Context>);
            OperationStatus internalStatus;

            do
                internalStatus = InternalRMW(ref key, ref input, ref context, ref pcontext, fasterSession, sessionCtx, serialNo);
            while (internalStatus == OperationStatus.RETRY_NOW);

            Status status;
            if (internalStatus == OperationStatus.SUCCESS || internalStatus == OperationStatus.NOTFOUND)
            {
                status = (Status)internalStatus;
            }
            else
            {
                status = HandleOperationStatus(sessionCtx, sessionCtx, pcontext, fasterSession, internalStatus);
            }

            sessionCtx.serialNum = serialNo;
            return status;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status ContextDelete<Input, Output, Context, FasterSession>(
            ref Key key, 
            Context context, 
            FasterSession fasterSession, 
            long serialNo, 
            FasterExecutionContext<Input, Output, Context> sessionCtx)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            var pcontext = default(PendingContext<Input, Output, Context>);
            OperationStatus internalStatus;

            do
                internalStatus = InternalDelete(ref key, ref context, ref pcontext, fasterSession, sessionCtx, serialNo);
            while (internalStatus == OperationStatus.RETRY_NOW);

            Status status;
            if (internalStatus == OperationStatus.SUCCESS || internalStatus == OperationStatus.NOTFOUND)
            {
                status = (Status)internalStatus;
            }
            else
            {
                status = HandleOperationStatus(sessionCtx, sessionCtx, pcontext, fasterSession, internalStatus);
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
            return StartStateMachine(new IndexResizeStateMachine());
        }

        /// <summary>
        /// Dispose FASTER instance
        /// </summary>
        public void Dispose()
        {
            Free();
            hlog.Dispose();
            readcache?.Dispose();
            if (disposeCheckpointManager)
                checkpointManager?.Dispose();
        }
    }
}