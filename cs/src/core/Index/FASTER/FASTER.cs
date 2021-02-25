// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace FASTER.core
{
    /// <summary>
    /// Flags for the Read-by-address methods
    /// </summary>
    [Flags]
    public enum ReadFlags
    {
        /// <summary>Default read operation</summary>
        None = 0,

        /// <summary>Skip the ReadCache when reading, including not inserting to ReadCache when pending reads are complete</summary>
        SkipReadCache = 0x00000001,
    }

    public partial class FasterKV<Key, Value> : FasterBase,
        IFasterKV<Key, Value>
    {
        internal readonly AllocatorBase<Key, Value> hlog;
        private readonly AllocatorBase<Key, Value> readcache;
        private readonly IFasterEqualityComparer<Key> comparer;

        internal readonly bool UseReadCache;
        private readonly CopyReadsToTail CopyReadsToTail;
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
        /// Number of active entries in hash index (does not correspond to total records, due to hash collisions)
        /// </summary>
        public long EntryCount => GetEntryCount();

        /// <summary>
        /// Size of index in #cache lines (64 bytes each)
        /// </summary>
        public long IndexSize => state[resizeInfo.version].size;

        /// <summary>
        /// Number of overflow buckets in use (64 bytes each)
        /// </summary>
        public long OverflowBucketCount => overflowBucketsAllocator.GetMaxValidAddress();

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

        /// <summary>
        /// An accessor to the record at a given logical address, for use in IFunctions callbacks.
        /// </summary>
        public RecordAccessor<Key, Value> RecordAccessor { get; }

        internal ConcurrentDictionary<string, CommitPoint> _recoveredSessions;

        /// <summary>
        /// Create FASTER instance
        /// </summary>
        /// <param name="size">Size of core index (#cache lines)</param>
        /// <param name="logSettings">Log settings</param>
        /// <param name="checkpointSettings">Checkpoint settings</param>
        /// <param name="serializerSettings">Serializer settings</param>
        /// <param name="comparer">FASTER equality comparer for key</param>
        /// <param name="variableLengthStructSettings"></param>
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

            this.RecordAccessor = new RecordAccessor<Key, Value>(this);

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
                CopyReadsToTail = CopyReadsToTail.None;
                UseReadCache = true;
            }

            UpdateVarLen(ref variableLengthStructSettings);

            if ((!Utility.IsBlittable<Key>() && variableLengthStructSettings?.keyLength == null) ||
                (!Utility.IsBlittable<Value>() && variableLengthStructSettings?.valueLength == null))
            {
                WriteDefaultOnDelete = true;

                hlog = new GenericAllocator<Key, Value>(logSettings, serializerSettings, this.comparer, null, epoch);
                Log = new LogAccessor<Key, Value>(this, hlog);
                if (UseReadCache)
                {
                    readcache = new GenericAllocator<Key, Value>(
                        new LogSettings
                        {
                            LogDevice = new NullDevice(),
                            ObjectLogDevice = new NullDevice(),
                            PageSizeBits = logSettings.ReadCacheSettings.PageSizeBits,
                            MemorySizeBits = logSettings.ReadCacheSettings.MemorySizeBits,
                            SegmentSizeBits = logSettings.ReadCacheSettings.MemorySizeBits,
                            MutableFraction = 1 - logSettings.ReadCacheSettings.SecondChanceFraction
                        }, serializerSettings, this.comparer, ReadCacheEvict, epoch);
                    readcache.Initialize();
                    ReadCache = new LogAccessor<Key, Value>(this, readcache);
                }
            }
            else if (variableLengthStructSettings != null)
            {
                hlog = new VariableLengthBlittableAllocator<Key, Value>(logSettings, variableLengthStructSettings,
                    this.comparer, null, epoch);
                Log = new LogAccessor<Key, Value>(this, hlog);
                if (UseReadCache)
                {
                    readcache = new VariableLengthBlittableAllocator<Key, Value>(
                        new LogSettings
                        {
                            LogDevice = new NullDevice(),
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
                            LogDevice = new NullDevice(),
                            PageSizeBits = logSettings.ReadCacheSettings.PageSizeBits,
                            MemorySizeBits = logSettings.ReadCacheSettings.MemorySizeBits,
                            SegmentSizeBits = logSettings.ReadCacheSettings.MemorySizeBits,
                            MutableFraction = 1 - logSettings.ReadCacheSettings.SecondChanceFraction
                        }, this.comparer, ReadCacheEvict, epoch);
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
        /// Initiate log-only checkpoint
        /// </summary>
        /// <param name="token">Checkpoint token</param>
        /// <param name="checkpointType">Checkpoint type</param>
        /// <param name="tryIncremental">Try to store as delta over last snapshot</param>
        /// <returns>Whether we could initiate the checkpoint. Use CompleteCheckpointAsync to wait completion.</returns>
        public bool TakeHybridLogCheckpoint(out Guid token, CheckpointType checkpointType, bool tryIncremental)
        {
            ISynchronizationTask backend;
            if (checkpointType == CheckpointType.FoldOver)
                backend = new FoldOverCheckpointTask();
            else if (checkpointType == CheckpointType.Snapshot)
            {
                if (tryIncremental && _lastSnapshotCheckpoint.info.guid != default && _lastSnapshotCheckpoint.info.finalLogicalAddress > hlog.FlushedUntilAddress)
                    backend = new IncrementalSnapshotCheckpointTask();
                else
                    backend = new SnapshotCheckpointTask();
            }
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
        /// <param name="numPagesToPreload">Number of pages to preload into memory (beyond what needs to be read for recovery)</param>
        /// <param name="undoFutureVersions">Whether records with versions beyond checkpoint version need to be undone (and invalidated on log)</param>
        public void Recover(int numPagesToPreload = -1, bool undoFutureVersions = true)
        {
            InternalRecoverFromLatestCheckpoints(numPagesToPreload, undoFutureVersions);
        }

        /// <summary>
        /// Asynchronously recover from the latest checkpoint (blocking operation)
        /// </summary>
        /// <param name="numPagesToPreload">Number of pages to preload into memory (beyond what needs to be read for recovery)</param>
        /// <param name="undoFutureVersions">Whether records with versions beyond checkpoint version need to be undone (and invalidated on log)</param>
        /// <param name="cancellationToken">Cancellation token</param>
        public ValueTask RecoverAsync(int numPagesToPreload = -1, bool undoFutureVersions = true, CancellationToken cancellationToken = default)
            => InternalRecoverFromLatestCheckpointsAsync(numPagesToPreload, undoFutureVersions, cancellationToken);

        /// <summary>
        /// Recover from specific token (blocking operation)
        /// </summary>
        /// <param name="fullCheckpointToken">Token</param>
        /// <param name="numPagesToPreload">Number of pages to preload into memory after recovery</param>
        /// <param name="undoFutureVersions">Whether records with versions beyond checkpoint version need to be undone (and invalidated on log)</param>
        public void Recover(Guid fullCheckpointToken, int numPagesToPreload = -1, bool undoFutureVersions = true)
        {
            InternalRecover(fullCheckpointToken, fullCheckpointToken, numPagesToPreload, undoFutureVersions);
        }

        /// <summary>
        /// Asynchronously recover from specific token (blocking operation)
        /// </summary>
        /// <param name="fullCheckpointToken">Token</param>
        /// <param name="numPagesToPreload">Number of pages to preload into memory after recovery</param>
        /// <param name="undoFutureVersions">Whether records with versions beyond checkpoint version need to be undone (and invalidated on log)</param>
        /// <param name="cancellationToken">Cancellation token</param>
        public ValueTask RecoverAsync(Guid fullCheckpointToken, int numPagesToPreload = -1, bool undoFutureVersions = true, CancellationToken cancellationToken = default) 
            => InternalRecoverAsync(fullCheckpointToken, fullCheckpointToken, numPagesToPreload, undoFutureVersions, cancellationToken);

        /// <summary>
        /// Recover from specific index and log token (blocking operation)
        /// </summary>
        /// <param name="indexCheckpointToken"></param>
        /// <param name="hybridLogCheckpointToken"></param>
        /// <param name="numPagesToPreload">Number of pages to preload into memory after recovery</param>
        /// <param name="undoFutureVersions">Whether records with versions beyond checkpoint version need to be undone (and invalidated on log)</param>
        public void Recover(Guid indexCheckpointToken, Guid hybridLogCheckpointToken, int numPagesToPreload = -1, bool undoFutureVersions = true)
        {
            InternalRecover(indexCheckpointToken, hybridLogCheckpointToken, numPagesToPreload, undoFutureVersions);
        }

        /// <summary>
        /// Asynchronously recover from specific index and log token (blocking operation)
        /// </summary>
        /// <param name="indexCheckpointToken"></param>
        /// <param name="hybridLogCheckpointToken"></param>
        /// <param name="numPagesToPreload">Number of pages to preload into memory after recovery</param>
        /// <param name="undoFutureVersions">Whether records with versions beyond checkpoint version need to be undone (and invalidated on log)</param>
        /// <param name="cancellationToken">Cancellation token</param>
        public ValueTask RecoverAsync(Guid indexCheckpointToken, Guid hybridLogCheckpointToken, int numPagesToPreload = -1, bool undoFutureVersions = true, CancellationToken cancellationToken = default) 
            => InternalRecoverAsync(indexCheckpointToken, hybridLogCheckpointToken, numPagesToPreload, undoFutureVersions, cancellationToken);

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
                    continue; // we need to re-check loop, so we return only when we are at REST

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
            var internalStatus = InternalRead(ref key, ref input, ref output, Constants.kInvalidAddress, ref context, ref pcontext, fasterSession, sessionCtx, serialNo);
            Debug.Assert(internalStatus != OperationStatus.RETRY_NOW);

            Status status;
            if (internalStatus == OperationStatus.SUCCESS || internalStatus == OperationStatus.NOTFOUND)
            {
                status = (Status)internalStatus;
            }
            else
            {
                status = HandleOperationStatus(sessionCtx, sessionCtx, ref pcontext, fasterSession, internalStatus, false, out _);
            }

            Debug.Assert(serialNo >= sessionCtx.serialNum, "Operation serial numbers must be non-decreasing");
            sessionCtx.serialNum = serialNo;
            return status;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status ContextRead<Input, Output, Context, FasterSession>(ref Key key, ref Input input, ref Output output, ref RecordInfo recordInfo, ReadFlags readFlags, Context context, FasterSession fasterSession, long serialNo,
            FasterExecutionContext<Input, Output, Context> sessionCtx)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            var pcontext = default(PendingContext<Input, Output, Context>);
            pcontext.operationFlags = PendingContext<Input, Output, Context>.GetOperationFlags(readFlags);
            var internalStatus = InternalRead(ref key, ref input, ref output, recordInfo.PreviousAddress, ref context, ref pcontext, fasterSession, sessionCtx, serialNo);
            Debug.Assert(internalStatus != OperationStatus.RETRY_NOW);

            Status status;
            if (internalStatus == OperationStatus.SUCCESS || internalStatus == OperationStatus.NOTFOUND)
            {
                recordInfo = pcontext.recordInfo;
                status = (Status)internalStatus;
            }
            else
            {
                recordInfo = default;
                status = HandleOperationStatus(sessionCtx, sessionCtx, ref pcontext, fasterSession, internalStatus, false, out _);
            }

            Debug.Assert(serialNo >= sessionCtx.serialNum, "Operation serial numbers must be non-decreasing");
            sessionCtx.serialNum = serialNo;
            return status;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status ContextReadAtAddress<Input, Output, Context, FasterSession>(long address, ref Input input, ref Output output, ReadFlags readFlags, Context context, FasterSession fasterSession, long serialNo,
            FasterExecutionContext<Input, Output, Context> sessionCtx)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            var pcontext = default(PendingContext<Input, Output, Context>);
            pcontext.operationFlags = PendingContext<Input, Output, Context>.GetOperationFlags(readFlags, noKey: true);
            Key key = default;
            var internalStatus = InternalRead(ref key, ref input, ref output, address, ref context, ref pcontext, fasterSession, sessionCtx, serialNo);
            Debug.Assert(internalStatus != OperationStatus.RETRY_NOW);

            Status status;
            if (internalStatus == OperationStatus.SUCCESS || internalStatus == OperationStatus.NOTFOUND)
            {
                status = (Status)internalStatus;
            }
            else
            {
                status = HandleOperationStatus(sessionCtx, sessionCtx, ref pcontext, fasterSession, internalStatus, false, out _);
            }

            Debug.Assert(serialNo >= sessionCtx.serialNum, "Operation serial numbers must be non-decreasing");
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
                status = HandleOperationStatus(sessionCtx, sessionCtx, ref pcontext, fasterSession, internalStatus, false, out _);
            }

            Debug.Assert(serialNo >= sessionCtx.serialNum, "Operation serial numbers must be non-decreasing");
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
                status = HandleOperationStatus(sessionCtx, sessionCtx, ref pcontext, fasterSession, internalStatus, false, out _);
            }

            Debug.Assert(serialNo >= sessionCtx.serialNum, "Operation serial numbers must be non-decreasing");
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
                status = HandleOperationStatus(sessionCtx, sessionCtx, ref pcontext, fasterSession, internalStatus, false, out _);
            }

            Debug.Assert(serialNo >= sessionCtx.serialNum, "Operation serial numbers must be non-decreasing");
            sessionCtx.serialNum = serialNo;
            return status;
        }

        /// <summary>
        /// Grow the hash index by a factor of two. Make sure to take a full checkpoint
        /// after growth, for persistence.
        /// </summary>
        /// <returns>Whether the grow completed</returns>
        public bool GrowIndex()
        {
            if (LightEpoch.AnyInstanceProtected())
                throw new FasterException("Cannot use GrowIndex when using legacy or non-async sessions");

            if (!StartStateMachine(new IndexResizeStateMachine())) return false;

            epoch.Resume();

            try
            {
                while (true)
                {
                    SystemState _systemState = SystemState.Copy(ref systemState);
                    if (_systemState.phase == Phase.IN_PROGRESS_GROW)
                    {
                        SplitBuckets(0);
                        epoch.ProtectAndDrain();
                    }
                    else
                    {
                        SystemState.RemoveIntermediate(ref _systemState);
                        if (_systemState.phase != Phase.PREPARE_GROW && _systemState.phase != Phase.IN_PROGRESS_GROW)
                        {
                            return true;
                        }
                    }
                }
            }
            finally
            {
                epoch.Suspend();
            }
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

        private void UpdateVarLen(ref VariableLengthStructSettings<Key, Value> variableLengthStructSettings)
        {
            if (typeof(Key) == typeof(SpanByte))
            {
                if (variableLengthStructSettings == null)
                    variableLengthStructSettings = new VariableLengthStructSettings<SpanByte, Value>() as VariableLengthStructSettings<Key, Value>;

                if (variableLengthStructSettings.keyLength == null)
                    (variableLengthStructSettings as VariableLengthStructSettings<SpanByte, Value>).keyLength = new SpanByteVarLenStruct();
            }
            else if (typeof(Key).IsGenericType && (typeof(Key).GetGenericTypeDefinition() == typeof(Memory<>)) && Utility.IsBlittableType(typeof(Key).GetGenericArguments()[0]))
            {
                if (variableLengthStructSettings == null)
                    variableLengthStructSettings = new VariableLengthStructSettings<Key, Value>();

                if (variableLengthStructSettings.keyLength == null)
                {
                    var m = typeof(MemoryVarLenStruct<>).MakeGenericType(typeof(Key).GetGenericArguments());
                    object o = Activator.CreateInstance(m);
                    variableLengthStructSettings.keyLength = o as IVariableLengthStruct<Key>;
                }
            }
            else if (typeof(Key).IsGenericType && (typeof(Key).GetGenericTypeDefinition() == typeof(ReadOnlyMemory<>)) && Utility.IsBlittableType(typeof(Key).GetGenericArguments()[0]))
            {
                if (variableLengthStructSettings == null)
                    variableLengthStructSettings = new VariableLengthStructSettings<Key, Value>();

                if (variableLengthStructSettings.keyLength == null)
                {
                    var m = typeof(ReadOnlyMemoryVarLenStruct<>).MakeGenericType(typeof(Key).GetGenericArguments());
                    object o = Activator.CreateInstance(m);
                    variableLengthStructSettings.keyLength = o as IVariableLengthStruct<Key>;
                }
            }

            if (typeof(Value) == typeof(SpanByte))
            {
                if (variableLengthStructSettings == null)
                    variableLengthStructSettings = new VariableLengthStructSettings<Key, SpanByte>() as VariableLengthStructSettings<Key, Value>;

                if (variableLengthStructSettings.valueLength == null)
                    (variableLengthStructSettings as VariableLengthStructSettings<Key, SpanByte>).valueLength = new SpanByteVarLenStruct();
            }
            else if (typeof(Value).IsGenericType && (typeof(Value).GetGenericTypeDefinition() == typeof(Memory<>)) && Utility.IsBlittableType(typeof(Value).GetGenericArguments()[0]))
            {
                if (variableLengthStructSettings == null)
                    variableLengthStructSettings = new VariableLengthStructSettings<Key, Value>();

                if (variableLengthStructSettings.valueLength == null)
                {
                    var m = typeof(MemoryVarLenStruct<>).MakeGenericType(typeof(Value).GetGenericArguments());
                    object o = Activator.CreateInstance(m);
                    variableLengthStructSettings.valueLength = o as IVariableLengthStruct<Value>;
                }
            }
            else if (typeof(Value).IsGenericType && (typeof(Value).GetGenericTypeDefinition() == typeof(ReadOnlyMemory<>)) && Utility.IsBlittableType(typeof(Value).GetGenericArguments()[0]))
            {
                if (variableLengthStructSettings == null)
                    variableLengthStructSettings = new VariableLengthStructSettings<Key, Value>();

                if (variableLengthStructSettings.valueLength == null)
                {
                    var m = typeof(ReadOnlyMemoryVarLenStruct<>).MakeGenericType(typeof(Value).GetGenericArguments());
                    object o = Activator.CreateInstance(m);
                    variableLengthStructSettings.valueLength = o as IVariableLengthStruct<Value>;
                }
            }
        }

        /// <summary>
        /// Total number of valid entries in hash table
        /// </summary>
        /// <returns></returns>
        private unsafe long GetEntryCount()
        {
            var version = resizeInfo.version;
            var table_size_ = state[version].size;
            var ptable_ = state[version].tableAligned;
            long total_entry_count = 0;
            long beginAddress = hlog.BeginAddress;

            for (long bucket = 0; bucket < table_size_; ++bucket)
            {
                HashBucket b = *(ptable_ + bucket);
                while (true)
                {
                    for (int bucket_entry = 0; bucket_entry < Constants.kOverflowBucketIndex; ++bucket_entry)
                        if (b.bucket_entries[bucket_entry] >= beginAddress)
                            ++total_entry_count;
                    if (b.bucket_entries[Constants.kOverflowBucketIndex] == 0) break;
                    b = *((HashBucket*)overflowBucketsAllocator.GetPhysicalAddress((b.bucket_entries[Constants.kOverflowBucketIndex])));
                }
            }
            return total_entry_count;
        }

        private unsafe string DumpDistributionInternal(int version)
        {
            var table_size_ = state[version].size;
            var ptable_ = state[version].tableAligned;
            long total_record_count = 0;
            long beginAddress = hlog.BeginAddress;
            Dictionary<int, long> histogram = new Dictionary<int, long>();

            for (long bucket = 0; bucket < table_size_; ++bucket)
            {
                List<int> tags = new List<int>();
                int cnt = 0;
                HashBucket b = *(ptable_ + bucket);
                while (true)
                {
                    for (int bucket_entry = 0; bucket_entry < Constants.kOverflowBucketIndex; ++bucket_entry)
                    {
                        var x = default(HashBucketEntry);
                        x.word = b.bucket_entries[bucket_entry];
                        if (((!x.ReadCache) && (x.Address >= beginAddress)) || (x.ReadCache && ((x.Address & ~Constants.kReadCacheBitMask) >= readcache.HeadAddress)))
                        {
                            if (tags.Contains(x.Tag) && !x.Tentative)
                                throw new FasterException("Duplicate tag found in index");
                            tags.Add(x.Tag);
                            ++cnt;
                            ++total_record_count;
                        }
                    }
                    if (b.bucket_entries[Constants.kOverflowBucketIndex] == 0) break;
                    b = *((HashBucket*)overflowBucketsAllocator.GetPhysicalAddress((b.bucket_entries[Constants.kOverflowBucketIndex])));
                }

                if (!histogram.ContainsKey(cnt)) histogram[cnt] = 0;
                histogram[cnt]++;
            }

            var distribution =
                $"Number of hash buckets: {table_size_}\n" +
                $"Number of overflow buckets: {OverflowBucketCount}\n" +
                $"Size of each bucket: {Constants.kEntriesPerBucket * sizeof(HashBucketEntry)} bytes\n" +
                $"Total distinct hash-table entry count: {{{total_record_count}}}\n" +
                $"Average #entries per hash bucket: {{{total_record_count / (double)table_size_:0.00}}}\n" +
                $"Histogram of #entries per bucket:\n";

            foreach (var kvp in histogram.OrderBy(e => e.Key))
            {
                distribution += $"  {kvp.Key} : {kvp.Value}\n";
            }

            return distribution;
        }

        /// <summary>
        /// Dumps the distribution of each non-empty bucket in the hash table.
        /// </summary>
        public string DumpDistribution()
        {
            return DumpDistributionInternal(resizeInfo.version);
        }
    }
}