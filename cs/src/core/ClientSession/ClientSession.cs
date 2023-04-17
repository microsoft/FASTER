// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace FASTER.core
{
    /// <summary>
    /// Thread-independent session interface to FASTER
    /// </summary>
    /// <typeparam name="Key"></typeparam>
    /// <typeparam name="Value"></typeparam>
    /// <typeparam name="Input"></typeparam>
    /// <typeparam name="Output"></typeparam>
    /// <typeparam name="Context"></typeparam>
    /// <typeparam name="Functions"></typeparam>
    public sealed class ClientSession<Key, Value, Input, Output, Context, Functions> : IClientSession, IFasterContext<Key, Value, Input, Output, Context>, IDisposable
        where Functions : IFunctions<Key, Value, Input, Output, Context>
    {
        internal readonly FasterKV<Key, Value> fht;

        internal readonly FasterKV<Key, Value>.FasterExecutionContext<Input, Output, Context> ctx;
        internal CommitPoint LatestCommitPoint;

        internal readonly Functions functions;
        internal readonly IVariableLengthStruct<Value, Input> variableLengthStruct;
        internal readonly IVariableLengthStruct<Input> inputVariableLengthStruct;

        internal CompletedOutputIterator<Key, Value, Input, Output, Context> completedOutputs;

        internal readonly InternalFasterSession FasterSession;

        readonly UnsafeContext<Key, Value, Input, Output, Context, Functions> uContext;
        readonly LockableUnsafeContext<Key, Value, Input, Output, Context, Functions> luContext;
        readonly LockableContext<Key, Value, Input, Output, Context, Functions> lContext;
        readonly BasicContext<Key, Value, Input, Output, Context, Functions> bContext;

        internal const string NotAsyncSessionErr = "Session does not support async operations";

        readonly ILoggerFactory loggerFactory;
        readonly ILogger logger;

        internal ulong TotalLockCount => sharedLockCount + exclusiveLockCount;
        internal ulong sharedLockCount;
        internal ulong exclusiveLockCount;

        bool isAcquiredLockable;

        internal void AcquireLockable()
        {
            CheckIsNotAcquiredLockable();

            while (true)
            {
                // Checkpoints cannot complete while we have active locking sessions.
                while (IsInPreparePhase())
                    Thread.Yield();

                fht.IncrementNumLockingSessions();
                isAcquiredLockable = true;

                if (!IsInPreparePhase())
                    break;
                InternalReleaseLockable();
                Thread.Yield();
            }
        }

        internal void ReleaseLockable()
        {
            CheckIsAcquiredLockable();
            if (TotalLockCount > 0)
                throw new FasterException($"EndLockable called with locks held: {sharedLockCount} shared locks, {exclusiveLockCount} exclusive locks");
            InternalReleaseLockable();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void InternalReleaseLockable()
        {
            isAcquiredLockable = false;
            fht.DecrementNumLockingSessions();
        }

        internal void CheckIsAcquiredLockable()
        {
            if (!isAcquiredLockable)
                throw new FasterException("Lockable method call when BeginLockable has not been called");
        }

        void CheckIsNotAcquiredLockable()
        {
            if (isAcquiredLockable)
                throw new FasterException("BeginLockable cannot be called twice (call EndLockable first)");
        }

        internal ClientSession(
            FasterKV<Key, Value> fht,
            FasterKV<Key, Value>.FasterExecutionContext<Input, Output, Context> ctx,
            Functions functions,
            SessionVariableLengthStructSettings<Value, Input> sessionVariableLengthStructSettings,
            ILoggerFactory loggerFactory = null)
        {
            this.bContext = new(this);
            this.uContext = new(this);

            if (fht.LockTable.IsEnabled)
            {
                this.lContext = new(this);
                this.luContext = new(this);
            }

            this.loggerFactory = loggerFactory;
            this.logger = loggerFactory?.CreateLogger($"ClientSession-{GetHashCode():X8}");
            this.fht = fht;
            this.ctx = ctx;
            this.functions = functions;
            LatestCommitPoint = new CommitPoint { UntilSerialNo = -1, ExcludedSerialNos = null };
            FasterSession = new InternalFasterSession(this);

            this.variableLengthStruct = sessionVariableLengthStructSettings?.valueLength;
            if (this.variableLengthStruct == default)
            {
                UpdateVarlen(ref this.variableLengthStruct);

                if ((this.variableLengthStruct == default) && (fht.hlog is VariableLengthBlittableAllocator<Key, Value> allocator))
                {
                    logger?.LogWarning("Warning: Session did not specify Input-specific functions for variable-length values via IVariableLengthStruct<Value, Input>");
                    this.variableLengthStruct = new DefaultVariableLengthStruct<Value, Input>(allocator.ValueLength);
                }
            }
            else
            {
                if (fht.hlog is not VariableLengthBlittableAllocator<Key, Value>)
                    logger?.LogWarning("Warning: Session param of variableLengthStruct provided for non-varlen allocator");
            }

            this.inputVariableLengthStruct = sessionVariableLengthStructSettings?.inputLength;

            if (inputVariableLengthStruct == default)
            {
                if (typeof(Input) == typeof(SpanByte))
                {
                    inputVariableLengthStruct = new SpanByteVarLenStruct() as IVariableLengthStruct<Input>;
                }
                else if (typeof(Input).IsGenericType && (typeof(Input).GetGenericTypeDefinition() == typeof(Memory<>)) && Utility.IsBlittableType(typeof(Input).GetGenericArguments()[0]))
                {
                    var m = typeof(MemoryVarLenStruct<>).MakeGenericType(typeof(Input).GetGenericArguments());
                    object o = Activator.CreateInstance(m);
                    inputVariableLengthStruct = o as IVariableLengthStruct<Input>;
                }
                else if (typeof(Input).IsGenericType && (typeof(Input).GetGenericTypeDefinition() == typeof(ReadOnlyMemory<>)) && Utility.IsBlittableType(typeof(Input).GetGenericArguments()[0]))
                {
                    var m = typeof(ReadOnlyMemoryVarLenStruct<>).MakeGenericType(typeof(Input).GetGenericArguments());
                    object o = Activator.CreateInstance(m);
                    inputVariableLengthStruct = o as IVariableLengthStruct<Input>;
                }
            }
        }

        private void UpdateVarlen(ref IVariableLengthStruct<Value, Input> variableLengthStruct)
        {
            if (fht.hlog is not VariableLengthBlittableAllocator<Key, Value>)
                return;

            if (typeof(Value) == typeof(SpanByte) && typeof(Input) == typeof(SpanByte))
            {
                variableLengthStruct = new SpanByteVarLenStructForSpanByteInput() as IVariableLengthStruct<Value, Input>;
            }
            else if (typeof(Value).IsGenericType && (typeof(Value).GetGenericTypeDefinition() == typeof(Memory<>)) && Utility.IsBlittableType(typeof(Value).GetGenericArguments()[0]))
            {
                if (typeof(Input).IsGenericType && (typeof(Input).GetGenericTypeDefinition() == typeof(Memory<>)) && typeof(Input).GetGenericArguments()[0] == typeof(Value).GetGenericArguments()[0])
                {
                    var m = typeof(MemoryVarLenStructForMemoryInput<>).MakeGenericType(typeof(Value).GetGenericArguments());
                    object o = Activator.CreateInstance(m);
                    variableLengthStruct = o as IVariableLengthStruct<Value, Input>;
                }
                else if (typeof(Input).IsGenericType && (typeof(Input).GetGenericTypeDefinition() == typeof(ReadOnlyMemory<>)) && typeof(Input).GetGenericArguments()[0] == typeof(Value).GetGenericArguments()[0])
                {
                    var m = typeof(MemoryVarLenStructForReadOnlyMemoryInput<>).MakeGenericType(typeof(Value).GetGenericArguments());
                    object o = Activator.CreateInstance(m);
                    variableLengthStruct = o as IVariableLengthStruct<Value, Input>;
                }
            }
        }

        /// <summary>
        /// Get session ID
        /// </summary>
        public int ID { get { return ctx.sessionID; } }

        /// <summary>
        /// Get session name
        /// </summary>
        public string Name { get { return ctx.sessionName; } }

        /// <summary>
        /// Next sequential serial no for session (current serial no + 1)
        /// </summary>
        public long NextSerialNo => ctx.serialNum + 1;

        /// <summary>
        /// Current serial no for session
        /// </summary>
        public long SerialNo => ctx.serialNum;

        /// <summary>
        /// Current version number of the session
        /// </summary>
        public long Version => ctx.version;

        /// <summary>
        /// Dispose session
        /// </summary>
        public void Dispose()
        {
            this.completedOutputs?.Dispose();
            CompletePending(true);
            fht.DisposeClientSession(ID, ctx.phase);
        }

        /// <summary>
        /// Return a new interface to Faster operations that supports manual epoch control.
        /// </summary>
        public UnsafeContext<Key, Value, Input, Output, Context, Functions> UnsafeContext => uContext;

        /// <summary>
        /// Return a new interface to Faster operations that supports manual locking and epoch control.
        /// </summary>
        public LockableUnsafeContext<Key, Value, Input, Output, Context, Functions> LockableUnsafeContext
        {
            get
            {
                if (!this.fht.LockTable.IsEnabled)
                    throw new FasterException($"LockableUnsafeContext requires {nameof(LockingMode.Standard)}");
                return luContext;
            }
        }

        /// <summary>
        /// Return a session wrapper that supports manual locking.
        /// </summary>
        public LockableContext<Key, Value, Input, Output, Context, Functions> LockableContext
        {
            get
            {
                if (!this.fht.LockTable.IsEnabled)
                    throw new FasterException($"LockableContext requires {nameof(LockingMode.Standard)}");
                return lContext;
            }
        }

        /// <summary>
        /// Return a session wrapper struct that passes through to client session
        /// </summary>
        public BasicContext<Key, Value, Input, Output, Context, Functions> BasicContext => bContext;

        #region IFasterContext
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read(ref Key key, ref Input input, ref Output output, Context userContext = default, long serialNo = 0)
        {
            UnsafeResumeThread();
            try
            {
                return fht.ContextRead(ref key, ref input, ref output, userContext, FasterSession, serialNo);
            }
            finally
            {
                UnsafeSuspendThread();
            }
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read(Key key, Input input, out Output output, Context userContext = default, long serialNo = 0)
        {
            output = default;
            return Read(ref key, ref input, ref output, userContext, serialNo);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read(ref Key key, ref Output output, Context userContext = default, long serialNo = 0)
        {
            Input input = default;
            return Read(ref key, ref input, ref output, userContext, serialNo);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read(Key key, out Output output, Context userContext = default, long serialNo = 0)
        {
            Input input = default;
            output = default;
            return Read(ref key, ref input, ref output, userContext, serialNo);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public (Status status, Output output) Read(Key key, Context userContext = default, long serialNo = 0)
        {
            Input input = default;
            Output output = default;
            return (Read(ref key, ref input, ref output, userContext, serialNo), output);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read(ref Key key, ref Input input, ref Output output, ref ReadOptions readOptions, out RecordMetadata recordMetadata, Context userContext = default, long serialNo = 0)
        {
            UnsafeResumeThread();
            try
            {
                return fht.ContextRead(ref key, ref input, ref output, ref readOptions, out recordMetadata, userContext, FasterSession, serialNo);
            }
            finally
            {
                UnsafeSuspendThread();
            }
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status ReadAtAddress(ref Input input, ref Output output, ref ReadOptions readOptions, Context userContext = default, long serialNo = 0)
        {
            UnsafeResumeThread();
            try
            {
                return fht.ContextReadAtAddress(ref input, ref output, ref readOptions, userContext, FasterSession, serialNo);
            }
            finally
            {
                UnsafeSuspendThread();
            }
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<FasterKV<Key, Value>.ReadAsyncResult<Input, Output, Context>> ReadAsync(ref Key key, ref Input input, Context userContext = default, long serialNo = 0, CancellationToken cancellationToken = default)
        {
            ReadOptions readOptions = default;
            return fht.ReadAsync(this.FasterSession, ref key, ref input, ref readOptions, userContext, serialNo, cancellationToken);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<FasterKV<Key, Value>.ReadAsyncResult<Input, Output, Context>> ReadAsync(Key key, Input input, Context context = default, long serialNo = 0, CancellationToken token = default)
        {
            ReadOptions readOptions = default;
            return fht.ReadAsync(this.FasterSession, ref key, ref input, ref readOptions, context, serialNo, token);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<FasterKV<Key, Value>.ReadAsyncResult<Input, Output, Context>> ReadAsync(ref Key key, Context userContext = default, long serialNo = 0, CancellationToken token = default)
        {
            Input input = default;
            ReadOptions readOptions = default;
            return fht.ReadAsync(this.FasterSession, ref key, ref input, ref readOptions, userContext, serialNo, token);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<FasterKV<Key, Value>.ReadAsyncResult<Input, Output, Context>> ReadAsync(Key key, Context context = default, long serialNo = 0, CancellationToken token = default)
        {
            Input input = default;
            ReadOptions readOptions = default;
            return fht.ReadAsync(this.FasterSession, ref key, ref input, ref readOptions, context, serialNo, token);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<FasterKV<Key, Value>.ReadAsyncResult<Input, Output, Context>> ReadAsync(ref Key key, ref Input input, ref ReadOptions readOptions,
                                                                                                 Context userContext = default, long serialNo = 0, CancellationToken cancellationToken = default) 
            => fht.ReadAsync(this.FasterSession, ref key, ref input, ref readOptions, userContext, serialNo, cancellationToken);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<FasterKV<Key, Value>.ReadAsyncResult<Input, Output, Context>> ReadAtAddressAsync(ref Input input, ref ReadOptions readOptions,
                                                                                                          Context userContext = default, long serialNo = 0, CancellationToken cancellationToken = default)
        {
            Key key = default;
            return fht.ReadAsync(this.FasterSession, ref key, ref input, ref readOptions, userContext, serialNo, cancellationToken, noKey: true);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(ref Key key, ref Value desiredValue, Context userContext = default, long serialNo = 0)
        {
            Input input = default;
            Output output = default;
            return Upsert(ref key, ref input, ref desiredValue, ref output, userContext, serialNo);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(ref Key key, ref Input input, ref Value desiredValue, ref Output output, Context userContext = default, long serialNo = 0)
        {
            UnsafeResumeThread();
            try
            {
                return fht.ContextUpsert(ref key, ref input, ref desiredValue, ref output, userContext, FasterSession, serialNo);
            }
            finally
            {
                UnsafeSuspendThread();
            }
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(ref Key key, ref Input input, ref Value desiredValue, ref Output output, out RecordMetadata recordMetadata, Context userContext = default, long serialNo = 0)
        {
            UnsafeResumeThread();
            try
            {
                return fht.ContextUpsert(ref key, ref input, ref desiredValue, ref output, out recordMetadata, userContext, FasterSession, serialNo);
            }
            finally
            {
                UnsafeSuspendThread();
            }
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(Key key, Value desiredValue, Context userContext = default, long serialNo = 0)
            => Upsert(ref key, ref desiredValue, userContext, serialNo);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(Key key, Input input, Value desiredValue, ref Output output, Context userContext = default, long serialNo = 0)
            => Upsert(ref key, ref input, ref desiredValue, ref output, userContext, serialNo);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<FasterKV<Key, Value>.UpsertAsyncResult<Input, Output, Context>> UpsertAsync(ref Key key, ref Value desiredValue, Context userContext = default, long serialNo = 0, CancellationToken token = default)
        {
            Input input = default;
            return UpsertAsync(ref key, ref input, ref desiredValue, userContext, serialNo, token);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<FasterKV<Key, Value>.UpsertAsyncResult<Input, Output, Context>> UpsertAsync(ref Key key, ref Input input, ref Value desiredValue, Context userContext = default, long serialNo = 0, CancellationToken token = default) 
            => fht.UpsertAsync<Input, Output, Context, InternalFasterSession>(this.FasterSession, ref key, ref input, ref desiredValue, userContext, serialNo, token);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<FasterKV<Key, Value>.UpsertAsyncResult<Input, Output, Context>> UpsertAsync(Key key, Value desiredValue, Context userContext = default, long serialNo = 0, CancellationToken token = default) 
            => UpsertAsync(ref key, ref desiredValue, userContext, serialNo, token);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<FasterKV<Key, Value>.UpsertAsyncResult<Input, Output, Context>> UpsertAsync(Key key, Input input, Value desiredValue, Context userContext = default, long serialNo = 0, CancellationToken token = default)
            => UpsertAsync(ref key, ref input, ref desiredValue, userContext, serialNo, token);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW(ref Key key, ref Input input, ref Output output, Context userContext = default, long serialNo = 0) 
            => RMW(ref key, ref input, ref output, out _, userContext, serialNo);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW(ref Key key, ref Input input, ref Output output, out RecordMetadata recordMetadata, Context userContext = default, long serialNo = 0)
        {
            UnsafeResumeThread();
            try
            {
                return fht.ContextRMW(ref key, ref input, ref output, out recordMetadata, userContext, FasterSession, serialNo);
            }
            finally
            {
                UnsafeSuspendThread();
            }
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW(Key key, Input input, out Output output, Context userContext = default, long serialNo = 0)
        {
            output = default;
            return RMW(ref key, ref input, ref output, userContext, serialNo);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW(ref Key key, ref Input input, Context userContext = default, long serialNo = 0)
        {
            Output output = default;
            return RMW(ref key, ref input, ref output, userContext, serialNo);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW(Key key, Input input, Context userContext = default, long serialNo = 0)
        {
            Output output = default;
            return RMW(ref key, ref input, ref output, userContext, serialNo);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<FasterKV<Key, Value>.RmwAsyncResult<Input, Output, Context>> RMWAsync(ref Key key, ref Input input, Context context = default, long serialNo = 0, CancellationToken token = default) 
            => fht.RmwAsync<Input, Output, Context, InternalFasterSession>(this.FasterSession, ref key, ref input, context, serialNo, token);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<FasterKV<Key, Value>.RmwAsyncResult<Input, Output, Context>> RMWAsync(Key key, Input input, Context context = default, long serialNo = 0, CancellationToken token = default)
            => RMWAsync(ref key, ref input, context, serialNo, token);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Delete(ref Key key, Context userContext = default, long serialNo = 0)
        {
            UnsafeResumeThread();
            try
            {
                return fht.ContextDelete<Input, Output, Context, InternalFasterSession>(ref key, userContext, FasterSession, serialNo);
            }
            finally
            {
                UnsafeSuspendThread();
            }
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Delete(Key key, Context userContext = default, long serialNo = 0)
            => Delete(ref key, userContext, serialNo);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<FasterKV<Key, Value>.DeleteAsyncResult<Input, Output, Context>> DeleteAsync(ref Key key, Context userContext = default, long serialNo = 0, CancellationToken token = default) 
            => fht.DeleteAsync<Input, Output, Context, InternalFasterSession>(this.FasterSession, ref key, userContext, serialNo, token);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<FasterKV<Key, Value>.DeleteAsyncResult<Input, Output, Context>> DeleteAsync(Key key, Context userContext = default, long serialNo = 0, CancellationToken token = default)
            => DeleteAsync(ref key, userContext, serialNo, token);

        /// <inheritdoc/>
        public void Refresh()
        {
            UnsafeResumeThread();
            fht.InternalRefresh<Input, Output, Context, InternalFasterSession>(FasterSession);
            UnsafeSuspendThread();
        }

        /// <inheritdoc/>
        public void ResetModified(ref Key key)
        {
            UnsafeResumeThread();
            try
            {
                UnsafeResetModified(ref key);
            }
            finally
            {
                UnsafeSuspendThread();
            }
        }

        /// <inheritdoc/>
        public bool NeedKeyLockCode => this.fht.LockTable.IsEnabled && this.fht.LockTable.NeedKeyLockCode;

        /// <inheritdoc/>
        public long GetLockCode(ref Key key, out long keyHash)
        {
            keyHash = this.fht.comparer.GetHashCode64(ref key);
            return this.fht.LockTable.IsEnabled ? this.fht.LockTable.GetLockCode(ref key, keyHash) : keyHash;
        }

        /// <inheritdoc/>
        public long GetLockCode(ref Key key, long keyHash) => this.fht.LockTable.IsEnabled ? this.fht.LockTable.GetLockCode(ref key, keyHash) : keyHash;

        /// <inheritdoc/>
        public int CompareLockCodes<TLockableKey>(TLockableKey key1, TLockableKey key2) where TLockableKey : ILockableKey => fht.LockTable.CompareLockCodes(key1, key2);

        /// <inheritdoc/>
        public int CompareLockCodes<TLockableKey>(ref TLockableKey key1, ref TLockableKey key2) where TLockableKey : ILockableKey => fht.LockTable.CompareLockCodes(ref key1, ref key2);

        /// <inheritdoc/>
        public void SortLockCodes<TLockableKey>(TLockableKey[] keys) where TLockableKey : ILockableKey => fht.LockTable.SortLockCodes(keys);

        /// <inheritdoc/>
        public void SortLockCodes<TLockableKey>(TLockableKey[] keys, int start, int count) where TLockableKey : ILockableKey => fht.LockTable.SortLockCodes(keys, start, count);

        #endregion IFasterContext

        #region Pending Operations

        /// <summary>
        /// Get list of pending requests (for current session)
        /// </summary>
        /// <returns></returns>
        public IEnumerable<long> GetPendingRequests()
        {
            foreach (var kvp in ctx.prevCtx?.ioPendingRequests)
                yield return kvp.Value.serialNum;

            foreach (var kvp in ctx.ioPendingRequests)
                yield return kvp.Value.serialNum;
        }

        /// <inheritdoc/>
        public bool CompletePending(bool wait = false, bool spinWaitForCommit = false)
            => CompletePending(false, wait, spinWaitForCommit);

        /// <inheritdoc/>
        public bool CompletePendingWithOutputs(out CompletedOutputIterator<Key, Value, Input, Output, Context> completedOutputs, bool wait = false, bool spinWaitForCommit = false)
        {
            InitializeCompletedOutputs();
            var result = CompletePending(true, wait, spinWaitForCommit);
            completedOutputs = this.completedOutputs;
            return result;
        }

        /// <summary>
        /// Synchronously complete outstanding pending synchronous operations, returning outputs for the completed operations.
        /// Assumes epoch protection is managed by user. Async operations must be completed individually.
        /// </summary>
        internal bool UnsafeCompletePendingWithOutputs<FasterSession>(FasterSession fasterSession, out CompletedOutputIterator<Key, Value, Input, Output, Context> completedOutputs, bool wait = false, bool spinWaitForCommit = false)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            InitializeCompletedOutputs();
            var result = UnsafeCompletePending(fasterSession, true, wait, spinWaitForCommit);
            completedOutputs = this.completedOutputs;
            return result;
        }

        private void InitializeCompletedOutputs()
        {
            if (this.completedOutputs is null)
                this.completedOutputs = new CompletedOutputIterator<Key, Value, Input, Output, Context>();
            else
                this.completedOutputs.Dispose();
        }

        internal bool CompletePending(bool getOutputs, bool wait, bool spinWaitForCommit)
        {
            UnsafeResumeThread();
            try
            {
                return UnsafeCompletePending(FasterSession, getOutputs, wait, spinWaitForCommit);
            }
            finally
            {
                UnsafeSuspendThread();
            }
        }

        internal bool UnsafeCompletePending<FasterSession>(FasterSession fasterSession, bool getOutputs, bool wait, bool spinWaitForCommit)
            where FasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            var requestedOutputs = getOutputs ? this.completedOutputs : default;
            var result = fht.InternalCompletePending(fasterSession, wait, requestedOutputs);
            if (spinWaitForCommit)
            {
                if (!wait)
                    throw new FasterException("Can spin-wait for commit (checkpoint completion) only if wait is true");
                do
                {
                    fht.InternalCompletePending(fasterSession, wait, requestedOutputs);
                    if (fht.InRestPhase())
                    {
                        fht.InternalCompletePending(fasterSession, wait, requestedOutputs);
                        return true;
                    }
                } while (wait);
            }
            return result;
        }

        /// <inheritdoc/>
        public ValueTask CompletePendingAsync(bool waitForCommit = false, CancellationToken token = default)
            => CompletePendingAsync(false, waitForCommit, token);

        /// <inheritdoc/>
        public async ValueTask<CompletedOutputIterator<Key, Value, Input, Output, Context>> CompletePendingWithOutputsAsync(bool waitForCommit = false, CancellationToken token = default)
        {
            InitializeCompletedOutputs();
            await CompletePendingAsync(true, waitForCommit, token).ConfigureAwait(false);
            return this.completedOutputs;
        }

        private async ValueTask CompletePendingAsync(bool getOutputs, bool waitForCommit = false, CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();

            if (fht.epoch.ThisInstanceProtected())
                throw new NotSupportedException("Async operations not supported over protected epoch");

            // Complete all pending operations on session
            await fht.CompletePendingAsync(this.FasterSession, token, getOutputs ? this.completedOutputs : null).ConfigureAwait(false);

            // Wait for commit if necessary
            if (waitForCommit)
                await WaitForCommitAsync(token).ConfigureAwait(false);
        }

        /// <summary>
        /// Check if at least one synchronous request is ready for CompletePending to be called on
        /// Returns completed immediately if there are no outstanding synchronous requests
        /// </summary>
        /// <param name="token"></param>
        /// <returns></returns>
        public async ValueTask ReadyToCompletePendingAsync(CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();

            if (fht.epoch.ThisInstanceProtected())
                throw new NotSupportedException("Async operations not supported over protected epoch");

            await FasterKV<Key, Value>.ReadyToCompletePendingAsync(this.ctx, token).ConfigureAwait(false);
        }

        #endregion Pending Operations

        #region Other Operations

        internal void UnsafeResetModified(ref Key key)
        {
            OperationStatus status;
            do
                status = fht.InternalModifiedBitOperation(ref key, out _);
            while (fht.HandleImmediateNonPendingRetryStatus<Input, Output, Context, InternalFasterSession>(status, FasterSession));
        }

        /// <inheritdoc/>
        public unsafe void ResetModified(Key key) => ResetModified(ref key);

        /// <inheritdoc/>
        internal bool IsModified(ref Key key)
        {
            UnsafeResumeThread();
            try
            {
                return UnsafeIsModified(ref key);
            }
            finally
            {
                UnsafeSuspendThread();
            }
        }

        internal bool UnsafeIsModified(ref Key key)
        {
            RecordInfo modifiedInfo;
            OperationStatus status;
            do
                status = fht.InternalModifiedBitOperation(ref key, out modifiedInfo, false);
            while (fht.HandleImmediateNonPendingRetryStatus<Input, Output, Context, InternalFasterSession>(status, FasterSession));
            return modifiedInfo.Modified;
        }

        /// <inheritdoc/>
        internal unsafe bool IsModified(Key key) => IsModified(ref key);

        /// <summary>
        /// Wait for commit of all operations completed until the current point in session.
        /// Does not itself issue checkpoint/commits.
        /// </summary>
        /// <returns></returns>
        public async ValueTask WaitForCommitAsync(CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();

            if (!ctx.prevCtx.pendingReads.IsEmpty || !ctx.pendingReads.IsEmpty)
                throw new FasterException("Make sure all async operations issued on this session are awaited and completed first");

            // Complete all pending sync operations on session
            await CompletePendingAsync(token: token).ConfigureAwait(false);

            var task = fht.CheckpointTask;
            CommitPoint localCommitPoint = LatestCommitPoint;
            if (localCommitPoint.UntilSerialNo >= ctx.serialNum && localCommitPoint.ExcludedSerialNos?.Count == 0)
                return;

            while (true)
            {
                await task.WithCancellationAsync(token).ConfigureAwait(false);
                Refresh();

                task = fht.CheckpointTask;
                localCommitPoint = LatestCommitPoint;
                if (localCommitPoint.UntilSerialNo >= ctx.serialNum && localCommitPoint.ExcludedSerialNos?.Count == 0)
                    break;
            }
        }

        /// <summary>
        /// Compact the log until specified address, moving active records to the tail of the log. BeginAddress is shifted, but the physical log
        /// is not deleted from disk. Caller is responsible for truncating the physical log on disk by taking a checkpoint or calling Log.Truncate
        /// </summary>
        /// <param name="compactUntilAddress">Compact log until this address</param>
        /// <param name="compactionType">Compaction type (whether we lookup records or scan log for liveness checking)</param>
        /// <returns>Address until which compaction was done</returns>
        public long Compact(long compactUntilAddress, CompactionType compactionType = CompactionType.Scan) 
            => Compact(compactUntilAddress, compactionType, default(DefaultCompactionFunctions<Key, Value>));

        /// <summary>
        /// Compact the log until specified address, moving active records to the tail of the log. BeginAddress is shifted, but the physical log
        /// is not deleted from disk. Caller is responsible for truncating the physical log on disk by taking a checkpoint or calling Log.Truncate
        /// </summary>
        /// <param name="input">Input for SingleWriter</param>
        /// <param name="output">Output from SingleWriter; it will be called all records that are moved, before Compact() returns, so the user must supply buffering or process each output completely</param>
        /// <param name="compactUntilAddress">Compact log until this address</param>
        /// <param name="compactionType">Compaction type (whether we lookup records or scan log for liveness checking)</param>
        /// <returns>Address until which compaction was done</returns>
        public long Compact(ref Input input, ref Output output, long compactUntilAddress, CompactionType compactionType = CompactionType.Scan)
            => Compact(ref input, ref output, compactUntilAddress, compactionType, default(DefaultCompactionFunctions<Key, Value>));

        /// <summary>
        /// Compact the log until specified address, moving active records to the tail of the log. BeginAddress is shifted, but the physical log
        /// is not deleted from disk. Caller is responsible for truncating the physical log on disk by taking a checkpoint or calling Log.Truncate
        /// </summary>
        /// <param name="untilAddress">Compact log until this address</param>
        /// <param name="compactionType">Compaction type (whether we lookup records or scan log for liveness checking)</param>
        /// <param name="compactionFunctions">User provided compaction functions (see <see cref="ICompactionFunctions{Key, Value}"/>).</param>
        /// <returns>Address until which compaction was done</returns>
        public long Compact<CompactionFunctions>(long untilAddress, CompactionType compactionType, CompactionFunctions compactionFunctions)
            where CompactionFunctions : ICompactionFunctions<Key, Value>
        {
            Input input = default;
            Output output = default;
            return fht.Compact<Input, Output, Context, Functions, CompactionFunctions>(functions, compactionFunctions, ref input, ref output, untilAddress, compactionType, 
                    new SessionVariableLengthStructSettings<Value, Input> { valueLength = variableLengthStruct, inputLength = inputVariableLengthStruct });
        }

        /// <summary>
        /// Compact the log until specified address, moving active records to the tail of the log. BeginAddress is shifted, but the physical log
        /// is not deleted from disk. Caller is responsible for truncating the physical log on disk by taking a checkpoint or calling Log.Truncate
        /// </summary>
        /// <param name="input">Input for SingleWriter</param>
        /// <param name="output">Output from SingleWriter; it will be called all records that are moved, before Compact() returns, so the user must supply buffering or process each output completely</param>
        /// <param name="untilAddress">Compact log until this address</param>
        /// <param name="compactionType">Compaction type (whether we lookup records or scan log for liveness checking)</param>
        /// <param name="compactionFunctions">User provided compaction functions (see <see cref="ICompactionFunctions{Key, Value}"/>).</param>
        /// <returns>Address until which compaction was done</returns>
        public long Compact<CompactionFunctions>(ref Input input, ref Output output, long untilAddress, CompactionType compactionType, CompactionFunctions compactionFunctions)
            where CompactionFunctions : ICompactionFunctions<Key, Value>
        {
            return fht.Compact<Input, Output, Context, Functions, CompactionFunctions>(functions, compactionFunctions, ref input, ref output, untilAddress, compactionType,
                    new SessionVariableLengthStructSettings<Value, Input> { valueLength = variableLengthStruct, inputLength = inputVariableLengthStruct });
        }

        /// <summary>
        /// Copy key and value to tail, succeed only if key is known to not exist in between expectedLogicalAddress and tail.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="desiredValue"></param>
        /// <param name="untilAddress">Lower-bound address (addresses are searched from tail (high) to head (low); do not search for "future records" earlier than this)</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status CompactionCopyToTail(ref Key key, ref Input input, ref Value desiredValue, ref Output output, long untilAddress)
        {
            UnsafeResumeThread();
            try
            {
                return fht.CompactionConditionalCopyToTail<Input, Output, Context, InternalFasterSession>(FasterSession, ref key, ref input, ref desiredValue, ref output, untilAddress);
            }
            finally
            {
                UnsafeSuspendThread();
            }
        }

        /// <summary>
        /// Experimental feature
        /// Checks whether specified record is present in memory
        /// (between HeadAddress and tail, or between fromAddress
        /// and tail), including tombstones.
        /// </summary>
        /// <param name="key">Key of the record.</param>
        /// <param name="logicalAddress">Logical address of record, if found</param>
        /// <param name="fromAddress">Look until this address</param>
        /// <returns>Status</returns>
        internal Status ContainsKeyInMemory(ref Key key, out long logicalAddress, long fromAddress = -1)
        {
            UnsafeResumeThread();
            try
            {
                return fht.InternalContainsKeyInMemory<Input, Output, Context, InternalFasterSession>(ref key, FasterSession, out logicalAddress, fromAddress);
            }
            finally
            {
                UnsafeSuspendThread();
            }
        }

        /// <summary>
        /// Iterator for all (distinct) live key-values stored in FASTER
        /// </summary>
        /// <param name="untilAddress">Report records until this address (tail by default)</param>
        /// <returns>FASTER iterator</returns>
        public IFasterScanIterator<Key, Value> Iterate(long untilAddress = -1)
        {
            if (untilAddress == -1)
                untilAddress = fht.Log.TailAddress;

            return new FasterKVIterator<Key, Value, Input, Output, Context, Functions>(fht, functions, untilAddress, loggerFactory: loggerFactory);
        }

        /// <summary>
        /// Resume session on current thread. IMPORTANT: Call SuspendThread before any async op.
        /// Call SuspendThread before any async op
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void UnsafeResumeThread()
        {
            // We do not track any "acquired" state here; if someone mixes calls between safe and unsafe contexts, they will 
            // get the "trying to acquire already-acquired epoch" error.
            fht.epoch.Resume();
            fht.InternalRefresh<Input, Output, Context, InternalFasterSession>(FasterSession);
        }

        /// <summary>
        /// Suspend session on current thread
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void UnsafeSuspendThread()
        {
            Debug.Assert(fht.epoch.ThisInstanceProtected());
            fht.epoch.Suspend();
        }

        void IClientSession.AtomicSwitch(long version)
        {
            fht.AtomicSwitch(ctx, ctx.prevCtx, version, fht._hybridLogCheckpoint.info.checkpointTokens);
        }

        /// <summary>
        /// Return true if Faster State Machine is in PREPARE sate
        /// </summary>
        internal bool IsInPreparePhase()
        {
            return this.fht.SystemState.Phase == Phase.PREPARE;
        }

        #endregion Other Operations

        #region IFasterSession

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool InPlaceUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref RecordInfo recordInfo, ref RMWInfo rmwInfo, out OperationStatus status)
        {
            recordInfo.SetDirty();

            // Note: KeyIndexes do not need notification of in-place updates because the key does not change.
            if (this.functions.InPlaceUpdater(ref key, ref input, ref value, ref output, ref rmwInfo))
            {
                rmwInfo.Action = RMWAction.Default;
                if (this.ctx.phase == Phase.REST)
                    this.fht.hlog.MarkPage(rmwInfo.Address, this.ctx.version);
                else
                    this.fht.hlog.MarkPageAtomic(rmwInfo.Address, this.ctx.version);
                status = OperationStatusUtils.AdvancedOpCode(OperationStatus.SUCCESS, StatusCode.InPlaceUpdatedRecord);
                return true;
            }
            if (rmwInfo.Action == RMWAction.CancelOperation)
            {
                status = OperationStatus.CANCELED;
                return false;
            }
            if (rmwInfo.Action == RMWAction.ExpireAndResume)
            {
                // This inserts the tombstone if appropriate
                return this.fht.ReinitializeExpiredRecord<Input, Output, Context, InternalFasterSession>(ref key, ref input, ref value, ref output, ref recordInfo,
                                                    ref rmwInfo, rmwInfo.Address, this.FasterSession, isIpu: true, out status);
            }
            if (rmwInfo.Action == RMWAction.ExpireAndStop)
            {
                recordInfo.Tombstone = true;
                status = OperationStatusUtils.AdvancedOpCode(OperationStatus.SUCCESS, StatusCode.InPlaceUpdatedRecord | StatusCode.Expired);
                return false;
            }

            status = OperationStatus.SUCCESS;
            return false;
        }


        // This is a struct to allow JIT to inline calls (and bypass default interface call mechanism)
        internal readonly struct InternalFasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            private readonly ClientSession<Key, Value, Input, Output, Context, Functions> _clientSession;

            public InternalFasterSession(ClientSession<Key, Value, Input, Output, Context, Functions> clientSession)
            {
                _clientSession = clientSession;
            }

            public bool IsManualLocking => false;

            #region IFunctions - Reads
            public bool SingleReader(ref Key key, ref Input input, ref Value value, ref Output dst, ref RecordInfo recordInfo, ref ReadInfo readInfo)
                => _clientSession.functions.SingleReader(ref key, ref input, ref value, ref dst, ref readInfo);

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool ConcurrentReader(ref Key key, ref Input input, ref Value value, ref Output dst, ref RecordInfo recordInfo, ref ReadInfo readInfo, out EphemeralLockResult lockResult)
            {
                lockResult = EphemeralLockResult.Success;
                return _clientSession.fht.DoEphemeralLocking
                    ? ConcurrentReaderLockEphemeral(ref key, ref input, ref value, ref dst, ref recordInfo, ref readInfo, out lockResult)
                    : _clientSession.functions.ConcurrentReader(ref key, ref input, ref value, ref dst, ref readInfo);
            }

            public bool ConcurrentReaderLockEphemeral(ref Key key, ref Input input, ref Value value, ref Output dst, ref RecordInfo recordInfo, ref ReadInfo readInfo, out EphemeralLockResult lockResult)
            {
                lockResult = recordInfo.TryLockShared() ? EphemeralLockResult.Success : EphemeralLockResult.Failed;
                if (lockResult == EphemeralLockResult.Failed)
                    return false;
                try
                {
                    return _clientSession.functions.ConcurrentReader(ref key, ref input, ref value, ref dst, ref readInfo);
                }
                finally
                {
                    recordInfo.UnlockShared();
                }
            }

            public void ReadCompletionCallback(ref Key key, ref Input input, ref Output output, Context ctx, Status status, RecordMetadata recordMetadata)
                => _clientSession.functions.ReadCompletionCallback(ref key, ref input, ref output, ctx, status, recordMetadata);

            #endregion IFunctions - Reads

            #region IFunctions - Upserts
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool SingleWriter(ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref RecordInfo recordInfo, ref UpsertInfo upsertInfo, WriteReason reason) 
                => _clientSession.functions.SingleWriter(ref key, ref input, ref src, ref dst, ref output, ref upsertInfo, reason);

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void PostSingleWriter(ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref RecordInfo recordInfo, ref UpsertInfo upsertInfo, WriteReason reason) 
            {
                if (_clientSession.fht.DoEphemeralLocking)
                    PostSingleWriterLockEphemeral(ref key, ref input, ref src, ref dst, ref output, ref recordInfo, ref upsertInfo, reason);
                else
                    PostSingleWriterNoLock(ref key, ref input, ref src, ref dst, ref output, ref recordInfo, ref upsertInfo, reason);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void PostSingleWriterNoLock(ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref RecordInfo recordInfo, ref UpsertInfo upsertInfo, WriteReason reason)
            {
                recordInfo.SetDirtyAndModified();
                _clientSession.functions.PostSingleWriter(ref key, ref input, ref src, ref dst, ref output, ref upsertInfo, reason);
            }

            public void PostSingleWriterLockEphemeral(ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref RecordInfo recordInfo, ref UpsertInfo upsertInfo, WriteReason reason)
            {
                try
                {
                    PostSingleWriterNoLock(ref key, ref input, ref src, ref dst, ref output, ref recordInfo, ref upsertInfo, reason);
                }
                finally
                {
                    if (reason == WriteReason.Upsert)
                        recordInfo.UnlockExclusive();
                    else if (recordInfo.IsLockedShared) // readcache records are not locked
                        recordInfo.UnlockShared();
                }
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool ConcurrentWriter(ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref RecordInfo recordInfo, ref UpsertInfo upsertInfo, out EphemeralLockResult lockResult)
            {
                lockResult = EphemeralLockResult.Success;
                return _clientSession.fht.DoEphemeralLocking
                    ? ConcurrentWriterLockEphemeral(ref key, ref input, ref src, ref dst, ref output, ref recordInfo, ref upsertInfo, out lockResult)
                    : ConcurrentWriterNoLock(ref key, ref input, ref src, ref dst, ref output, ref recordInfo, ref upsertInfo);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private bool ConcurrentWriterNoLock(ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref RecordInfo recordInfo, ref UpsertInfo upsertInfo)
            {
                if (!_clientSession.functions.ConcurrentWriter(ref key, ref input, ref src, ref dst, ref output, ref upsertInfo))
                    return false;
                recordInfo.SetDirtyAndModified();
                return true;
            }

            public bool ConcurrentWriterLockEphemeral(ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref RecordInfo recordInfo, ref UpsertInfo upsertInfo, out EphemeralLockResult lockResult)
            {
                lockResult = recordInfo.TryLockExclusive() ? EphemeralLockResult.Success : EphemeralLockResult.Failed;
                if (lockResult == EphemeralLockResult.Failed)
                    return false;
                try
                {
                    if (ConcurrentWriterNoLock(ref key, ref input, ref src, ref dst, ref output, ref recordInfo, ref upsertInfo))
                        return true;
                    if (upsertInfo.Action != UpsertAction.CancelOperation)
                        lockResult = EphemeralLockResult.HoldForSeal;
                    return false;
                }
                finally
                {
                    if (lockResult != EphemeralLockResult.HoldForSeal)
                        recordInfo.UnlockExclusive();
                }
            }
            #endregion IFunctions - Upserts

            #region IFunctions - RMWs
            #region InitialUpdater
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool NeedInitialUpdate(ref Key key, ref Input input, ref Output output, ref RMWInfo rmwInfo)
                => _clientSession.functions.NeedInitialUpdate(ref key, ref input, ref output, ref rmwInfo);

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool InitialUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref RecordInfo recordInfo, ref RMWInfo rmwInfo) 
                => _clientSession.functions.InitialUpdater(ref key, ref input, ref value, ref output, ref rmwInfo);

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void PostInitialUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref RecordInfo recordInfo, ref RMWInfo rmwInfo) 
            {
                if (_clientSession.fht.DoEphemeralLocking)
                    PostInitialUpdaterLockEphemeral(ref key, ref input, ref value, ref output, ref recordInfo, ref rmwInfo);
                else
                    PostInitialUpdaterNoLock(ref key, ref input, ref value, ref output, ref recordInfo, ref rmwInfo);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void PostInitialUpdaterNoLock(ref Key key, ref Input input, ref Value value, ref Output output, ref RecordInfo recordInfo, ref RMWInfo rmwInfo)
            {
                recordInfo.SetDirtyAndModified();
                _clientSession.functions.PostInitialUpdater(ref key, ref input, ref value, ref output, ref rmwInfo);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void PostInitialUpdaterLockEphemeral(ref Key key, ref Input input, ref Value value, ref Output output, ref RecordInfo recordInfo, ref RMWInfo rmwInfo)
            {
                try
                { 
                    PostInitialUpdaterNoLock(ref key, ref input, ref value, ref output, ref recordInfo, ref rmwInfo);
                }
                finally
                {
                    recordInfo.UnlockExclusive();
                }
            }
            #endregion InitialUpdater

            #region CopyUpdater
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool NeedCopyUpdate(ref Key key, ref Input input, ref Value oldValue, ref Output output, ref RMWInfo rmwInfo)
                => _clientSession.functions.NeedCopyUpdate(ref key, ref input, ref oldValue, ref output, ref rmwInfo);

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool CopyUpdater(ref Key key, ref Input input, ref Value oldValue, ref Value newValue, ref Output output, ref RecordInfo recordInfo, ref RMWInfo rmwInfo) 
                => _clientSession.functions.CopyUpdater(ref key, ref input, ref oldValue, ref newValue, ref output, ref rmwInfo);

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void PostCopyUpdater(ref Key key, ref Input input, ref Value oldValue, ref Value newValue, ref Output output, ref RecordInfo recordInfo, ref RMWInfo rmwInfo) 
            {
                if (_clientSession.fht.DoEphemeralLocking)
                    PostCopyUpdaterLockEphemeral(ref key, ref input, ref oldValue, ref newValue, ref output, ref recordInfo, ref rmwInfo);
                else
                    PostCopyUpdaterNoLock(ref key, ref input, ref oldValue, ref newValue, ref output, ref recordInfo, ref rmwInfo);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void PostCopyUpdaterNoLock(ref Key key, ref Input input, ref Value oldValue, ref Value newValue, ref Output output, ref RecordInfo recordInfo, ref RMWInfo rmwInfo)
            {
                recordInfo.SetDirtyAndModified();
                _clientSession.functions.PostCopyUpdater(ref key, ref input, ref oldValue, ref newValue, ref output, ref rmwInfo);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void PostCopyUpdaterLockEphemeral(ref Key key, ref Input input, ref Value oldValue, ref Value newValue, ref Output output, ref RecordInfo recordInfo, ref RMWInfo rmwInfo)
            {
                try
                {
                    PostCopyUpdaterNoLock(ref key, ref input, ref oldValue, ref newValue, ref output, ref recordInfo, ref rmwInfo);
                }
                finally
                {
                    recordInfo.UnlockExclusive();
                }
            }
            #endregion CopyUpdater

            #region InPlaceUpdater
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool InPlaceUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref RecordInfo recordInfo, ref RMWInfo rmwInfo, out OperationStatus status,
                                       out EphemeralLockResult lockResult)
            {
                lockResult = EphemeralLockResult.Success;
                return _clientSession.fht.DoEphemeralLocking
                                ? InPlaceUpdaterLockEphemeral(ref key, ref input, ref value, ref output, ref recordInfo, ref rmwInfo, out status, out lockResult)
                                : InPlaceUpdaterNoLock(ref key, ref input, ref value, ref output, ref recordInfo, ref rmwInfo, out status);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool InPlaceUpdaterNoLock(ref Key key, ref Input input, ref Value value, ref Output output, ref RecordInfo recordInfo, ref RMWInfo rmwInfo, out OperationStatus status)
            {
                if (!_clientSession.InPlaceUpdater(ref key, ref input, ref value, ref output, ref recordInfo, ref rmwInfo, out status))
                    return false;
                recordInfo.SetDirtyAndModified();
                return true;
            }

            public bool InPlaceUpdaterLockEphemeral(ref Key key, ref Input input, ref Value value, ref Output output, ref RecordInfo recordInfo, ref RMWInfo rmwInfo, out OperationStatus status,
                                                    out EphemeralLockResult lockResult)
            {
                lockResult = recordInfo.TryLockExclusive() ? EphemeralLockResult.Success : EphemeralLockResult.Failed;
                if (lockResult == EphemeralLockResult.Failed)
                {
                    status = OperationStatus.RETRY_LATER;
                    return false;
                }
                try
                { 
                    if (InPlaceUpdaterNoLock(ref key, ref input, ref value, ref output, ref recordInfo, ref rmwInfo, out status))
                        return true;
                    // Expiration sets additional bits beyond SUCCESS, and Cancel does not set SUCCESS.
                    if (status == OperationStatus.SUCCESS)
                        lockResult = EphemeralLockResult.HoldForSeal;
                    return false;
                }
                finally
                {
                    if (lockResult != EphemeralLockResult.HoldForSeal)
                        recordInfo.UnlockExclusive();
                }
            }

            public void RMWCompletionCallback(ref Key key, ref Input input, ref Output output, Context ctx, Status status, RecordMetadata recordMetadata)
                => _clientSession.functions.RMWCompletionCallback(ref key, ref input, ref output, ctx, status, recordMetadata);

            #endregion InPlaceUpdater
            #endregion IFunctions - RMWs

            #region IFunctions - Deletes
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool SingleDeleter(ref Key key, ref Value value, ref RecordInfo recordInfo, ref DeleteInfo deleteInfo) 
                => _clientSession.functions.SingleDeleter(ref key, ref value, ref deleteInfo);

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void PostSingleDeleter(ref Key key, ref RecordInfo recordInfo, ref DeleteInfo deleteInfo) 
            {
                if (_clientSession.fht.DoEphemeralLocking)
                    PostSingleDeleterLockEphemeral(ref key, ref recordInfo, ref deleteInfo);
                else
                    PostSingleDeleterNoLock(ref key, ref recordInfo, ref deleteInfo);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void PostSingleDeleterNoLock(ref Key key, ref RecordInfo recordInfo, ref DeleteInfo deleteInfo)
            {
                recordInfo.SetDirtyAndModified();
                _clientSession.functions.PostSingleDeleter(ref key, ref deleteInfo);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void PostSingleDeleterLockEphemeral(ref Key key, ref RecordInfo recordInfo, ref DeleteInfo deleteInfo)
            {
                try
                {
                    PostSingleDeleterNoLock(ref key, ref recordInfo, ref deleteInfo);
                }
                finally
                {
                    recordInfo.UnlockExclusive();
                }
            }
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool ConcurrentDeleter(ref Key key, ref Value value, ref RecordInfo recordInfo, ref DeleteInfo deleteInfo, out EphemeralLockResult lockResult)
            {
                lockResult = EphemeralLockResult.Success;
                return _clientSession.fht.DoEphemeralLocking
                    ? ConcurrentDeleterLockEphemeral(ref key, ref value, ref recordInfo, ref deleteInfo, out lockResult)
                    : ConcurrentDeleterNoLock(ref key, ref value, ref recordInfo, ref deleteInfo);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool ConcurrentDeleterNoLock(ref Key key, ref Value value, ref RecordInfo recordInfo, ref DeleteInfo deleteInfo)
            { 
                if (!_clientSession.functions.ConcurrentDeleter(ref key, ref value, ref deleteInfo))
                    return false;
                recordInfo.SetDirtyAndModified();
                recordInfo.SetTombstone();
                return true;
            }

            public bool ConcurrentDeleterLockEphemeral(ref Key key, ref Value value, ref RecordInfo recordInfo, ref DeleteInfo deleteInfo, out EphemeralLockResult lockResult)
            {
                lockResult = recordInfo.TryLockExclusive() ? EphemeralLockResult.Success : EphemeralLockResult.Failed;
                if (lockResult == EphemeralLockResult.Failed)
                    return false;
                try
                {
                    return ConcurrentDeleterNoLock(ref key, ref value, ref recordInfo, ref deleteInfo);
                }
                finally
                {
                    recordInfo.UnlockExclusive();
                }
            }
            #endregion IFunctions - Deletes

            #region IFunctions - Dispose
            public void DisposeSingleWriter(ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref RecordInfo recordInfo, ref UpsertInfo upsertInfo, WriteReason reason)
                => _clientSession.functions.DisposeSingleWriter(ref key, ref input, ref src, ref dst, ref output, ref upsertInfo, reason);
            public void DisposeCopyUpdater(ref Key key, ref Input input, ref Value oldValue, ref Value newValue, ref Output output, ref RecordInfo recordInfo, ref RMWInfo rmwInfo)
                => _clientSession.functions.DisposeCopyUpdater(ref key, ref input, ref oldValue, ref newValue, ref output, ref rmwInfo);
            public void DisposeInitialUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref RecordInfo recordInfo, ref RMWInfo rmwInfo)
                => _clientSession.functions.DisposeInitialUpdater(ref key, ref input, ref value, ref output, ref rmwInfo);
            public void DisposeSingleDeleter(ref Key key, ref Value value, ref RecordInfo recordInfo, ref DeleteInfo deleteInfo)
                => _clientSession.functions.DisposeSingleDeleter(ref key, ref value, ref deleteInfo);
            public void DisposeDeserializedFromDisk(ref Key key, ref Value value, ref RecordInfo recordInfo)
                => _clientSession.functions.DisposeDeserializedFromDisk(ref key, ref value);
            #endregion IFunctions - Dispose

            #region IFunctions - Checkpointing
            public void CheckpointCompletionCallback(int sessionID, string sessionName, CommitPoint commitPoint)
            {
                _clientSession.functions.CheckpointCompletionCallback(sessionID, sessionName, commitPoint);
                _clientSession.LatestCommitPoint = commitPoint;
            }
            #endregion IFunctions - Checkpointing

            #region Transient locking
            public bool TryLockTransientExclusive(ref Key key, ref OperationStackContext<Key, Value> stackCtx)
            {
                if (!_clientSession.fht.DoTransientLocking)
                    return true;
                if (!_clientSession.fht.LockTable.TryLockTransientExclusive(ref key, ref stackCtx.hei))
                    return false;
                return stackCtx.recSrc.HasTransientLock = true;
            }

            public bool TryLockTransientShared(ref Key key, ref OperationStackContext<Key, Value> stackCtx)
            {
                if (!_clientSession.fht.DoTransientLocking)
                    return true;
                if (!_clientSession.fht.LockTable.TryLockTransientShared(ref key, ref stackCtx.hei))
                    return false;
                return stackCtx.recSrc.HasTransientLock = true;
            }

            public void UnlockTransientExclusive(ref Key key, ref OperationStackContext<Key, Value> stackCtx)
            {
                if (!_clientSession.fht.DoTransientLocking)
                    return;
                _clientSession.fht.LockTable.UnlockExclusive(ref key, ref stackCtx.hei);
                stackCtx.recSrc.HasTransientLock = false;
            }

            public void UnlockTransientShared(ref Key key, ref OperationStackContext<Key, Value> stackCtx)
            {
                if (!_clientSession.fht.DoTransientLocking)
                    return;
                _clientSession.fht.LockTable.UnlockShared(ref key, ref stackCtx.hei);
                stackCtx.recSrc.HasTransientLock = false;
            }
            #endregion Transient locking

            #region Internal utilities
            public int GetInitialLength(ref Input input)
                => _clientSession.variableLengthStruct.GetInitialLength(ref input);

            public int GetLength(ref Value t, ref Input input)
                => _clientSession.variableLengthStruct.GetLength(ref t, ref input);

            public IHeapContainer<Input> GetHeapContainer(ref Input input)
            {
                if (_clientSession.inputVariableLengthStruct == default)
                    return new StandardHeapContainer<Input>(ref input);
                return new VarLenHeapContainer<Input>(ref input, _clientSession.inputVariableLengthStruct, _clientSession.fht.hlog.bufferPool);
            }

            public void UnsafeResumeThread() => _clientSession.UnsafeResumeThread();

            public void UnsafeSuspendThread() => _clientSession.UnsafeSuspendThread();

            public bool CompletePendingWithOutputs(out CompletedOutputIterator<Key, Value, Input, Output, Context> completedOutputs, bool wait = false, bool spinWaitForCommit = false)
                => _clientSession.CompletePendingWithOutputs(out completedOutputs, wait, spinWaitForCommit);

            public FasterKV<Key, Value>.FasterExecutionContext<Input, Output, Context> Ctx => this._clientSession.ctx;
            #endregion Internal utilities
        }
        #endregion IFasterSession
    }
}
