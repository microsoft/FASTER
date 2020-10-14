// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma warning disable 0162

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
    public sealed class ClientSession<Key, Value, Input, Output, Context, Functions> : IClientSession, IDisposable
        where Functions : IFunctions<Key, Value, Input, Output, Context>
    {
        private readonly FasterKV<Key, Value> fht;

        internal readonly bool SupportAsync = false;
        internal readonly FasterKV<Key, Value>.FasterExecutionContext<Input, Output, Context> ctx;
        internal CommitPoint LatestCommitPoint;

        internal readonly Functions functions;
        internal readonly IVariableLengthStruct<Value, Input> variableLengthStruct;
        internal readonly IVariableLengthStruct<Input> inputVariableLengthStruct;

        internal readonly AsyncFasterSession FasterSession;

        internal ClientSession(
            FasterKV<Key, Value> fht,
            FasterKV<Key, Value>.FasterExecutionContext<Input, Output, Context> ctx,
            Functions functions,
            bool supportAsync,
            SessionVariableLengthStructSettings<Value, Input> sessionVariableLengthStructSettings = null)
        {
            this.fht = fht;
            this.ctx = ctx;
            this.functions = functions;
            SupportAsync = supportAsync;
            LatestCommitPoint = new CommitPoint { UntilSerialNo = -1, ExcludedSerialNos = null };
            FasterSession = new AsyncFasterSession(this);

            this.variableLengthStruct = sessionVariableLengthStructSettings?.valueLength;
            if (this.variableLengthStruct == default)
            {
                if (fht.hlog is VariableLengthBlittableAllocator<Key, Value> allocator)
                {
                    Debug.WriteLine("Warning: Session did not specify Input-specific functions for variable-length values via IVariableLengthStruct<Value, Input>");
                    this.variableLengthStruct = new DefaultVariableLengthStruct<Value, Input>(allocator.ValueLength);
                }
            }
            else
            {
                if (!(fht.hlog is VariableLengthBlittableAllocator<Key, Value>))
                    Debug.WriteLine("Warning: Session param of variableLengthStruct provided for non-varlen allocator");
            }

            this.inputVariableLengthStruct = sessionVariableLengthStructSettings?.inputLength;

            if (typeof(Input) == typeof(SpanByte) && inputVariableLengthStruct == default)
            {
                inputVariableLengthStruct = new SpanByteLength() as IVariableLengthStruct<Input>;
            }
            
            // Session runs on a single thread
            if (!supportAsync)
                UnsafeResumeThread();
        }

        /// <summary>
        /// Get session ID
        /// </summary>
        public string ID { get { return ctx.guid; } }

        /// <summary>
        /// Next sequential serial no for session (current serial no + 1)
        /// </summary>
        public long NextSerialNo => ctx.serialNum + 1;

        /// <summary>
        /// Current serial no for session
        /// </summary>
        public long SerialNo => ctx.serialNum;

        /// <summary>
        /// Dispose session
        /// </summary>
        public void Dispose()
        {
            CompletePending(true);
            fht.DisposeClientSession(ID);

            // Session runs on a single thread
            if (!SupportAsync)
                UnsafeSuspendThread();
        }

        /// <summary>
        /// Read operation
        /// </summary>
        /// <param name="key">The key to look up</param>
        /// <param name="input">Input to help extract the retrieved value into <paramref name="output"/></param>
        /// <param name="output">The location to place the retrieved value</param>
        /// <param name="userContext">User application context passed in case the read goes pending due to IO</param>
        /// <param name="serialNo">The serial number of the operation (used in recovery)</param>
        /// <returns><paramref name="output"/> is populated by the <see cref="IFunctions{Key, Value, Context}"/> implementation</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read(ref Key key, ref Input input, ref Output output, Context userContext = default, long serialNo = 0)
        {
            if (SupportAsync) UnsafeResumeThread();
            try
            {
                return fht.ContextRead(ref key, ref input, ref output, userContext, FasterSession, serialNo, ctx);
            }
            finally
            {
                if (SupportAsync) UnsafeSuspendThread();
            }
        }

        /// <summary>
        /// Read operation
        /// </summary>
        /// <param name="key">The key to look up</param>
        /// <param name="input">Input to help extract the retrieved value into <paramref name="output"/></param>
        /// <param name="output">The location to place the retrieved value</param>
        /// <param name="userContext">User application context passed in case the read goes pending due to IO</param>
        /// <param name="serialNo">The serial number of the operation (used in recovery)</param>
        /// <returns><paramref name="output"/> is populated by the <see cref="IFunctions{Key, Value, Context}"/> implementation</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read(Key key, Input input, out Output output, Context userContext = default, long serialNo = 0)
        {
            output = default;
            return Read(ref key, ref input, ref output, userContext, serialNo);
        }

        /// <summary>
        /// Read operation
        /// </summary>
        /// <param name="key">The key to look up</param>
        /// <param name="output">The location to place the retrieved value</param>
        /// <param name="userContext">User application context passed in case the read goes pending due to IO</param>
        /// <param name="serialNo">The serial number of the operation (used in recovery)</param>
        /// <returns><paramref name="output"/> is populated by the <see cref="IFunctions{Key, Value, Context}"/> implementation</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read(ref Key key, ref Output output, Context userContext = default, long serialNo = 0)
        {
            Input input = default;
            return Read(ref key, ref input, ref output, userContext, serialNo);
        }

        /// <summary>
        /// Read operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="output"></param>
        /// <param name="userContext"></param>
        /// <param name="serialNo"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read(Key key, out Output output, Context userContext = default, long serialNo = 0)
        {
            Input input = default;
            output = default;
            return Read(ref key, ref input, ref output, userContext, serialNo);
        }

        /// <summary>
        /// Read operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="userContext"></param>
        /// <param name="serialNo"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public (Status, Output) Read(Key key, Context userContext = default, long serialNo = 0)
        {
            Input input = default;
            Output output = default;
            return (Read(ref key, ref input, ref output, userContext, serialNo), output);
        }

        /// <summary>
        /// Read operation that accepts a <paramref name="startAddress"/> to start the lookup at instead of starting at the hash table entry for <paramref name="key"/>,
        ///     and returns the <see cref="RecordInfo"/> for the found record (which contains previous address in the hash chain for this key; this can
        ///     be used as <paramref name="startAddress"/> in a subsequent call to iterate all records for <paramref name="key"/>).
        /// </summary>
        /// <param name="key">The key to look up</param>
        /// <param name="input">Input to help extract the retrieved value into <paramref name="output"/></param>
        /// <param name="output">The location to place the retrieved value</param>
        /// <param name="startAddress">Start at this address rather than the address in the hash table for <paramref name="key"/>"/></param>
        /// <param name="recordInfo">Receives a copy of the record's header. From this the <see cref="RecordInfo.PreviousAddress"/> can be obtained and passed
        ///     in a subsequent call as <paramref name="startAddress"/>, thereby enumerating all records in a hash chain.</param>
        /// <param name="userContext">User application context passed in case the read goes pending due to IO</param>
        /// <param name="serialNo">The serial number of the operation (used in recovery)</param>
        /// <returns><paramref name="output"/> is populated by the <see cref="IFunctions{Key, Value, Context}"/> implementation</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read(ref Key key, ref Input input, ref Output output, long startAddress, out RecordInfo recordInfo, Context userContext = default, long serialNo = 0)
        {
            if (SupportAsync) UnsafeResumeThread();
            try
            {
                return fht.ContextRead(ref key, ref input, ref output, startAddress, out recordInfo, userContext, FasterSession, serialNo, ctx);
            }
            finally
            {
                if (SupportAsync) UnsafeSuspendThread();
            }
        }

        /// <summary>
        /// Async read operation. May return uncommitted results; to ensure reading of committed results, complete the read and then call WaitForCommitAsync.
        /// </summary>
        /// <param name="key">The key to look up</param>
        /// <param name="input">Input to help extract the retrieved value into output</param>
        /// <param name="userContext">User application context passed in case the read goes pending due to IO</param>
        /// <param name="serialNo">The serial number of the operation (used in recovery)</param>
        /// <param name="cancellationToken">Token to cancel the operation</param>
        /// <returns>ReadAsyncResult - call <see cref="FasterKV{Key, Value}.ReadAsyncResult{Input, Output, Context, Functions}.Complete()"/>
        ///     or <see cref="FasterKV{Key, Value}.ReadAsyncResult{Input, Output, Context, Functions}.Complete(out RecordInfo)"/> 
        ///     on the return value to complete the read operation and obtain the result status, the output that is populated by the 
        ///     <see cref="IFunctions{Key, Value, Context}"/> implementation, and optionally a copy of the header for the retrieved record</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<FasterKV<Key, Value>.ReadAsyncResult<Input, Output, Context, Functions>> ReadAsync(ref Key key, ref Input input, Context userContext = default, long serialNo = 0, CancellationToken cancellationToken = default)
        {
            Debug.Assert(SupportAsync, "Session does not support async operations");
            return fht.ReadAsync(this, ref key, ref input, Constants.kInvalidAddress, userContext, serialNo, cancellationToken);
        }

        /// <summary>
        /// Async read operation, may return uncommitted result
        /// To ensure reading of committed result, complete the read and then call WaitForCommitAsync.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="context"></param>
        /// <param name="serialNo"></param>
        /// <param name="token"></param>
        /// <returns>ReadAsyncResult - call Complete() on the return value to complete the read operation</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<FasterKV<Key, Value>.ReadAsyncResult<Input, Output, Context, Functions>> ReadAsync(Key key, Input input, Context context = default, long serialNo = 0, CancellationToken token = default)
        {
            Debug.Assert(SupportAsync, "Session does not support async operations");
            return fht.ReadAsync(this, ref key, ref input, Constants.kInvalidAddress, context, serialNo, token);
        }


        /// <summary>
        /// Async read operation. May return uncommitted results; to ensure reading of committed results, complete the read and then call WaitForCommitAsync.
        /// </summary>
        /// <param name="key">The key to look up</param>
        /// <param name="userContext">User application context passed in case the read goes pending due to IO</param>
        /// <param name="serialNo">The serial number of the operation (used in recovery)</param>
        /// <param name="token">Token to cancel the operation</param>
        /// <returns>ReadAsyncResult - call <see cref="FasterKV{Key, Value}.ReadAsyncResult{Input, Output, Context, Functions}.Complete()"/>
        ///     or <see cref="FasterKV{Key, Value}.ReadAsyncResult{Input, Output, Context, Functions}.Complete(out RecordInfo)"/> 
        ///     on the return value to complete the read operation and obtain the result status, the output that is populated by the 
        ///     <see cref="IFunctions{Key, Value, Context}"/> implementation, and optionally a copy of the header for the retrieved record</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<FasterKV<Key, Value>.ReadAsyncResult<Input, Output, Context, Functions>> ReadAsync(ref Key key, Context userContext = default, long serialNo = 0, CancellationToken token = default)
        {
            Debug.Assert(SupportAsync, "Session does not support async operations");
            Input input = default;
            return fht.ReadAsync(this, ref key, ref input, Constants.kInvalidAddress, userContext, serialNo, token);
        }

        /// <summary>
        /// Async read operation, may return uncommitted result
        /// To ensure reading of committed result, complete the read and then call WaitForCommitAsync.
        /// </summary>
        /// <param name="key"></param>
        /// <param name="context"></param>
        /// <param name="serialNo"></param>
        /// <param name="token"></param>
        /// <returns>ReadAsyncResult - call Complete() on the return value to complete the read operation</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<FasterKV<Key, Value>.ReadAsyncResult<Input, Output, Context, Functions>> ReadAsync(Key key, Context context = default, long serialNo = 0, CancellationToken token = default)
        {
            Input input = default;
            return fht.ReadAsync(this, ref key, ref input, Constants.kInvalidAddress, context, serialNo, token);
        }

        /// <summary>
        /// Async read operation that accepts a <paramref name="startAddress"/> to start the lookup at instead of starting at the hash table entry for <paramref name="key"/>,
        ///     and returns the <see cref="RecordInfo"/> for the found record (which contains previous address in the hash chain for this key; this can
        ///     be used as <paramref name="startAddress"/> in a subsequent call to iterate all records for <paramref name="key"/>).
        /// </summary>
        /// <param name="key">The key to look up</param>
        /// <param name="input">Input to help extract the retrieved value into output</param>
        /// <param name="startAddress">Start at this address rather than the address in the hash table for <paramref name="key"/>"/></param>
        /// <param name="userContext">User application context passed in case the read goes pending due to IO</param>
        /// <param name="serialNo">The serial number of the operation (used in recovery)</param>
        /// <param name="cancellationToken">Token to cancel the operation</param>
        /// <returns>ReadAsyncResult - call <see cref="FasterKV{Key, Value}.ReadAsyncResult{Input, Output, Context, Functions}.Complete()"/>
        ///     or <see cref="FasterKV{Key, Value}.ReadAsyncResult{Input, Output, Context, Functions}.Complete(out RecordInfo)"/> 
        ///     on the return value to complete the read operation and obtain the result status, the output that is populated by the 
        ///     <see cref="IFunctions{Key, Value, Context}"/> implementation, and optionally a copy of the header for the retrieved record</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<FasterKV<Key, Value>.ReadAsyncResult<Input, Output, Context, Functions>> ReadAsync(ref Key key, ref Input input, long startAddress, Context userContext = default, long serialNo = 0, CancellationToken cancellationToken = default)
        {
            Debug.Assert(SupportAsync, "Session does not support async operations");
            return fht.ReadAsync(this, ref key, ref input, startAddress, userContext, serialNo, cancellationToken);
        }

        /// <summary>
        /// Upsert operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="desiredValue"></param>
        /// <param name="userContext"></param>
        /// <param name="serialNo"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(ref Key key, ref Value desiredValue, Context userContext = default, long serialNo = 0)
        {
            if (SupportAsync) UnsafeResumeThread();
            try
            {
                return fht.ContextUpsert(ref key, ref desiredValue, userContext, FasterSession, serialNo, ctx);
            }
            finally
            {
                if (SupportAsync) UnsafeSuspendThread();
            }
        }

        /// <summary>
        /// Upsert operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="desiredValue"></param>
        /// <param name="userContext"></param>
        /// <param name="serialNo"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(Key key, Value desiredValue, Context userContext = default, long serialNo = 0)
            => Upsert(ref key, ref desiredValue, userContext, serialNo);

        /// <summary>
        /// RMW operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="userContext"></param>
        /// <param name="serialNo"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW(ref Key key, ref Input input, Context userContext = default, long serialNo = 0)
        {
            if (SupportAsync) UnsafeResumeThread();
            try
            {
                return fht.ContextRMW(ref key, ref input, userContext, FasterSession, serialNo, ctx);
            }
            finally
            {
                if (SupportAsync) UnsafeSuspendThread();
            }
        }

        /// <summary>
        /// RMW operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="userContext"></param>
        /// <param name="serialNo"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW(Key key, Input input, Context userContext = default, long serialNo = 0)
            => RMW(ref key, ref input, userContext, serialNo);

        /// <summary>
        /// Async RMW operation
        /// Await operation in session before issuing next one
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="context"></param>
        /// <param name="serialNo"></param>
        /// <param name="token"></param>
        /// <returns>ValueTask for RMW result, user needs to await and then call Complete() on the result</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<FasterKV<Key, Value>.RmwAsyncResult<Input, Output, Context, Functions>> RMWAsync(ref Key key, ref Input input, Context context = default, long serialNo = 0, CancellationToken token = default)
        {
            Debug.Assert(SupportAsync, "Session does not support async operations");
            return fht.RmwAsync(this, ref key, ref input, context, serialNo, token);
        }

        /// <summary>
        /// Async RMW operation
        /// Await operation in session before issuing next one
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="context"></param>
        /// <param name="serialNo"></param>
        /// <param name="token"></param>
        /// <returns>ValueTask for RMW result, user needs to await and then call Complete() on the result</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<FasterKV<Key, Value>.RmwAsyncResult<Input, Output, Context, Functions>> RMWAsync(Key key, Input input, Context context = default, long serialNo = 0, CancellationToken token = default)
            => RMWAsync(ref key, ref input, context, serialNo, token);

        /// <summary>
        /// Delete operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="userContext"></param>
        /// <param name="serialNo"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Delete(ref Key key, Context userContext = default, long serialNo = 0)
        {
            if (SupportAsync) UnsafeResumeThread();
            try
            {
                return fht.ContextDelete(ref key, userContext, FasterSession, serialNo, ctx);
            }
            finally
            {
                if (SupportAsync) UnsafeSuspendThread();
            }
        }

        /// <summary>
        /// Delete operation
        /// </summary>
        /// <param name="key"></param>
        /// <param name="userContext"></param>
        /// <param name="serialNo"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Delete(Key key, Context userContext = default, long serialNo = 0)
            => Delete(ref key, userContext, serialNo);

        /// <summary>
        /// Experimental feature
        /// Checks whether specified record is present in memory
        /// (between HeadAddress and tail, or between fromAddress
        /// and tail)
        /// </summary>
        /// <param name="key">Key of the record.</param>
        /// <param name="fromAddress">Look until this address</param>
        /// <returns>Status</returns>
        internal Status ContainsKeyInMemory(ref Key key, long fromAddress = -1)
        {
            return fht.InternalContainsKeyInMemory(ref key, ctx, FasterSession, fromAddress);
        }

        /// <summary>
        /// Get list of pending requests (for current session)
        /// </summary>
        /// <returns></returns>
        public IEnumerable<long> GetPendingRequests()
        {
            foreach (var kvp in ctx.prevCtx?.ioPendingRequests)
                yield return kvp.Value.serialNum;

            foreach (var val in ctx.prevCtx?.retryRequests)
                yield return val.serialNum;

            foreach (var kvp in ctx.ioPendingRequests)
                yield return kvp.Value.serialNum;

            foreach (var val in ctx.retryRequests)
                yield return val.serialNum;
        }

        /// <summary>
        /// Refresh session epoch and handle checkpointing phases. Used only
        /// in case of thread-affinitized sessions (async support is disabled).
        /// </summary>
        public void Refresh()
        {
            if (SupportAsync) UnsafeResumeThread();
            fht.InternalRefresh(ctx, FasterSession);
            if (SupportAsync) UnsafeSuspendThread();
        }

        /// <summary>
        /// Sync complete all outstanding pending operations
        /// Async operations (ReadAsync) must be completed individually
        /// </summary>
        /// <param name="spinWait">Spin-wait for all pending operations on session to complete</param>
        /// <param name="spinWaitForCommit">Extend spin-wait until ongoing commit/checkpoint, if any, completes</param>
        /// <returns></returns>
        public bool CompletePending(bool spinWait = false, bool spinWaitForCommit = false)
        {
            if (SupportAsync) UnsafeResumeThread();
            try
            {
                var result = fht.InternalCompletePending(ctx, FasterSession, spinWait);
                if (spinWaitForCommit)
                {
                    if (spinWait != true)
                    {
                        throw new FasterException("Can spin-wait for checkpoint completion only if spinWait is true");
                    }
                    do
                    {
                        fht.InternalCompletePending(ctx, FasterSession, spinWait);
                        if (fht.InRestPhase())
                        {
                            fht.InternalCompletePending(ctx, FasterSession, spinWait);
                            return true;
                        }
                    } while (spinWait);
                }
                return result;
            }
            finally
            {
                if (SupportAsync) UnsafeSuspendThread();
            }
        }

        /// <summary>
        /// Complete all outstanding pending operations asynchronously
        /// Async operations (ReadAsync) must be completed individually
        /// </summary>
        /// <returns></returns>
        public async ValueTask CompletePendingAsync(bool waitForCommit = false, CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();

            if (fht.epoch.ThisInstanceProtected())
                throw new NotSupportedException("Async operations not supported over protected epoch");

            // Complete all pending operations on session
            await fht.CompletePendingAsync(this, token);

            // Wait for commit if necessary
            if (waitForCommit)
                await WaitForCommitAsync(token);
        }

        /// <summary>
        /// Check if at least one request is ready for CompletePending to be called on
        /// Returns completed immediately if there are no outstanding requests
        /// </summary>
        /// <param name="token"></param>
        /// <returns></returns>
        public async ValueTask ReadyToCompletePendingAsync(CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();

            if (fht.epoch.ThisInstanceProtected())
                throw new NotSupportedException("Async operations not supported over protected epoch");

            await fht.ReadyToCompletePendingAsync(this, token);
        }

        /// <summary>
        /// Wait for commit of all operations completed until the current point in session.
        /// Does not itself issue checkpoint/commits.
        /// </summary>
        /// <returns></returns>
        public async ValueTask WaitForCommitAsync(CancellationToken token = default)
        {
            token.ThrowIfCancellationRequested();

            // Complete all pending operations on session
            await CompletePendingAsync();

            var task = fht.CheckpointTask;
            CommitPoint localCommitPoint = LatestCommitPoint;
            if (localCommitPoint.UntilSerialNo >= ctx.serialNum && localCommitPoint.ExcludedSerialNos?.Count == 0)
                return;

            while (true)
            {
                await task.WithCancellationAsync(token);
                Refresh();

                task = fht.CheckpointTask;
                localCommitPoint = LatestCommitPoint;
                if (localCommitPoint.UntilSerialNo >= ctx.serialNum && localCommitPoint.ExcludedSerialNos?.Count == 0)
                    break;
            }
        }

        /// <summary>
        /// Resume session on current thread
        /// Call SuspendThread before any async op
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void UnsafeResumeThread()
        {
            fht.epoch.Resume();
            fht.InternalRefresh(ctx, FasterSession);
        }

        /// <summary>
        /// Suspend session on current thread
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void UnsafeSuspendThread()
        {
            fht.epoch.Suspend();
        }

        void IClientSession.AtomicSwitch(int version)
        {
            fht.AtomicSwitch(ctx, ctx.prevCtx, version, fht._hybridLogCheckpoint.info.checkpointTokens);
        }

        // This is a struct to allow JIT to inline calls (and bypass default interface call mechanism)
        internal struct AsyncFasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            private readonly ClientSession<Key, Value, Input, Output, Context, Functions> _clientSession;

            public AsyncFasterSession(ClientSession<Key, Value, Input, Output, Context, Functions> clientSession)
            {
                _clientSession = clientSession;
            }

            public void CheckpointCompletionCallback(string guid, CommitPoint commitPoint)
            {
                _clientSession.functions.CheckpointCompletionCallback(guid, commitPoint);
                _clientSession.LatestCommitPoint = commitPoint;
            }

            public void ConcurrentReader(ref Key key, ref Input input, ref Value value, ref Output dst)
            {
                _clientSession.functions.ConcurrentReader(ref key, ref input, ref value, ref dst);
            }

            public bool ConcurrentWriter(ref Key key, ref Value src, ref Value dst)
            {
                return _clientSession.functions.ConcurrentWriter(ref key, ref src, ref dst);
            }

            public bool NeedCopyUpdate(ref Key key, ref Input input, ref Value oldValue)
                => _clientSession.functions.NeedCopyUpdate(ref key, ref input, ref oldValue);

            public void CopyUpdater(ref Key key, ref Input input, ref Value oldValue, ref Value newValue)
            {
                _clientSession.functions.CopyUpdater(ref key, ref input, ref oldValue, ref newValue);
            }

            public void DeleteCompletionCallback(ref Key key, Context ctx)
            {
                _clientSession.functions.DeleteCompletionCallback(ref key, ctx);
            }

            public int GetInitialLength(ref Input input)
            {
                return _clientSession.variableLengthStruct.GetInitialLength(ref input);
            }

            public int GetLength(ref Value t, ref Input input)
            {
                return _clientSession.variableLengthStruct.GetLength(ref t, ref input);
            }

            public void InitialUpdater(ref Key key, ref Input input, ref Value value)
            {
                _clientSession.functions.InitialUpdater(ref key, ref input, ref value);
            }

            public bool InPlaceUpdater(ref Key key, ref Input input, ref Value value)
            {
                return _clientSession.functions.InPlaceUpdater(ref key, ref input, ref value);
            }

            public void ReadCompletionCallback(ref Key key, ref Input input, ref Output output, Context ctx, Status status, RecordInfo recordInfo)
            {
                _clientSession.functions.ReadCompletionCallback(ref key, ref input, ref output, ctx, status, recordInfo);
            }

            public void RMWCompletionCallback(ref Key key, ref Input input, Context ctx, Status status)
            {
                _clientSession.functions.RMWCompletionCallback(ref key, ref input, ctx, status);
            }

            public void SingleReader(ref Key key, ref Input input, ref Value value, ref Output dst)
            {
                _clientSession.functions.SingleReader(ref key, ref input, ref value, ref dst);
            }

            public void SingleWriter(ref Key key, ref Value src, ref Value dst)
            {
                _clientSession.functions.SingleWriter(ref key, ref src, ref dst);
            }

            public void UnsafeResumeThread()
            {
                _clientSession.UnsafeResumeThread();
            }

            public void UnsafeSuspendThread()
            {
                _clientSession.UnsafeSuspendThread();
            }

            public void UpsertCompletionCallback(ref Key key, ref Value value, Context ctx)
            {
                _clientSession.functions.UpsertCompletionCallback(ref key, ref value, ctx);
            }

            public IHeapContainer<Input> GetHeapContainer(ref Input input)
            {
                if (_clientSession.inputVariableLengthStruct == default)
                    return new StandardHeapContainer<Input>(ref input);

                return new VarLenHeapContainer<Input>(ref input, _clientSession.inputVariableLengthStruct, _clientSession.fht.hlog.bufferPool);
            }
        }
    }
}
