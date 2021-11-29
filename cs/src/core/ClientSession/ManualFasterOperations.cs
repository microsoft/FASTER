// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace FASTER.core
{
    /// <summary>
    /// Faster Operations implementation that allows manual control of record locking and epoch management. For advanced use only.
    /// </summary>
    public sealed class ManualFasterOperations<Key, Value, Input, Output, Context, Functions> : IFasterOperations<Key, Value, Input, Output, Context>, IDisposable
        where Functions : IFunctions<Key, Value, Input, Output, Context>
    {
        readonly ClientSession<Key, Value, Input, Output, Context, Functions> clientSession;

        internal readonly InternalFasterSession FasterSession;
        bool isAcquired;

        ulong TotalLockCount => sharedLockCount + exclusiveLockCount;
        internal ulong sharedLockCount;
        internal ulong exclusiveLockCount;

        void CheckAcquired()
        {
            if (!isAcquired)
                throw new FasterException("Method call on not-acquired ManualFasterOperations");
        }

        internal ManualFasterOperations(ClientSession<Key, Value, Input, Output, Context, Functions> clientSession)
        {
            this.clientSession = clientSession;
            FasterSession = new InternalFasterSession(clientSession);
        }

        /// <summary>
        /// Resume session on current thread. IMPORTANT: Call SuspendThread before any async op.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void UnsafeResumeThread() => clientSession.UnsafeResumeThread();

        /// <summary>
        /// Resume session on current thread. IMPORTANT: Call SuspendThread before any async op.
        /// </summary>
        /// <param name="resumeEpoch">Epoch that session resumes on; can be saved to see if epoch has changed</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void UnsafeResumeThread(out int resumeEpoch) => clientSession.UnsafeResumeThread(out resumeEpoch);

        /// <summary>
        /// Suspend session on current thread
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void UnsafeSuspendThread() => clientSession.UnsafeSuspendThread();

        /// <summary>
        /// Synchronously complete outstanding pending synchronous operations.
        /// Async operations must be completed individually.
        /// </summary>
        /// <param name="wait">Wait for all pending operations on session to complete</param>
        /// <param name="spinWaitForCommit">Spin-wait until ongoing commit/checkpoint, if any, completes</param>
        /// <returns>True if all pending operations have completed, false otherwise</returns>
        public bool UnsafeCompletePending(bool wait = false, bool spinWaitForCommit = false)
            => this.clientSession.UnsafeCompletePending(this.FasterSession, false, wait, spinWaitForCommit);

        /// <summary>
        /// Synchronously complete outstanding pending synchronous operations, returning outputs for the completed operations.
        /// Assumes epoch protection is managed by user. Async operations must be completed individually.
        /// </summary>
        /// <param name="completedOutputs">Outputs completed by this operation</param>
        /// <param name="wait">Wait for all pending operations on session to complete</param>
        /// <param name="spinWaitForCommit">Spin-wait until ongoing commit/checkpoint, if any, completes</param>
        /// <returns>True if all pending operations have completed, false otherwise</returns>
        public bool UnsafeCompletePendingWithOutputs(out CompletedOutputIterator<Key, Value, Input, Output, Context> completedOutputs, bool wait = false, bool spinWaitForCommit = false)
            => this.clientSession.UnsafeCompletePendingWithOutputs(this.FasterSession, out completedOutputs, wait, spinWaitForCommit);

        #region Acquire and Dispose
        internal void Acquire()
        {
            if (this.isAcquired)
                throw new FasterException("Trying to acquire an already-acquired ManualFasterOperations");
            this.isAcquired = true;
        }

        /// <summary>
        /// Does not actually dispose of anything; asserts the epoch has been suspended
        /// </summary>
        public void Dispose()
        {
            if (LightEpoch.AnyInstanceProtected())
                throw new FasterException("Disposing ManualFasterOperations with a protected epoch; must call UnsafeSuspendThread");
            if (TotalLockCount > 0)
                throw new FasterException($"Disposing ManualFasterOperations with locks held: {sharedLockCount} shared locks, {exclusiveLockCount} exclusive locks");
        }
        #endregion Acquire and Dispose

        #region Key Locking

        /// <summary>
        /// Lock the key with the specified <paramref name="lockType"/>, waiting until it is acquired
        /// </summary>
        /// <param name="key">The key to lock</param>
        /// <param name="lockType">The type of lock to take</param>
        /// <param name="retrieveData">Whether to retrieve data (and copy to the tail of the log) if the key is not in the mutable region</param>
        /// <param name="lockInfo">Information about the acquired lock</param>
        public unsafe void Lock(ref Key key, LockType lockType, bool retrieveData, ref LockInfo lockInfo)
        {
            CheckAcquired();
            LockOperation lockOp = new(LockOperationType.LockRead, lockType);

            Input input = default;
            Output output = default;
            RecordMetadata recordMetadata = default;

            // Note: this does not use RMW because that would complicate the RMW process:
            //  - InternalRMW would have to know whether we are doing retrieveData
            //  - this.CopyUpdater would have to call SingleWriter to simply copy the data over unchanged
            // The assumption is that if retrieveData is true, there is an expectation the key already exists, so only ContextRead would be called.

            bool success = false;
            if (retrieveData)
            {
                var status = clientSession.fht.ContextRead(ref key, ref input, ref output, ref lockOp, ref recordMetadata, ReadFlags.CopyToTail, context: default, FasterSession, serialNo: 0, clientSession.ctx);
                success = status == Status.OK;
                if (status == Status.PENDING)
                {
                    // This bottoms out in WaitPending which assumes the epoch is protected, and releases it. So we don't release it here.
                    this.UnsafeCompletePendingWithOutputs(out var completedOutputs, wait: true);
                    completedOutputs.Next();
                    recordMetadata = completedOutputs.Current.RecordMetadata;
                    completedOutputs.Dispose();
                    success = true;
                }
            }

            if (!success)
            {
                lockOp.LockOperationType = LockOperationType.LockUpsert;
                Value value = default;
                var status = clientSession.fht.ContextUpsert(ref key, ref input, ref value, ref output, ref lockOp, out recordMetadata, context: default, FasterSession, serialNo: 0, clientSession.ctx);
                Debug.Assert(status == Status.OK);
            }

            lockInfo.LockType = lockType == LockType.ExclusiveFromShared ? LockType.Exclusive : lockType;
            lockInfo.Address = recordMetadata.Address;
            if (lockInfo.LockType == LockType.Exclusive)
                ++this.exclusiveLockCount;
            else
                ++this.sharedLockCount;
        }

        /// <summary>
        /// Lock the key with the specified <paramref name="lockType"/>, waiting until it is acquired
        /// </summary>
        /// <param name="key">The key to lock</param>
        /// <param name="lockType">The type of lock to take</param>
        /// <param name="retrieveData">Whether to retrieve data (and copy to the tail of the log) if the key is not in the mutable region</param>
        /// <param name="lockInfo">Information about the acquired lock</param>
        public unsafe void Lock(Key key, LockType lockType, bool retrieveData, ref LockInfo lockInfo) 
            => Lock(ref key, lockType, retrieveData, ref lockInfo);

        /// <summary>
        /// Lock the key with the specified <paramref name="lockType"/>, waiting until it is acquired
        /// </summary>
        /// <param name="key">The key to lock</param>
        /// <param name="lockType">The type of lock to take</param>
        /// <param name="retrieveData">Whether to retrieve data (and copy to the tail of the log) if the key is not in the mutable region</param>
        public unsafe void Lock(ref Key key, LockType lockType, bool retrieveData)
        {
            LockInfo lockInfo = default;
            Lock(ref key, lockType, retrieveData, ref lockInfo);
        }

        /// <summary>
        /// Lock the key with the specified <paramref name="lockType"/>, waiting until it is acquired
        /// </summary>
        /// <param name="key">The key to lock</param>
        /// <param name="lockType">The type of lock to take</param>
        /// <param name="retrieveData">Whether to retrieve data (and copy to the tail of the log) if the key is not in the mutable region</param>
        public unsafe void Lock(Key key, LockType lockType, bool retrieveData)
        {
            LockInfo lockInfo = default;
            Lock(ref key, lockType, retrieveData, ref lockInfo);
        }

        /// <summary>
        /// Lock the key with the specified <paramref name="lockType"/>
        /// </summary>
        /// <param name="key">The key to lock</param>
        /// <param name="lockType">The type of lock to take</param>
        /// <param name="lockInfo">Information about the acquired lock</param>
        public void Unlock(ref Key key, LockType lockType, ref LockInfo lockInfo)
        {
            CheckAcquired();
            LockOperation lockOp = new(LockOperationType.Unlock, lockType);

            Input input = default;
            Output output = default;
            RecordMetadata recordMetadata = default;

            var status = clientSession.fht.ContextRead(ref key, ref input, ref output, ref lockOp, ref recordMetadata, ReadFlags.None, context: default, FasterSession, serialNo: 0, clientSession.ctx);
            if (status == Status.PENDING)
            {
                // Do nothing here, as a lock that goes into the on-disk region is considered unlocked--we will not allow that anyway.
                // This bottoms out in WaitPending which assumes the epoch is protected, and releases it. So we don't release it here.
                this.UnsafeCompletePending(wait: true);
            }

            if (lockInfo.LockType == LockType.Exclusive)
                --this.exclusiveLockCount;
            else
                --this.sharedLockCount;
        }

        /// <summary>
        /// Lock the key with the specified <paramref name="lockInfo"/> lock type.
        /// </summary>
        /// <param name="key">The key to lock</param>
        /// <param name="lockInfo">Information about the acquired lock</param>
        public void Unlock(Key key, ref LockInfo lockInfo)
            => Unlock(ref key, lockInfo.LockType, ref lockInfo);

        /// <summary>
        /// Lock the key with the specified <paramref name="lockType"/>
        /// </summary>
        /// <param name="key">The key to lock</param>
        /// <param name="lockType">The type of lock to take</param>
        public void Unlock(Key key, LockType lockType)
        {
            LockInfo lockInfo = default;
            Unlock(ref key, lockType, ref lockInfo);
        }

        #endregion Key Locking

        #region IFasterOperations

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read(ref Key key, ref Input input, ref Output output, Context userContext = default, long serialNo = 0)
        {
            Debug.Assert(clientSession.fht.epoch.ThisInstanceProtected());
            return clientSession.fht.ContextRead(ref key, ref input, ref output, userContext, FasterSession, serialNo, clientSession.ctx);
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
        public Status Read(ref Key key, ref Input input, ref Output output, ref RecordMetadata recordMetadata, ReadFlags readFlags = ReadFlags.None, Context userContext = default, long serialNo = 0)
        {
            Debug.Assert(clientSession.fht.epoch.ThisInstanceProtected());
            LockOperation lockOp = default;
            return clientSession.fht.ContextRead(ref key, ref input, ref output, ref lockOp, ref recordMetadata, readFlags, userContext, FasterSession, serialNo, clientSession.ctx);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status ReadAtAddress(long address, ref Input input, ref Output output, ReadFlags readFlags = ReadFlags.None, Context userContext = default, long serialNo = 0)
        {
            Debug.Assert(clientSession.fht.epoch.ThisInstanceProtected());
            return clientSession.fht.ContextReadAtAddress(address, ref input, ref output, readFlags, userContext, FasterSession, serialNo, clientSession.ctx);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<FasterKV<Key, Value>.ReadAsyncResult<Input, Output, Context>> ReadAsync(ref Key key, ref Input input, Context userContext = default, long serialNo = 0, CancellationToken cancellationToken = default)
        {
            Debug.Assert(!LightEpoch.AnyInstanceProtected());
            return clientSession.fht.ReadAsync(FasterSession, clientSession.ctx, ref key, ref input, Constants.kInvalidAddress, userContext, serialNo, cancellationToken);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<FasterKV<Key, Value>.ReadAsyncResult<Input, Output, Context>> ReadAsync(Key key, Input input, Context context = default, long serialNo = 0, CancellationToken token = default)
        {
            Debug.Assert(!LightEpoch.AnyInstanceProtected());
            return clientSession.fht.ReadAsync(FasterSession, clientSession.ctx, ref key, ref input, Constants.kInvalidAddress, context, serialNo, token);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<FasterKV<Key, Value>.ReadAsyncResult<Input, Output, Context>> ReadAsync(ref Key key, Context userContext = default, long serialNo = 0, CancellationToken token = default)
        {
            Debug.Assert(!LightEpoch.AnyInstanceProtected());
            Input input = default;
            return clientSession.fht.ReadAsync(FasterSession, clientSession.ctx, ref key, ref input, Constants.kInvalidAddress, userContext, serialNo, token);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<FasterKV<Key, Value>.ReadAsyncResult<Input, Output, Context>> ReadAsync(Key key, Context context = default, long serialNo = 0, CancellationToken token = default)
        {
            Debug.Assert(!LightEpoch.AnyInstanceProtected());
            Input input = default;
            return clientSession.fht.ReadAsync(FasterSession, clientSession.ctx, ref key, ref input, Constants.kInvalidAddress, context, serialNo, token);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<FasterKV<Key, Value>.ReadAsyncResult<Input, Output, Context>> ReadAsync(ref Key key, ref Input input, long startAddress, ReadFlags readFlags = ReadFlags.None,
                                                                                                 Context userContext = default, long serialNo = 0, CancellationToken cancellationToken = default)
        {
            Debug.Assert(!LightEpoch.AnyInstanceProtected());
            var operationFlags = FasterKV<Key, Value>.PendingContext<Input, Output, Context>.GetOperationFlags(readFlags);
            return clientSession.fht.ReadAsync(FasterSession, clientSession.ctx, ref key, ref input, startAddress, userContext, serialNo, cancellationToken, operationFlags);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<FasterKV<Key, Value>.ReadAsyncResult<Input, Output, Context>> ReadAtAddressAsync(long address, ref Input input, ReadFlags readFlags = ReadFlags.None,
                                                                                                          Context userContext = default, long serialNo = 0, CancellationToken cancellationToken = default)
        {
            Debug.Assert(!LightEpoch.AnyInstanceProtected());
            Key key = default;
            var operationFlags = FasterKV<Key, Value>.PendingContext<Input, Output, Context>.GetOperationFlags(readFlags, noKey: true);
            return clientSession.fht.ReadAsync(FasterSession, clientSession.ctx, ref key, ref input, address, userContext, serialNo, cancellationToken, operationFlags);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(ref Key key, ref Value desiredValue, Context userContext = default, long serialNo = 0)
        {
            Debug.Assert(clientSession.fht.epoch.ThisInstanceProtected());
            Input input = default;
            Output output = default;
            return Upsert(ref key, ref input, ref desiredValue, ref output, out _, userContext, serialNo);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(ref Key key, ref Input input, ref Value desiredValue, ref Output output, Context userContext = default, long serialNo = 0)
        {
            Debug.Assert(clientSession.fht.epoch.ThisInstanceProtected());
            return clientSession.fht.ContextUpsert(ref key, ref input, ref desiredValue, ref output, userContext, FasterSession, serialNo, clientSession.ctx);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(ref Key key, ref Input input, ref Value desiredValue, ref Output output, out RecordMetadata recordMetadata, Context userContext = default, long serialNo = 0)
        {
            Debug.Assert(clientSession.fht.epoch.ThisInstanceProtected());
            LockOperation lockOp = default;
            return clientSession.fht.ContextUpsert(ref key, ref input, ref desiredValue, ref output, ref lockOp, out recordMetadata, userContext, FasterSession, serialNo, clientSession.ctx);
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
        {
            Debug.Assert(!LightEpoch.AnyInstanceProtected());
            return clientSession.fht.UpsertAsync(FasterSession, clientSession.ctx, ref key, ref input, ref desiredValue, userContext, serialNo, token);
        }

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
            Debug.Assert(clientSession.fht.epoch.ThisInstanceProtected());
            return clientSession.fht.ContextRMW(ref key, ref input, ref output, out recordMetadata, userContext, FasterSession, serialNo, clientSession.ctx);
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
        {
            Debug.Assert(!LightEpoch.AnyInstanceProtected());
            return clientSession.fht.RmwAsync(FasterSession, clientSession.ctx, ref key, ref input, context, serialNo, token);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<FasterKV<Key, Value>.RmwAsyncResult<Input, Output, Context>> RMWAsync(Key key, Input input, Context context = default, long serialNo = 0, CancellationToken token = default)
            => RMWAsync(ref key, ref input, context, serialNo, token);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Delete(ref Key key, Context userContext = default, long serialNo = 0)
        {
            Debug.Assert(clientSession.fht.epoch.ThisInstanceProtected());
            return clientSession.fht.ContextDelete(ref key, userContext, FasterSession, serialNo, clientSession.ctx);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Delete(Key key, Context userContext = default, long serialNo = 0)
            => Delete(ref key, userContext, serialNo);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<FasterKV<Key, Value>.DeleteAsyncResult<Input, Output, Context>> DeleteAsync(ref Key key, Context userContext = default, long serialNo = 0, CancellationToken token = default)
        {
            Debug.Assert(!LightEpoch.AnyInstanceProtected());
            return clientSession.fht.DeleteAsync(FasterSession, clientSession.ctx, ref key, userContext, serialNo, token);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<FasterKV<Key, Value>.DeleteAsyncResult<Input, Output, Context>> DeleteAsync(Key key, Context userContext = default, long serialNo = 0, CancellationToken token = default)
            => DeleteAsync(ref key, userContext, serialNo, token);

        /// <inheritdoc/>
        public void Refresh()
        {
            Debug.Assert(!LightEpoch.AnyInstanceProtected());
            clientSession.fht.InternalRefresh(clientSession.ctx, FasterSession);
        }

        #endregion IFasterOperations

        #region IFasterSession

        // This is a struct to allow JIT to inline calls (and bypass default interface call mechanism)
        internal readonly struct InternalFasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            private readonly ClientSession<Key, Value, Input, Output, Context, Functions> _clientSession;

            public InternalFasterSession(ClientSession<Key, Value, Input, Output, Context, Functions> clientSession)
            {
                _clientSession = clientSession;
            }

            #region IFunctions - Optional features supported
            public bool SupportsLocking => false;       // We only lock explicitly in Lock/Unlock, which are longer-duration locks.

            public bool SupportsPostOperations => true; // We need this for user record locking, but check for user's setting before calling user code

            public bool IsManualOperations => true;
            #endregion IFunctions - Optional features supported

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            void HandleLockOperation(ref RecordInfo recordInfo, ref LockOperation lockOp, out bool isLock)
            {
                isLock = false;
                if (lockOp.LockOperationType == LockOperationType.Unlock)
                {
                    if (recordInfo.Stub)
                    {
                        recordInfo.Stub = false;
                        recordInfo.SetInvalid();
                    }
                    if (lockOp.LockType == LockType.Shared)
                        this.UnlockShared(ref recordInfo);
                    else if (lockOp.LockType == LockType.Exclusive)
                        this.UnlockExclusive(ref recordInfo);
                    else
                        Debug.Fail($"Unexpected LockType: {lockOp.LockType}");
                    return;
                }
                isLock = true;
                if (lockOp.LockType == LockType.Shared)
                    this.LockShared(ref recordInfo);
                else if (lockOp.LockType == LockType.Exclusive)
                    this.LockExclusive(ref recordInfo);
                else if (lockOp.LockType == LockType.ExclusiveFromShared)
                    this.LockExclusiveFromShared(ref recordInfo);
                else
                    Debug.Fail($"Unexpected LockType: {lockOp.LockType}");
            }

            #region IFunctions - Reads
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool SingleReader(ref Key key, ref Input input, ref Value value, ref Output dst, ref LockOperation lockOp, ref RecordInfo recordInfo, long address)
            {
                if (lockOp.IsSet)
                {
                    // No value is returned to the client through the lock sequence; for consistency all key locks must be acquired before their values are read.
                    HandleLockOperation(ref recordInfo, ref lockOp, out _);
                    return true;
                }
                return _clientSession.functions.SingleReader(ref key, ref input, ref value, ref dst, ref recordInfo, address);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool ConcurrentReader(ref Key key, ref Input input, ref Value value, ref Output dst, ref LockOperation lockOp, ref RecordInfo recordInfo, long address)
            {
                if (lockOp.IsSet)
                {
                    // No value is returned to the client through the lock sequence; for consistency all key locks must be acquired before their values are read.
                    HandleLockOperation(ref recordInfo, ref lockOp, out _);
                    return true;
                }
                return _clientSession.functions.ConcurrentReader(ref key, ref input, ref value, ref dst, ref recordInfo, address);
            }

            public void ReadCompletionCallback(ref Key key, ref Input input, ref Output output, Context ctx, Status status, RecordMetadata recordMetadata)
                => _clientSession.functions.ReadCompletionCallback(ref key, ref input, ref output, ctx, status, recordMetadata);

            #endregion IFunctions - Reads

            // Our general locking rule in this "session" is: we don't lock unless explicitly requested via lockOp.IsSet. If it is requested, then as with
            // ClientSession, except for readcache usage of SingleWriter, all operations that append a record must lock in the <Operation>() call. Unlike
            // ClientSession, we do *not* unlock in the Post<Operation> call; instead we wait for explicit client user unlock.

            #region IFunctions - Upserts
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void SingleWriter(ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref LockOperation lockOp, ref RecordInfo recordInfo, long address)
            {
                _clientSession.functions.SingleWriter(ref key, ref input, ref src, ref dst, ref output, ref recordInfo, address);

                // Lock here, and do not unlock in PostSingleWriter; wait for the user to explicitly unlock
                if (lockOp.IsSet)
                {
                    Debug.Assert(lockOp.LockOperationType != LockOperationType.Unlock); // Should have caught this in InternalUpsert
                    HandleLockOperation(ref recordInfo, ref lockOp, out _);

                    // If this is a lock for upsert, then we've failed to find an in-memory record for this key, and we're creating a stub with a default value.
                    if (lockOp.LockOperationType == LockOperationType.LockUpsert)
                        recordInfo.Stub = true;
                }
                else if (lockOp.IsStubPromotion)
                {
                    this.LockExclusive(ref recordInfo);
                }
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void PostSingleWriter(ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref LockOperation lockOp, ref RecordInfo recordInfo, long address)
            {
                if (_clientSession.functions.SupportsPostOperations)
                    _clientSession.functions.PostSingleWriter(ref key, ref input, ref src, ref dst, ref output, ref recordInfo, address);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool ConcurrentWriter(ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref LockOperation lockOp, ref RecordInfo recordInfo, long address)
            {
                if (lockOp.IsSet)
                {
                    // All lock operations in ConcurrentWriter can return immediately.
                    HandleLockOperation(ref recordInfo, ref lockOp, out _);
                    return true;
                }

                // Note: KeyIndexes do not need notification of in-place updates because the key does not change.
                return _clientSession.functions.ConcurrentWriter(ref key, ref input, ref src, ref dst, ref output, ref recordInfo, address);
            }

            public void UpsertCompletionCallback(ref Key key, ref Input input, ref Value value, Context ctx)
                => _clientSession.functions.UpsertCompletionCallback(ref key, ref input, ref value, ctx);
#endregion IFunctions - Upserts

            #region IFunctions - RMWs
            #region InitialUpdater
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool NeedInitialUpdate(ref Key key, ref Input input, ref Output output)
                => _clientSession.functions.NeedInitialUpdate(ref key, ref input, ref output);

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void InitialUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref LockOperation lockOp, ref RecordInfo recordInfo, long address)
            {
                _clientSession.functions.InitialUpdater(ref key, ref input, ref value, ref output, ref recordInfo, address);
                if (lockOp.IsStubPromotion)
                {
                    this.LockExclusive(ref recordInfo);
                }
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void PostInitialUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref RecordInfo recordInfo, long address)
            {
                if (_clientSession.functions.SupportsPostOperations)
                    _clientSession.functions.PostInitialUpdater(ref key, ref input, ref value, ref output, ref recordInfo, address);
            }
            #endregion InitialUpdater

            #region CopyUpdater
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool NeedCopyUpdate(ref Key key, ref Input input, ref Value oldValue, ref Output output)
                => _clientSession.functions.NeedCopyUpdate(ref key, ref input, ref oldValue, ref output);

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void CopyUpdater(ref Key key, ref Input input, ref Value oldValue, ref Value newValue, ref Output output, ref RecordInfo recordInfo, long address) 
                => _clientSession.functions.CopyUpdater(ref key, ref input, ref oldValue, ref newValue, ref output, ref recordInfo, address);

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool PostCopyUpdater(ref Key key, ref Input input, ref Value oldValue, ref Value newValue, ref Output output, ref RecordInfo recordInfo, long address)
            {
                return !_clientSession.functions.SupportsPostOperations
                    || _clientSession.functions.PostCopyUpdater(ref key, ref input, ref oldValue, ref newValue, ref output, ref recordInfo, address);
            }
            #endregion CopyUpdater

            #region InPlaceUpdater
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool InPlaceUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref RecordInfo recordInfo, long address)
            {
                // Note: KeyIndexes do not need notification of in-place updates because the key does not change.
                return _clientSession.functions.InPlaceUpdater(ref key, ref input, ref value, ref output, ref recordInfo, address);
            }

            public void RMWCompletionCallback(ref Key key, ref Input input, ref Output output, Context ctx, Status status, RecordMetadata recordMetadata)
                => _clientSession.functions.RMWCompletionCallback(ref key, ref input, ref output, ctx, status, recordMetadata);

            #endregion InPlaceUpdater
            #endregion IFunctions - RMWs

            #region IFunctions - Deletes
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void PostSingleDeleter(ref Key key, ref RecordInfo recordInfo, long address)
            {
                if (_clientSession.functions.SupportsPostOperations)
                    _clientSession.functions.PostSingleDeleter(ref key, ref recordInfo, address);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool ConcurrentDeleter(ref Key key, ref Value value, ref RecordInfo recordInfo, long address)
            {
                recordInfo.Tombstone = true;
                return _clientSession.functions.ConcurrentDeleter(ref key, ref value, ref recordInfo, address);
            }

            public void DeleteCompletionCallback(ref Key key, Context ctx)
                => _clientSession.functions.DeleteCompletionCallback(ref key, ctx);
            #endregion IFunctions - Deletes

            #region IFunctions - Locking

            public void LockExclusive(ref RecordInfo recordInfo) => recordInfo.LockExclusive();

            public void UnlockExclusive(ref RecordInfo recordInfo) => recordInfo.UnlockExclusive();

            public bool TryLockExclusive(ref RecordInfo recordInfo, int spinCount = 1) => recordInfo.TryLockExclusive(spinCount);

            public void LockShared(ref RecordInfo recordInfo) => recordInfo.LockShared();

            public void UnlockShared(ref RecordInfo recordInfo) => recordInfo.UnlockShared();

            public bool TryLockShared(ref RecordInfo recordInfo, int spinCount = 1) => recordInfo.TryLockShared(spinCount);

            public void LockExclusiveFromShared(ref RecordInfo recordInfo) => recordInfo.LockExclusiveFromShared();

            public bool TryLockExclusiveFromShared(ref RecordInfo recordInfo, int spinCount = 1) => recordInfo.TryLockExclusiveFromShared(spinCount);

            public bool IsLocked(ref RecordInfo recordInfo) => recordInfo.IsLocked;

            public bool IsLockedExclusive(ref RecordInfo recordInfo) => recordInfo.IsLockedExclusive;

            public bool IsLockedShared(ref RecordInfo recordInfo) => recordInfo.IsLockedShared;

            public void TransferLocks(ref RecordInfo oldRecordInfo, ref RecordInfo newRecordInfo) => newRecordInfo.TransferLocksFrom(ref oldRecordInfo);
            #endregion IFunctions - Locking

            #region IFunctions - Checkpointing
            public void CheckpointCompletionCallback(string guid, CommitPoint commitPoint)
            {
                _clientSession.functions.CheckpointCompletionCallback(guid, commitPoint);
                _clientSession.LatestCommitPoint = commitPoint;
            }
#endregion IFunctions - Checkpointing

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
            #endregion Internal utilities
        }
#endregion IFasterSession
    }
}
