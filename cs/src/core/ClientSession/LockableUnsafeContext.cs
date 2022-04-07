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
    /// Faster Context implementation that allows manual control of record locking and epoch management. For advanced use only.
    /// </summary>
    public sealed class LockableUnsafeContext<Key, Value, Input, Output, Context, Functions, StoreFunctions> : IFasterContext<Key, Value, Input, Output, Context, StoreFunctions>, IDisposable
        where Functions : IFunctions<Key, Value, Input, Output, Context>
    {
        readonly ClientSession<Key, Value, Input, Output, Context, Functions, StoreFunctions> clientSession;

        internal readonly InternalFasterSession FasterSession;
        bool isAcquired;

        ulong TotalLockCount => sharedLockCount + exclusiveLockCount;
        internal ulong sharedLockCount;
        internal ulong exclusiveLockCount;

        void CheckAcquired()
        {
            if (!isAcquired)
                throw new FasterException("Method call on not-acquired LockableUnsafeContext");
        }

        internal LockableUnsafeContext(ClientSession<Key, Value, Input, Output, Context, Functions, StoreFunctions> clientSession)
        {
            this.clientSession = clientSession;
            FasterSession = new InternalFasterSession(clientSession);
        }

        /// <summary>
        /// Resume session on current thread. IMPORTANT: Call SuspendThread before any async op.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ResumeThread()
        {
            CheckAcquired();
            clientSession.UnsafeResumeThread();
        }

        /// <summary>
        /// Resume session on current thread. IMPORTANT: Call SuspendThread before any async op.
        /// </summary>
        /// <param name="resumeEpoch">Epoch that the session resumed on; can be saved to see if epoch has changed</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ResumeThread(out int resumeEpoch)
        {
            CheckAcquired();
            clientSession.UnsafeResumeThread(out resumeEpoch);
        }

        /// <summary>
        /// Suspend session on current thread
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SuspendThread()
        {
            Debug.Assert(clientSession.fht.epoch.ThisInstanceProtected());
            clientSession.UnsafeSuspendThread();
        }

        /// <summary>
        /// Current epoch of the session
        /// </summary>
        public int LocalCurrentEpoch => clientSession.fht.epoch.LocalCurrentEpoch;

        /// <inheritdoc/>
        public bool CompletePending(bool wait = false, bool spinWaitForCommit = false)
        {
            Debug.Assert(clientSession.fht.epoch.ThisInstanceProtected());
            return this.clientSession.UnsafeCompletePending(this.FasterSession, false, wait, spinWaitForCommit);
        }

        /// <inheritdoc/>
        public bool CompletePendingWithOutputs(out CompletedOutputIterator<Key, Value, Input, Output, Context> completedOutputs, bool wait = false, bool spinWaitForCommit = false)
        {
            Debug.Assert(clientSession.fht.epoch.ThisInstanceProtected());
            return this.clientSession.UnsafeCompletePendingWithOutputs(this.FasterSession, out completedOutputs, wait, spinWaitForCommit);
        }

        #region Acquire and Dispose
        internal void Acquire()
        {
            this.clientSession.fht.IncrementNumLockingSessions();
            if (this.isAcquired)
                throw new FasterException("Trying to acquire an already-acquired LockableUnsafeContext");
            this.isAcquired = true;
        }

        /// <summary>
        /// Does not actually dispose of anything; asserts the epoch has been suspended
        /// </summary>
        public void Dispose()
        {
            if (clientSession.fht.epoch.ThisInstanceProtected())
                throw new FasterException("Disposing LockableUnsafeContext with a protected epoch; must call UnsafeSuspendThread");
            if (TotalLockCount > 0)
                throw new FasterException($"Disposing LockableUnsafeContext with locks held: {sharedLockCount} shared locks, {exclusiveLockCount} exclusive locks");
            this.isAcquired = false;
            this.clientSession.fht.DecrementNumLockingSessions();
        }
        #endregion Acquire and Dispose

        #region Key Locking

        /// <summary>
        /// Lock the key with the specified <paramref name="lockType"/>, waiting until it is acquired
        /// </summary>
        /// <param name="key">The key to lock</param>
        /// <param name="lockType">The type of lock to take</param>
        public unsafe void Lock(ref Key key, LockType lockType)
        {
            CheckAcquired();
            Debug.Assert(clientSession.fht.epoch.ThisInstanceProtected(), "Epoch protection required for Lock()");

            LockOperation lockOp = new(LockOperationType.Lock, lockType);

            OperationStatus status;
            bool oneMiss = false;
            do
                status = clientSession.fht.InternalLock(ref key, lockOp, ref oneMiss, out _);
            while (status == OperationStatus.RETRY_NOW);
            Debug.Assert(status == OperationStatus.SUCCESS);

            if (lockType == LockType.Exclusive)
                ++this.exclusiveLockCount;
            else
                ++this.sharedLockCount;
        }

        /// <summary>
        /// Lock the key with the specified <paramref name="lockType"/>, waiting until it is acquired
        /// </summary>
        /// <param name="key">The key to lock</param>
        /// <param name="lockType">The type of lock to take</param>
        public unsafe void Lock(Key key, LockType lockType) => Lock(ref key, lockType);

        /// <summary>
        /// Lock the key with the specified <paramref name="lockType"/>
        /// </summary>
        /// <param name="key">The key to lock</param>
        /// <param name="lockType">The type of lock to release</param>
        public void Unlock(ref Key key, LockType lockType)
        {
            CheckAcquired();
            Debug.Assert(clientSession.fht.epoch.ThisInstanceProtected(), "Epoch protection required for Unlock()");

            LockOperation lockOp = new(LockOperationType.Unlock, lockType);

            OperationStatus status;
            bool oneMiss = false;
            do
                status = clientSession.fht.InternalLock(ref key, lockOp, ref oneMiss, out _);
            while (status == OperationStatus.RETRY_NOW);
            Debug.Assert(status == OperationStatus.SUCCESS);

            if (lockType == LockType.Exclusive)
                --this.exclusiveLockCount;
            else
                --this.sharedLockCount;
        }

        /// <summary>
        /// Unlock the key with the specified <paramref name="lockType"/>
        /// </summary>
        /// <param name="key">The key to lock</param>
        /// <param name="lockType">The type of lock to release</param>
        public void Unlock(Key key, LockType lockType) => Unlock(ref key, lockType);

        /// <summary>
        /// Determines if the key is locked. Note this value may be obsolete as soon as it returns.
        /// </summary>
        /// <param name="key">The key to lock</param>
        public (bool exclusive, byte shared) IsLocked(ref Key key)
        {
            CheckAcquired();
            Debug.Assert(clientSession.fht.epoch.ThisInstanceProtected(), "Epoch protection required for IsLocked()");

            LockOperation lockOp = new(LockOperationType.IsLocked, LockType.None);

            OperationStatus status;
            RecordInfo lockInfo;
            bool oneMiss = false;
            do
                status = clientSession.fht.InternalLock(ref key, lockOp, ref oneMiss, out lockInfo);
            while (status == OperationStatus.RETRY_NOW);
            Debug.Assert(status == OperationStatus.SUCCESS);
            return (lockInfo.IsLockedExclusive, lockInfo.NumLockedShared);
        }

        /// <summary>
        /// Determines if the key is locked. Note this value may be obsolete as soon as it returns.
        /// </summary>
        /// <param name="key">The key to lock</param>
        public (bool exclusive, byte shared) IsLocked(Key key) => IsLocked(ref key);

        #endregion Key Locking

        #region IFasterContext

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
        public Status Read(ref Key key, ref Input input, ref Output output, ref ReadOptions readOptions, out RecordMetadata recordMetadata, Context userContext = default, long serialNo = 0)
        {
            Debug.Assert(clientSession.fht.epoch.ThisInstanceProtected());
            return clientSession.fht.ContextRead(ref key, ref input, ref output, ref readOptions, out recordMetadata, userContext, FasterSession, serialNo, clientSession.ctx);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status ReadAtAddress(ref Input input, ref Output output, ref ReadOptions readOptions, Context userContext = default, long serialNo = 0)
        {
            Debug.Assert(clientSession.fht.epoch.ThisInstanceProtected());
            return clientSession.fht.ContextReadAtAddress(ref input, ref output, ref readOptions, userContext, FasterSession, serialNo, clientSession.ctx);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<FasterKV<Key, Value, StoreFunctions>.ReadAsyncResult<Input, Output, Context>> ReadAsync(ref Key key, ref Input input, Context userContext = default, long serialNo = 0, CancellationToken cancellationToken = default)
        {
            Debug.Assert(!clientSession.fht.epoch.ThisInstanceProtected());
            ReadOptions readOptions = default;
            return clientSession.fht.ReadAsync(FasterSession, clientSession.ctx, ref key, ref input, ref readOptions, userContext, serialNo, cancellationToken);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<FasterKV<Key, Value, StoreFunctions>.ReadAsyncResult<Input, Output, Context>> ReadAsync(Key key, Input input, Context context = default, long serialNo = 0, CancellationToken token = default)
        {
            Debug.Assert(!clientSession.fht.epoch.ThisInstanceProtected());
            ReadOptions readOptions = default;
            return clientSession.fht.ReadAsync(FasterSession, clientSession.ctx, ref key, ref input, ref readOptions, context, serialNo, token);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<FasterKV<Key, Value, StoreFunctions>.ReadAsyncResult<Input, Output, Context>> ReadAsync(ref Key key, Context userContext = default, long serialNo = 0, CancellationToken token = default)
        {
            Debug.Assert(!clientSession.fht.epoch.ThisInstanceProtected());
            Input input = default;
            ReadOptions readOptions = default;
            return clientSession.fht.ReadAsync(FasterSession, clientSession.ctx, ref key, ref input, ref readOptions, userContext, serialNo, token);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<FasterKV<Key, Value, StoreFunctions>.ReadAsyncResult<Input, Output, Context>> ReadAsync(Key key, Context context = default, long serialNo = 0, CancellationToken token = default)
        {
            Debug.Assert(!clientSession.fht.epoch.ThisInstanceProtected());
            Input input = default;
            ReadOptions readOptions = default;
            return clientSession.fht.ReadAsync(FasterSession, clientSession.ctx, ref key, ref input, ref readOptions, context, serialNo, token);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<FasterKV<Key, Value, StoreFunctions>.ReadAsyncResult<Input, Output, Context>> ReadAsync(ref Key key, ref Input input, ref ReadOptions readOptions,
                                                                                                 Context userContext = default, long serialNo = 0, CancellationToken cancellationToken = default)
        {
            Debug.Assert(!clientSession.fht.epoch.ThisInstanceProtected());
            return clientSession.fht.ReadAsync(FasterSession, clientSession.ctx, ref key, ref input, ref readOptions, userContext, serialNo, cancellationToken);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<FasterKV<Key, Value, StoreFunctions>.ReadAsyncResult<Input, Output, Context>> ReadAtAddressAsync(ref Input input, ref ReadOptions readOptions,
                                                                                                          Context userContext = default, long serialNo = 0, CancellationToken cancellationToken = default)
        {
            Debug.Assert(!clientSession.fht.epoch.ThisInstanceProtected());
            Key key = default;
            return clientSession.fht.ReadAsync(FasterSession, clientSession.ctx, ref key, ref input, ref readOptions, userContext, serialNo, cancellationToken, noKey: true);
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
            return clientSession.fht.ContextUpsert(ref key, ref input, ref desiredValue, ref output, out recordMetadata, userContext, FasterSession, serialNo, clientSession.ctx);
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
        public ValueTask<FasterKV<Key, Value, StoreFunctions>.UpsertAsyncResult<Input, Output, Context>> UpsertAsync(ref Key key, ref Value desiredValue, Context userContext = default, long serialNo = 0, CancellationToken token = default)
        {
            Input input = default;
            return UpsertAsync(ref key, ref input, ref desiredValue, userContext, serialNo, token);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<FasterKV<Key, Value, StoreFunctions>.UpsertAsyncResult<Input, Output, Context>> UpsertAsync(ref Key key, ref Input input, ref Value desiredValue, Context userContext = default, long serialNo = 0, CancellationToken token = default)
        {
            Debug.Assert(!clientSession.fht.epoch.ThisInstanceProtected());
            return clientSession.fht.UpsertAsync(FasterSession, clientSession.ctx, ref key, ref input, ref desiredValue, userContext, serialNo, token);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<FasterKV<Key, Value, StoreFunctions>.UpsertAsyncResult<Input, Output, Context>> UpsertAsync(Key key, Value desiredValue, Context userContext = default, long serialNo = 0, CancellationToken token = default)
            => UpsertAsync(ref key, ref desiredValue, userContext, serialNo, token);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<FasterKV<Key, Value, StoreFunctions>.UpsertAsyncResult<Input, Output, Context>> UpsertAsync(Key key, Input input, Value desiredValue, Context userContext = default, long serialNo = 0, CancellationToken token = default)
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
        public ValueTask<FasterKV<Key, Value, StoreFunctions>.RmwAsyncResult<Input, Output, Context>> RMWAsync(ref Key key, ref Input input, Context context = default, long serialNo = 0, CancellationToken token = default)
        {
            Debug.Assert(!clientSession.fht.epoch.ThisInstanceProtected());
            return clientSession.fht.RmwAsync(FasterSession, clientSession.ctx, ref key, ref input, context, serialNo, token);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<FasterKV<Key, Value, StoreFunctions>.RmwAsyncResult<Input, Output, Context>> RMWAsync(Key key, Input input, Context context = default, long serialNo = 0, CancellationToken token = default)
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
        public ValueTask<FasterKV<Key, Value, StoreFunctions>.DeleteAsyncResult<Input, Output, Context>> DeleteAsync(ref Key key, Context userContext = default, long serialNo = 0, CancellationToken token = default)
        {
            Debug.Assert(!clientSession.fht.epoch.ThisInstanceProtected());
            return clientSession.fht.DeleteAsync(FasterSession, clientSession.ctx, ref key, userContext, serialNo, token);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<FasterKV<Key, Value, StoreFunctions>.DeleteAsyncResult<Input, Output, Context>> DeleteAsync(Key key, Context userContext = default, long serialNo = 0, CancellationToken token = default)
            => DeleteAsync(ref key, userContext, serialNo, token);

        /// <inheritdoc/>
        public void Refresh()
        {
            Debug.Assert(clientSession.fht.epoch.ThisInstanceProtected());
            clientSession.fht.InternalRefresh(clientSession.ctx, FasterSession);
        }

        #endregion IFasterContext

        #region IFasterSession

        // This is a struct to allow JIT to inline calls (and bypass default interface call mechanism)
        internal readonly struct InternalFasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            private readonly ClientSession<Key, Value, Input, Output, Context, Functions, StoreFunctions> _clientSession;

            public InternalFasterSession(ClientSession<Key, Value, Input, Output, Context, Functions, StoreFunctions> clientSession)
            {
                _clientSession = clientSession;
            }

            #region IFunctions - Optional features supported
            public bool DisableLocking => true;       // We only lock explicitly in Lock/Unlock, which are longer-duration locks.

            public bool IsManualLocking => true;

            public SessionType SessionType => SessionType.LockableUnsafeContext;
            #endregion IFunctions - Optional features supported

            #region IFunctions - Reads
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool SingleReader(ref Key key, ref Input input, ref Value value, ref Output dst, ref RecordInfo recordInfo, ref ReadInfo readInfo) 
                => _clientSession.functions.SingleReader(ref key, ref input, ref value, ref dst, ref readInfo);

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool ConcurrentReader(ref Key key, ref Input input, ref Value value, ref Output dst, ref RecordInfo recordInfo, ref ReadInfo readInfo, out bool lockFailed)
            {
                lockFailed = false;
                if (_clientSession.functions.ConcurrentReader(ref key, ref input, ref value, ref dst, ref readInfo))
                    return true;
                if (readInfo.Action == ReadAction.Expire)
                    recordInfo.Tombstone = true;
                return false;
            }

            public void ReadCompletionCallback(ref Key key, ref Input input, ref Output output, Context ctx, Status status, RecordMetadata recordMetadata)
                => _clientSession.functions.ReadCompletionCallback(ref key, ref input, ref output, ctx, status, recordMetadata);

            #endregion IFunctions - Reads

            // Our general locking rule in this "session" is: we don't lock unless explicitly requested.

            #region IFunctions - Upserts
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool SingleWriter(ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref RecordInfo recordInfo, ref UpsertInfo upsertInfo, WriteReason reason) 
                => _clientSession.functions.SingleWriter(ref key, ref input, ref src, ref dst, ref output, ref upsertInfo, reason);

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void PostSingleWriter(ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref RecordInfo recordInfo, ref UpsertInfo upsertInfo, WriteReason reason)
                => _clientSession.functions.PostSingleWriter(ref key, ref input, ref src, ref dst, ref output, ref upsertInfo, reason);

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool ConcurrentWriter(ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref RecordInfo recordInfo, ref UpsertInfo upsertInfo, out bool lockFailed)
            {
                // Note: KeyIndexes do not need notification of in-place updates because the key does not change.
                lockFailed = false;
                recordInfo.SetDirty();
                return _clientSession.functions.ConcurrentWriter(ref key, ref input, ref src, ref dst, ref output, ref upsertInfo);
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
                => _clientSession.functions.PostInitialUpdater(ref key, ref input, ref value, ref output, ref rmwInfo);
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
                => _clientSession.functions.PostCopyUpdater(ref key, ref input, ref oldValue, ref newValue, ref output, ref rmwInfo);
            #endregion CopyUpdater

            #region InPlaceUpdater
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool InPlaceUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref RecordInfo recordInfo, ref RMWInfo rmwInfo, out bool lockFailed, out OperationStatus status)
            {
                lockFailed = false;
                return _clientSession.InPlaceUpdater(ref key, ref input, ref output, ref value, ref recordInfo, ref rmwInfo, out status);
            }

            public void RMWCompletionCallback(ref Key key, ref Input input, ref Output output, Context ctx, Status status, RecordMetadata recordMetadata)
                => _clientSession.functions.RMWCompletionCallback(ref key, ref input, ref output, ctx, status, recordMetadata);

            #endregion InPlaceUpdater
            #endregion IFunctions - RMWs

            #region IFunctions - Deletes
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool SingleDeleter(ref Key key, ref Value value, ref RecordInfo recordInfo, ref DeleteInfo deleteInfo)
            {
                value = default;
                return true;
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void PostSingleDeleter(ref Key key, ref RecordInfo recordInfo, ref DeleteInfo deleteInfo)
            {
                recordInfo.SetDirty();
                _clientSession.functions.PostSingleDeleter(ref key, ref deleteInfo);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool ConcurrentDeleter(ref Key key, ref Value value, ref RecordInfo recordInfo, ref DeleteInfo deleteInfo, out bool lockFailed)
            {
                lockFailed = false;
                recordInfo.SetDirty();
                recordInfo.Tombstone = true;
                return _clientSession.functions.ConcurrentDeleter(ref key, ref value, ref deleteInfo);
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
