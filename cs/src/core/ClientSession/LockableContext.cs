// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace FASTER.core
{
    /// <summary>
    /// Faster Context implementation that allows manual control of record locking and epoch management. For advanced use only.
    /// </summary>
    public readonly struct LockableContext<Key, Value, Input, Output, Context, Functions> : IFasterContext<Key, Value, Input, Output, Context>, ILockableContext<Key>
        where Functions : IFunctions<Key, Value, Input, Output, Context>
    {
        readonly ClientSession<Key, Value, Input, Output, Context, Functions> clientSession;
        readonly InternalFasterSession FasterSession;

        internal LockableContext(ClientSession<Key, Value, Input, Output, Context, Functions> clientSession)
        {
            this.clientSession = clientSession;
            FasterSession = new InternalFasterSession(clientSession);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void UnsafeResumeThread()
        {
            clientSession.CheckAcquired();
            Debug.Assert(!clientSession.fht.epoch.ThisInstanceProtected());
            clientSession.UnsafeResumeThread();
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void UnsafeResumeThread(out int resumeEpoch)
        {
            clientSession.CheckAcquired();
            Debug.Assert(!clientSession.fht.epoch.ThisInstanceProtected());
            clientSession.UnsafeResumeThread(out resumeEpoch);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void UnsafeSuspendThread()
        {
            clientSession.CheckAcquired();
            Debug.Assert(clientSession.fht.epoch.ThisInstanceProtected());
            clientSession.UnsafeSuspendThread();
        }

        /// <inheritdoc/>
        public int LocalCurrentEpoch => clientSession.fht.epoch.LocalCurrentEpoch;

        #region Acquire and Dispose

        /// <summary>
        /// Acquire a lockable context
        /// </summary>
        public void Acquire()
        {
            clientSession.Acquire();
        }

        /// <summary>
        /// Release a lockable context; does not actually dispose of anything
        /// </summary>
        public void Release()
        {
            if (clientSession.fht.epoch.ThisInstanceProtected())
                throw new FasterException("Releasing LockableContext with a protected epoch; must call UnsafeSuspendThread");
            if (clientSession.TotalLockCount > 0)
                throw new FasterException($"Releasing LockableContext with locks held: {clientSession.sharedLockCount} shared locks, {clientSession.exclusiveLockCount} exclusive locks");
            clientSession.Release();
        }
        #endregion Acquire and Dispose

        #region Key Locking

        /// <inheritdoc/>
        public unsafe void Lock(ref Key key, LockType lockType)
        {
            clientSession.CheckAcquired();
            Debug.Assert(!clientSession.fht.epoch.ThisInstanceProtected());
            clientSession.UnsafeResumeThread();
            try
            {
                LockOperation lockOp = new(LockOperationType.Lock, lockType);

                OperationStatus status;
                bool oneMiss = false;
                do
                    status = clientSession.fht.InternalLock(ref key, lockOp, ref oneMiss, out _);
                while (clientSession.fht.HandleImmediateNonPendingRetryStatus(status, clientSession.ctx, FasterSession));
                Debug.Assert(status == OperationStatus.SUCCESS);

                if (lockType == LockType.Exclusive)
                    ++clientSession.exclusiveLockCount;
                else
                    ++clientSession.sharedLockCount;
            }
            finally
            {
                clientSession.UnsafeSuspendThread();
            }
        }

        /// <inheritdoc/>
        public unsafe void Lock(Key key, LockType lockType) => Lock(ref key, lockType);

        /// <inheritdoc/>
        public void Unlock(ref Key key, LockType lockType)
        {
            clientSession.CheckAcquired();
            Debug.Assert(!clientSession.fht.epoch.ThisInstanceProtected());
            clientSession.UnsafeResumeThread();
            try
            {
                LockOperation lockOp = new(LockOperationType.Unlock, lockType);

                OperationStatus status;
                bool oneMiss = false;
                do
                    status = clientSession.fht.InternalLock(ref key, lockOp, ref oneMiss, out _);
                while (clientSession.fht.HandleImmediateNonPendingRetryStatus(status, clientSession.ctx, FasterSession));
                Debug.Assert(status == OperationStatus.SUCCESS);

                if (lockType == LockType.Exclusive)
                    --clientSession.exclusiveLockCount;
                else
                    --clientSession.sharedLockCount;
            }
            finally
            {
                clientSession.UnsafeSuspendThread();
            }
        }

        /// <inheritdoc/>
        public void Unlock(Key key, LockType lockType) => Unlock(ref key, lockType);

        /// <inheritdoc/>
        public (bool exclusive, byte shared) IsLocked(ref Key key)
        {
            clientSession.CheckAcquired();
            Debug.Assert(!clientSession.fht.epoch.ThisInstanceProtected());
            clientSession.UnsafeResumeThread();
            try
            {
                LockOperation lockOp = new(LockOperationType.IsLocked, LockType.None);

                OperationStatus status;
                RecordInfo lockInfo;
                bool oneMiss = false;
                do
                    status = clientSession.fht.InternalLock(ref key, lockOp, ref oneMiss, out lockInfo);
                while (clientSession.fht.HandleImmediateNonPendingRetryStatus(status, clientSession.ctx, FasterSession));
                Debug.Assert(status == OperationStatus.SUCCESS);
                return (lockInfo.IsLockedExclusive, lockInfo.NumLockedShared);
            }
            finally
            {
                clientSession.UnsafeSuspendThread();
            }
       }

        /// <inheritdoc/>
        public (bool exclusive, byte shared) IsLocked(Key key) => IsLocked(ref key);

        /// <summary>
        /// The session id of FasterSession
        /// </summary>
        public int SessionID { get { return clientSession.ctx.sessionID; } }

        #endregion Key Locking

        #region IFasterContext

        /// <inheritdoc/>
        public bool CompletePending(bool wait = false, bool spinWaitForCommit = false)
        {
            Debug.Assert(!clientSession.fht.epoch.ThisInstanceProtected());
            clientSession.UnsafeResumeThread();
            try
            {
                return this.clientSession.UnsafeCompletePending(this.FasterSession, false, wait, spinWaitForCommit);
            }
            finally
            {
                clientSession.UnsafeSuspendThread();
            }
        }

        /// <inheritdoc/>
        public bool CompletePendingWithOutputs(out CompletedOutputIterator<Key, Value, Input, Output, Context> completedOutputs, bool wait = false, bool spinWaitForCommit = false)
        {
            Debug.Assert(!clientSession.fht.epoch.ThisInstanceProtected());
            clientSession.UnsafeResumeThread();
            try
            {
                return this.clientSession.UnsafeCompletePendingWithOutputs(this.FasterSession, out completedOutputs, wait, spinWaitForCommit);
            }
            finally
            {
                clientSession.UnsafeSuspendThread();
            }
        }

        /// <inheritdoc/>
        public ValueTask CompletePendingAsync(bool waitForCommit = false, CancellationToken token = default)
            => this.clientSession.CompletePendingAsync(waitForCommit, token);

        /// <inheritdoc/>
        public ValueTask<CompletedOutputIterator<Key, Value, Input, Output, Context>> CompletePendingWithOutputsAsync(bool waitForCommit = false, CancellationToken token = default)
            => this.clientSession.CompletePendingWithOutputsAsync(waitForCommit, token);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read(ref Key key, ref Input input, ref Output output, Context userContext = default, long serialNo = 0)
        {
            Debug.Assert(!clientSession.fht.epoch.ThisInstanceProtected());
            clientSession.UnsafeResumeThread();
            try
            {
                return clientSession.fht.ContextRead(ref key, ref input, ref output, userContext, FasterSession, serialNo, clientSession.ctx);
            }
            finally
            {
                clientSession.UnsafeSuspendThread();
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
            Debug.Assert(!clientSession.fht.epoch.ThisInstanceProtected());
            clientSession.UnsafeResumeThread();
            try
            {
                return clientSession.fht.ContextRead(ref key, ref input, ref output, ref readOptions, out recordMetadata, userContext, FasterSession, serialNo, clientSession.ctx);
            }
            finally
            {
                clientSession.UnsafeSuspendThread();
            }
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status ReadAtAddress(ref Input input, ref Output output, ref ReadOptions readOptions, Context userContext = default, long serialNo = 0)
        {
            Debug.Assert(!clientSession.fht.epoch.ThisInstanceProtected());
            clientSession.UnsafeResumeThread();
            try
            {
                return clientSession.fht.ContextReadAtAddress(ref input, ref output, ref readOptions, userContext, FasterSession, serialNo, clientSession.ctx);
            }
            finally
            {
                clientSession.UnsafeSuspendThread();
            }
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<FasterKV<Key, Value>.ReadAsyncResult<Input, Output, Context>> ReadAsync(ref Key key, ref Input input, Context userContext = default, long serialNo = 0, CancellationToken cancellationToken = default)
        {
            Debug.Assert(!clientSession.fht.epoch.ThisInstanceProtected());
            ReadOptions readOptions = default;
            return clientSession.fht.ReadAsync(FasterSession, clientSession.ctx, ref key, ref input, ref readOptions, userContext, serialNo, cancellationToken);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<FasterKV<Key, Value>.ReadAsyncResult<Input, Output, Context>> ReadAsync(Key key, Input input, Context context = default, long serialNo = 0, CancellationToken token = default)
        {
            Debug.Assert(!clientSession.fht.epoch.ThisInstanceProtected());
            ReadOptions readOptions = default;
            return clientSession.fht.ReadAsync(FasterSession, clientSession.ctx, ref key, ref input, ref readOptions, context, serialNo, token);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<FasterKV<Key, Value>.ReadAsyncResult<Input, Output, Context>> ReadAsync(ref Key key, Context userContext = default, long serialNo = 0, CancellationToken token = default)
        {
            Debug.Assert(!clientSession.fht.epoch.ThisInstanceProtected());
            Input input = default;
            ReadOptions readOptions = default;
            return clientSession.fht.ReadAsync(FasterSession, clientSession.ctx, ref key, ref input, ref readOptions, userContext, serialNo, token);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<FasterKV<Key, Value>.ReadAsyncResult<Input, Output, Context>> ReadAsync(Key key, Context context = default, long serialNo = 0, CancellationToken token = default)
        {
            Debug.Assert(!clientSession.fht.epoch.ThisInstanceProtected());
            Input input = default;
            ReadOptions readOptions = default;
            return clientSession.fht.ReadAsync(FasterSession, clientSession.ctx, ref key, ref input, ref readOptions, context, serialNo, token);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<FasterKV<Key, Value>.ReadAsyncResult<Input, Output, Context>> ReadAsync(ref Key key, ref Input input, ref ReadOptions readOptions,
                                                                                                 Context userContext = default, long serialNo = 0, CancellationToken cancellationToken = default)
        {
            Debug.Assert(!clientSession.fht.epoch.ThisInstanceProtected());
            return clientSession.fht.ReadAsync(FasterSession, clientSession.ctx, ref key, ref input, ref readOptions, userContext, serialNo, cancellationToken);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<FasterKV<Key, Value>.ReadAsyncResult<Input, Output, Context>> ReadAtAddressAsync(ref Input input, ref ReadOptions readOptions,
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
            Debug.Assert(!clientSession.fht.epoch.ThisInstanceProtected());
            Input input = default;
            Output output = default;
            return Upsert(ref key, ref input, ref desiredValue, ref output, out _, userContext, serialNo);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(ref Key key, ref Input input, ref Value desiredValue, ref Output output, Context userContext = default, long serialNo = 0)
        {
            Debug.Assert(!clientSession.fht.epoch.ThisInstanceProtected());
            clientSession.UnsafeResumeThread();
            try
            {
                return clientSession.fht.ContextUpsert(ref key, ref input, ref desiredValue, ref output, userContext, FasterSession, serialNo, clientSession.ctx);
            }
            finally
            {
                clientSession.UnsafeSuspendThread();
            }
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(ref Key key, ref Input input, ref Value desiredValue, ref Output output, out RecordMetadata recordMetadata, Context userContext = default, long serialNo = 0)
        {
            Debug.Assert(!clientSession.fht.epoch.ThisInstanceProtected());
            clientSession.UnsafeResumeThread();
            try
            {
                return clientSession.fht.ContextUpsert(ref key, ref input, ref desiredValue, ref output, out recordMetadata, userContext, FasterSession, serialNo, clientSession.ctx);
            }
            finally
            {
                clientSession.UnsafeSuspendThread();
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
        {
            Debug.Assert(!clientSession.fht.epoch.ThisInstanceProtected());
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
            Debug.Assert(!clientSession.fht.epoch.ThisInstanceProtected());
            clientSession.UnsafeResumeThread();
            try
            {
                return clientSession.fht.ContextRMW(ref key, ref input, ref output, out recordMetadata, userContext, FasterSession, serialNo, clientSession.ctx);
            }
            finally
            {
                clientSession.UnsafeSuspendThread();
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
        {
            Debug.Assert(!clientSession.fht.epoch.ThisInstanceProtected());
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
            Debug.Assert(!clientSession.fht.epoch.ThisInstanceProtected());
            clientSession.UnsafeResumeThread();
            try
            {
                return clientSession.fht.ContextDelete(ref key, userContext, FasterSession, serialNo, clientSession.ctx);
            }
            finally
            {
                clientSession.UnsafeSuspendThread();
            }
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Delete(Key key, Context userContext = default, long serialNo = 0)
            => Delete(ref key, userContext, serialNo);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<FasterKV<Key, Value>.DeleteAsyncResult<Input, Output, Context>> DeleteAsync(ref Key key, Context userContext = default, long serialNo = 0, CancellationToken token = default)
        {
            Debug.Assert(!clientSession.fht.epoch.ThisInstanceProtected());
            return clientSession.fht.DeleteAsync(FasterSession, clientSession.ctx, ref key, userContext, serialNo, token);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<FasterKV<Key, Value>.DeleteAsyncResult<Input, Output, Context>> DeleteAsync(Key key, Context userContext = default, long serialNo = 0, CancellationToken token = default)
            => DeleteAsync(ref key, userContext, serialNo, token);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ResetModified(ref Key key)
            => clientSession.ResetModified(ref key);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool IsModified(Key key)
            => clientSession.IsModified(ref key);

        /// <inheritdoc/>
        public void Refresh()
        {
            Debug.Assert(!clientSession.fht.epoch.ThisInstanceProtected());
            clientSession.UnsafeResumeThread();
            try
            {
                clientSession.fht.InternalRefresh(clientSession.ctx, FasterSession);
            }
            finally
            {
                clientSession.UnsafeSuspendThread();
            }
        }

        #endregion IFasterContext

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
            public bool DisableLocking => true;       // We only lock explicitly in Lock/Unlock, which are longer-duration locks.

            public bool IsManualLocking => true;

            public SessionType SessionType => SessionType.LockableContext;
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
            {
                recordInfo.SetDirtyAndModified();
                _clientSession.functions.PostSingleWriter(ref key, ref input, ref src, ref dst, ref output, ref upsertInfo, reason);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool ConcurrentWriter(ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref RecordInfo recordInfo, ref UpsertInfo upsertInfo, out bool lockFailed)
            {
                // Note: KeyIndexes do not need notification of in-place updates because the key does not change.
                lockFailed = false;
                recordInfo.SetDirtyAndModified();
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
            {
                recordInfo.SetDirtyAndModified();
                _clientSession.functions.PostInitialUpdater(ref key, ref input, ref value, ref output, ref rmwInfo);
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
                recordInfo.SetDirtyAndModified();
                _clientSession.functions.PostCopyUpdater(ref key, ref input, ref oldValue, ref newValue, ref output, ref rmwInfo);
            }
            #endregion CopyUpdater

            #region InPlaceUpdater
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool InPlaceUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref RecordInfo recordInfo, ref RMWInfo rmwInfo, out bool lockFailed, out OperationStatus status)
            {
                recordInfo.SetDirtyAndModified();
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
                => _clientSession.functions.SingleDeleter(ref key, ref value, ref deleteInfo);

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void PostSingleDeleter(ref Key key, ref RecordInfo recordInfo, ref DeleteInfo deleteInfo)
            {
                recordInfo.SetDirtyAndModified();
                _clientSession.functions.PostSingleDeleter(ref key, ref deleteInfo);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool ConcurrentDeleter(ref Key key, ref Value value, ref RecordInfo recordInfo, ref DeleteInfo deleteInfo, out bool lockFailed)
            {
                lockFailed = false;
                recordInfo.SetDirtyAndModified();
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
