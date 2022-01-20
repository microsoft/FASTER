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
    /// Faster Operations implementation that allows manual control of record epoch management. For advanced use only.
    /// </summary>
    public sealed class UnsafeContext<Key, Value, Input, Output, Context, Functions> : IFasterContext<Key, Value, Input, Output, Context>, IDisposable
        where Functions : IFunctions<Key, Value, Input, Output, Context>
    {
        readonly ClientSession<Key, Value, Input, Output, Context, Functions> clientSession;

        internal readonly InternalFasterSession FasterSession;
        bool isAcquired;

        void CheckAcquired()
        {
            if (!isAcquired)
                throw new FasterException("Method call on not-acquired UnsafeContext");
        }

        internal UnsafeContext(ClientSession<Key, Value, Input, Output, Context, Functions> clientSession)
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

        /// <summary>
        /// Synchronously complete outstanding pending synchronous operations.
        /// Async operations must be completed individually.
        /// </summary>
        /// <param name="wait">Wait for all pending operations on session to complete</param>
        /// <param name="spinWaitForCommit">Spin-wait until ongoing commit/checkpoint, if any, completes</param>
        /// <returns>True if all pending operations have completed, false otherwise</returns>
        public bool CompletePending(bool wait = false, bool spinWaitForCommit = false)
            => this.clientSession.UnsafeCompletePending(this.FasterSession, false, wait, spinWaitForCommit);

        /// <summary>
        /// Synchronously complete outstanding pending synchronous operations, returning outputs for the completed operations.
        /// Assumes epoch protection is managed by user. Async operations must be completed individually.
        /// </summary>
        /// <param name="completedOutputs">Outputs completed by this operation</param>
        /// <param name="wait">Wait for all pending operations on session to complete</param>
        /// <param name="spinWaitForCommit">Spin-wait until ongoing commit/checkpoint, if any, completes</param>
        /// <returns>True if all pending operations have completed, false otherwise</returns>
        public bool CompletePendingWithOutputs(out CompletedOutputIterator<Key, Value, Input, Output, Context> completedOutputs, bool wait = false, bool spinWaitForCommit = false)
            => this.clientSession.UnsafeCompletePendingWithOutputs(this.FasterSession, out completedOutputs, wait, spinWaitForCommit);

        #region Acquire and Dispose
        internal void Acquire()
        {
            if (this.isAcquired)
                throw new FasterException("Trying to acquire an already-acquired UnsafeContext");
            this.isAcquired = true;
        }

        /// <summary>
        /// Does not actually dispose of anything; asserts the epoch has been suspended
        /// </summary>
        public void Dispose()
        {
            if (LightEpoch.AnyInstanceProtected())
                throw new FasterException("Disposing UnsafeContext with a protected epoch; must call UnsafeSuspendThread");
            this.isAcquired = false;
        }
        #endregion Acquire and Dispose

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
        public Status Read(ref Key key, ref Input input, ref Output output, ref RecordMetadata recordMetadata, ReadFlags readFlags = ReadFlags.None, Context userContext = default, long serialNo = 0)
        {
            Debug.Assert(clientSession.fht.epoch.ThisInstanceProtected());
            return clientSession.fht.ContextRead(ref key, ref input, ref output, ref recordMetadata, readFlags, userContext, FasterSession, serialNo, clientSession.ctx);
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
            public bool SupportsLocking => _clientSession.fht.SupportsLocking;

            public bool IsManualLocking => false;
            #endregion IFunctions - Optional features supported

            #region IFunctions - Reads
            public bool SingleReader(ref Key key, ref Input input, ref Value value, ref Output dst, ref RecordInfo recordInfo, long address)
                => _clientSession.functions.SingleReader(ref key, ref input, ref value, ref dst, ref recordInfo, address);

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool ConcurrentReader(ref Key key, ref Input input, ref Value value, ref Output dst, ref RecordInfo recordInfo, long address, out bool lockFailed)
            {
                lockFailed = false;
                return !this.SupportsLocking
                                   ? _clientSession.functions.ConcurrentReader(ref key, ref input, ref value, ref dst, ref recordInfo, address)
                                   : ConcurrentReaderLock(ref key, ref input, ref value, ref dst, ref recordInfo, address, out lockFailed);
            }

            public bool ConcurrentReaderLock(ref Key key, ref Input input, ref Value value, ref Output dst, ref RecordInfo recordInfo, long address, out bool lockFailed)
            {
                if (!recordInfo.LockShared())
                {
                    lockFailed = true;
                    return false;
                }
                try
                {
                    lockFailed = false;
                    return _clientSession.functions.ConcurrentReader(ref key, ref input, ref value, ref dst, ref recordInfo, address);
                }
                finally
                {
                    recordInfo.UnlockShared();
                }
            }

            public void ReadCompletionCallback(ref Key key, ref Input input, ref Output output, Context ctx, Status status, RecordMetadata recordMetadata)
                => _clientSession.functions.ReadCompletionCallback(ref key, ref input, ref output, ctx, status, recordMetadata);

            #endregion IFunctions - Reads

            // Except for readcache/copy-to-tail usage of SingleWriter, all operations that append a record must lock in the <Operation>() call and unlock
            // in the Post<Operation> call; otherwise another session can try to access the record as soon as it's CAS'd and before Post<Operation> is called.

            #region IFunctions - Upserts
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void SingleWriter(ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref RecordInfo recordInfo, long address) 
                => _clientSession.functions.SingleWriter(ref key, ref input, ref src, ref dst, ref output, ref recordInfo, address);

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void PostSingleWriter(ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref RecordInfo recordInfo, long address)
                => _clientSession.functions.PostSingleWriter(ref key, ref input, ref src, ref dst, ref output, ref recordInfo, address);

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void CopyWriter(ref Key key, ref Value src, ref Value dst, ref RecordInfo recordInfo, long address) 
                => _clientSession.functions.CopyWriter(ref key, ref src, ref dst, ref recordInfo, address);

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void PostCopyWriter(ref Key key, ref Value src, ref Value dst, ref RecordInfo recordInfo, long address)
            {
                _clientSession.functions.PostCopyWriter(ref key, ref src, ref dst, ref recordInfo, address);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool ConcurrentWriter(ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref RecordInfo recordInfo, long address, out bool lockFailed)
            {
                lockFailed = false;
                return !this.SupportsLocking
                                   ? ConcurrentWriterNoLock(ref key, ref input, ref src, ref dst, ref output, ref recordInfo, address)
                                   : ConcurrentWriterLock(ref key, ref input, ref src, ref dst, ref output, ref recordInfo, address, out lockFailed);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private bool ConcurrentWriterNoLock(ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref RecordInfo recordInfo, long address)
            {
                recordInfo.SetDirty();
                // Note: KeyIndexes do not need notification of in-place updates because the key does not change.
                return _clientSession.functions.ConcurrentWriter(ref key, ref input, ref src, ref dst, ref output, ref recordInfo, address);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private bool ConcurrentWriterLock(ref Key key, ref Input input, ref Value src, ref Value dst, ref Output output, ref RecordInfo recordInfo, long address, out bool lockFailed)
            {
                if (!recordInfo.LockExclusive())
                {
                    lockFailed = true;
                    return false;
                }
                try
                {
                    lockFailed = false;
                    return !recordInfo.Tombstone && ConcurrentWriterNoLock(ref key, ref input, ref src, ref dst, ref output, ref recordInfo, address);
                }
                finally
                {
                    recordInfo.UnlockExclusive();
                }
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
            public void InitialUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref RecordInfo recordInfo, long address) 
                => _clientSession.functions.InitialUpdater(ref key, ref input, ref value, ref output, ref recordInfo, address);

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void PostInitialUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref RecordInfo recordInfo, long address) 
                => _clientSession.functions.PostInitialUpdater(ref key, ref input, ref value, ref output, ref recordInfo, address);
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
                => _clientSession.functions.PostCopyUpdater(ref key, ref input, ref oldValue, ref newValue, ref output, ref recordInfo, address);
            #endregion CopyUpdater

            #region InPlaceUpdater
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool InPlaceUpdater(ref Key key, ref Input input, ref Value value, ref Output output, ref RecordInfo recordInfo, long address, out bool lockFailed)
            {
                lockFailed = false;
                return !this.SupportsLocking
                                   ? InPlaceUpdaterNoLock(ref key, ref input, ref output, ref value, ref recordInfo, address)
                                   : InPlaceUpdaterLock(ref key, ref input, ref output, ref value, ref recordInfo, address, out lockFailed);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private bool InPlaceUpdaterNoLock(ref Key key, ref Input input, ref Output output, ref Value value, ref RecordInfo recordInfo, long address)
            {
                recordInfo.SetDirty();
                // Note: KeyIndexes do not need notification of in-place updates because the key does not change.
                return _clientSession.functions.InPlaceUpdater(ref key, ref input, ref value, ref output, ref recordInfo, address);
            }

            private bool InPlaceUpdaterLock(ref Key key, ref Input input, ref Output output, ref Value value, ref RecordInfo recordInfo, long address, out bool lockFailed)
            {
                if (!recordInfo.LockExclusive())
                {
                    lockFailed = true;
                    return false;
                }
                try
                {
                    lockFailed = false;
                    return !recordInfo.Tombstone && InPlaceUpdaterNoLock(ref key, ref input, ref output, ref value, ref recordInfo, address);
                }
                finally
                {
                    recordInfo.UnlockExclusive();
                }
            }

            public void RMWCompletionCallback(ref Key key, ref Input input, ref Output output, Context ctx, Status status, RecordMetadata recordMetadata)
                => _clientSession.functions.RMWCompletionCallback(ref key, ref input, ref output, ctx, status, recordMetadata);

            #endregion InPlaceUpdater
            #endregion IFunctions - RMWs

            #region IFunctions - Deletes
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void PostSingleDeleter(ref Key key, ref RecordInfo recordInfo, long address)
            {
                recordInfo.SetDirty();
                _clientSession.functions.PostSingleDeleter(ref key, ref recordInfo, address);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void SingleDeleter(ref Key key, ref Value value, ref RecordInfo recordInfo, long address) { value = default; }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool ConcurrentDeleter(ref Key key, ref Value value, ref RecordInfo recordInfo, long address, out bool lockFailed)
            {
                lockFailed = false;
                return (!this.SupportsLocking)
                                   ? ConcurrentDeleterNoLock(ref key, ref value, ref recordInfo, address)
                                   : ConcurrentDeleterLock(ref key, ref value, ref recordInfo, address, out lockFailed);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private bool ConcurrentDeleterNoLock(ref Key key, ref Value value, ref RecordInfo recordInfo, long address)
            {
                recordInfo.SetDirty();
                recordInfo.SetTombstone();
                return _clientSession.functions.ConcurrentDeleter(ref key, ref value, ref recordInfo, address);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private bool ConcurrentDeleterLock(ref Key key, ref Value value, ref RecordInfo recordInfo, long address, out bool lockFailed)
            {
                if (!recordInfo.LockExclusive())
                {
                    lockFailed = true;
                    return false;
                }
                try
                {
                    lockFailed = false;
                    return ConcurrentDeleterNoLock(ref key, ref value, ref recordInfo, address);
                }
                finally
                {
                    recordInfo.UnlockExclusive();
                }
            }

            public void DeleteCompletionCallback(ref Key key, Context ctx)
                => _clientSession.functions.DeleteCompletionCallback(ref key, ctx);
            #endregion IFunctions - Deletes

            #region Key and Value management
            public void DisposeKey(ref Key key) { _clientSession.functions.DisposeKey(ref key); }

            public void DisposeValue(ref Value value) { _clientSession.functions.DisposeValue(ref value); }
            #endregion Key and Value management

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
