// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma warning disable 0162

using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace FASTER.core
{
    /// <summary>
    /// The legacy FASTER key-value store
    /// </summary>
    /// <typeparam name="Key">Key</typeparam>
    /// <typeparam name="Value">Value</typeparam>
    /// <typeparam name="Input">Input</typeparam>
    /// <typeparam name="Output">Output</typeparam>
    /// <typeparam name="Context">Context</typeparam>
    /// <typeparam name="Functions">Functions</typeparam>
    //[Obsolete("Use FasteKV that provides functions with sessions")]
    public partial class FasterKV<Key, Value, Input, Output, Context, Functions> : IDisposable, IFasterKV<Key, Value, Input, Output, Context, Functions>
        where Functions : IFunctions<Key, Value, Input, Output, Context>
    {
        private FastThreadLocal<FasterKV<Key, Value>.FasterExecutionContext<Input, Output, Context>> _threadCtx;

        private readonly FasterKV<Key, Value> _fasterKV;
        private readonly Functions _functions;
        private readonly IVariableLengthStruct<Value, Input> _variableLengthStructForInput;

        private LegacyFasterSession FasterSession => new LegacyFasterSession(this);

        /// <inheritdoc />
        public long EntryCount => _fasterKV.EntryCount;

        /// <inheritdoc />
        public long IndexSize => _fasterKV.IndexSize;

        /// <inheritdoc />
        public IFasterEqualityComparer<Key> Comparer => _fasterKV.Comparer;

        /// <inheritdoc />
        public LogAccessor<Key, Value> Log => _fasterKV.Log;

        /// <inheritdoc />
        public LogAccessor<Key, Value> ReadCache => _fasterKV.ReadCache;

        /// <inheritdoc />
        public FasterKV(long size, Functions functions, LogSettings logSettings,
            CheckpointSettings checkpointSettings = null, SerializerSettings<Key, Value> serializerSettings = null,
            IFasterEqualityComparer<Key> comparer = null,
            VariableLengthStructSettings<Key, Value> variableLengthStructSettings = null, IVariableLengthStruct<Value, Input> variableLengthStructForInput = null)
        {
            _functions = functions;
            _fasterKV = new FasterKV<Key, Value>(size, logSettings, checkpointSettings, serializerSettings, comparer, variableLengthStructSettings);
            _variableLengthStructForInput = variableLengthStructForInput;
            if (_fasterKV.hlog is VariableLengthBlittableAllocator<Key, Value> allocator && _variableLengthStructForInput == default)
            {
                _variableLengthStructForInput = new DefaultVariableLengthStruct<Value, Input>(allocator.ValueLength);
            }
        }

        /// <summary>
        /// Dispose FASTER instance - legacy items
        /// </summary>
        private void LegacyDispose()
        {
            _threadCtx?.Dispose();
        }

        private bool InLegacySession()
        {
            return _threadCtx != null;
        }

        /// <summary>
        /// Legacy API: Start session with FASTER - call once per thread before using FASTER
        /// </summary>
        /// <returns></returns>
        [Obsolete("Use NewSession() instead.")]
        public Guid StartSession()
        {
            if (_threadCtx == null)
                _threadCtx = new FastThreadLocal<FasterKV<Key, Value>.FasterExecutionContext<Input, Output, Context>>();

            return InternalAcquire();
        }

        /// <summary>
        /// Legacy API: Continue session with FASTER
        /// </summary>
        /// <param name="guid"></param>
        /// <returns></returns>
        [Obsolete("Use ResumeSession() instead.")]
        public CommitPoint ContinueSession(Guid guid)
        {
            StartSession();

            var cp = _fasterKV.InternalContinue<Input, Output, Context>(guid.ToString(), out var ctx);
            _threadCtx.Value = ctx;

            return cp;
        }

        /// <summary>
        ///  Legacy API: Stop session with FASTER
        /// </summary>
        [Obsolete("Use and dispose NewSession() instead.")]
        public void StopSession()
        {
            InternalRelease(_threadCtx.Value);
        }

        /// <summary>
        ///  Legacy API: Refresh epoch (release memory pins)
        /// </summary>
        [Obsolete("Use NewSession(), where Refresh() is not required by default.")]
        public void Refresh()
        {
            _fasterKV.InternalRefresh(_threadCtx.Value, FasterSession);
        }

        /// <summary>
        ///  Legacy API: Complete all pending operations issued by this session
        /// </summary>
        /// <param name="wait">Whether we spin-wait for pending operations to complete</param>
        /// <returns>Whether all pending operations have completed</returns>
        [Obsolete("Use NewSession() and invoke CompletePending() on the session.")]
        public bool CompletePending(bool wait = false)
        {
            return _fasterKV.InternalCompletePending(_threadCtx.Value, FasterSession, wait);
        }

        /// <summary>
        /// Legacy API: Read operation
        /// </summary>
        /// <param name="key">Key of read</param>
        /// <param name="input">Input argument used by Reader to select what part of value to read</param>
        /// <param name="output">Reader stores the read result in output</param>
        /// <param name="context">User context to identify operation in asynchronous callback</param>
        /// <param name="serialNo">Increasing sequence number of operation (used for recovery)</param>
        /// <returns>Status of operation</returns>
        [Obsolete("Use NewSession() and invoke Read() on the session.")]
        public Status Read(ref Key key, ref Input input, ref Output output, Context context, long serialNo)
        {
            return _fasterKV.ContextRead(ref key, ref input, ref output, context, FasterSession, serialNo, _threadCtx.Value);
        }

        /// <summary>
        /// Legacy API: (Blind) upsert operation
        /// </summary>
        /// <param name="key">Key of read</param>
        /// <param name="value">Value being upserted</param>
        /// <param name="context">User context to identify operation in asynchronous callback</param>
        /// <param name="serialNo">Increasing sequence number of operation (used for recovery)</param>
        /// <returns>Status of operation</returns>
        [Obsolete("Use NewSession() and invoke Upsert() on the session.")]
        public Status Upsert(ref Key key, ref Value value, Context context, long serialNo)
        {
            return _fasterKV.ContextUpsert(ref key, ref value, context, FasterSession, serialNo, _threadCtx.Value);
        }

        /// <summary>
        /// Atomic read-modify-write operation
        /// </summary>
        /// <param name="key">Key of read</param>
        /// <param name="input">Input argument used by RMW callback to perform operation</param>
        /// <param name="context">User context to identify operation in asynchronous callback</param>
        /// <param name="serialNo">Increasing sequence number of operation (used for recovery)</param>
        /// <returns>Status of operation</returns>
        [Obsolete("Use NewSession() and invoke RMW() on the session.")]
        public Status RMW(ref Key key, ref Input input, Context context, long serialNo)
        {
            return _fasterKV.ContextRMW(ref key, ref input, context, FasterSession, serialNo, _threadCtx.Value);
        }

        /// <summary>
        /// Delete entry (use tombstone if necessary)
        /// Hash entry is removed as a best effort (if key is in memory and at 
        /// the head of hash chain.
        /// Value is set to null (using ConcurrentWrite) if it is in mutable region
        /// </summary>
        /// <param name="key">Key of delete</param>
        /// <param name="context">User context to identify operation in asynchronous callback</param>
        /// <param name="serialNo">Increasing sequence number of operation (used for recovery)</param>
        /// <returns>Status of operation</returns>
        [Obsolete("Use NewSession() and invoke Delete() on the session.")]
        public Status Delete(ref Key key, Context context, long serialNo)
        {
            return _fasterKV.ContextDelete(ref key, context, FasterSession, serialNo, _threadCtx.Value);
        }

        /// <summary>
        /// Legacy API: Complete the ongoing checkpoint (if any)
        /// </summary>
        /// <param name="spinWait">Spin-wait for completion</param>
        /// <returns></returns>
        [Obsolete("Use NewSession() and CompleteCheckpointAsync() instead.")]
        public bool CompleteCheckpoint(bool spinWait = false)
        {
            if (!InLegacySession())
            {
                _fasterKV.CompleteCheckpointAsync().GetAwaiter().GetResult();
                return true;
            }

            // the thread has an active legacy session
            // so we need to constantly complete pending 
            // and refresh (done inside CompletePending)
            // for the checkpoint to be proceed
            do
            {
                CompletePending();
                if (_fasterKV.systemState.phase == Phase.REST)
                {
                    CompletePending();
                    return true;
                }
            } while (spinWait);

            return false;
        }

        /// <inheritdoc />
        public IFasterScanIterator<Key, Value> Iterate(long untilAddress = -1) => _fasterKV.Iterate(untilAddress);

        /// <inheritdoc />
        public IFasterScanIterator<Key, Value> Iterate<CompactionFunctions>(CompactionFunctions compactionFunctions, long untilAddress = -1)
            where CompactionFunctions : ICompactionFunctions<Key, Value>
            => _fasterKV.Iterate(compactionFunctions, untilAddress);


        private Guid InternalAcquire()
        {
            _fasterKV.epoch.Resume();
            _threadCtx.InitializeThread();
            Phase phase = _fasterKV.systemState.phase;
            if (phase != Phase.REST)
            {
                throw new FasterException("Can acquire only in REST phase!");
            }
            Guid guid = Guid.NewGuid();
            _threadCtx.Value = new FasterKV<Key, Value>.FasterExecutionContext<Input, Output, Context>();
            _fasterKV.InitContext(_threadCtx.Value, guid.ToString());

            _threadCtx.Value.prevCtx = new FasterKV<Key, Value>.FasterExecutionContext<Input, Output, Context>();
            _fasterKV.InitContext(_threadCtx.Value.prevCtx, guid.ToString());
            _threadCtx.Value.prevCtx.version--;
            _fasterKV.InternalRefresh(_threadCtx.Value, FasterSession);
            return guid;
        }

        private void InternalRelease(FasterKV<Key, Value>.FasterExecutionContext<Input, Output, Context> ctx)
        {
            Debug.Assert(ctx.HasNoPendingRequests);
            if (ctx.prevCtx != null)
            {
                Debug.Assert(ctx.prevCtx.HasNoPendingRequests);
            }
            Debug.Assert(ctx.phase == Phase.REST);

            _fasterKV.epoch.Suspend();
        }

        /// <inheritdoc />
        public void Dispose()
        {
            _fasterKV.Dispose();
            LegacyDispose();
        }

        /// <inheritdoc />
        public ClientSession<Key, Value, Input, Output, Context, Functions> NewSession(string sessionId = null, bool threadAffinitized = false)
            => _fasterKV.NewSession<Input, Output, Context, Functions>(_functions, sessionId, threadAffinitized);

        /// <inheritdoc />
        public ClientSession<Key, Value, Input, Output, Context, Functions> ResumeSession(string sessionId, out CommitPoint commitPoint, bool threadAffinitized = false)
            => _fasterKV.ResumeSession<Input, Output, Context, Functions>(_functions, sessionId, out commitPoint, threadAffinitized);

        /// <inheritdoc />
        public bool GrowIndex() => _fasterKV.GrowIndex();

        /// <inheritdoc />
        public bool TakeFullCheckpoint(out Guid token, long targetVersion = -1) => _fasterKV.TakeFullCheckpoint(out token, targetVersion);

        /// <inheritdoc />
        public bool TakeFullCheckpoint(out Guid token, CheckpointType checkpointType, long targetVersion = -1) => _fasterKV.TakeFullCheckpoint(out token, checkpointType, targetVersion);

        /// <inheritdoc />
        public bool TakeIndexCheckpoint(out Guid token) => _fasterKV.TakeIndexCheckpoint(out token);

        /// <inheritdoc />
        public bool TakeHybridLogCheckpoint(out Guid token, long targetVersion = -1) => _fasterKV.TakeHybridLogCheckpoint(out token, targetVersion);

        /// <inheritdoc />
        public bool TakeHybridLogCheckpoint(out Guid token, CheckpointType checkpointType, long targetVersion = -1) => _fasterKV.TakeHybridLogCheckpoint(out token, checkpointType, targetVersion);

        /// <inheritdoc />
        public void Recover() => _fasterKV.Recover();

        /// <inheritdoc />
        public void Recover(Guid fullcheckpointToken) => _fasterKV.Recover(fullcheckpointToken);

        /// <inheritdoc />
        public void Recover(Guid indexToken, Guid hybridLogToken) => _fasterKV.Recover(indexToken, hybridLogToken);

        /// <inheritdoc />
        public ValueTask CompleteCheckpointAsync(CancellationToken token = default(CancellationToken)) => _fasterKV.CompleteCheckpointAsync(token);

        /// <inheritdoc />
        public string DumpDistribution() => _fasterKV.DumpDistribution();

        internal SystemState SystemState => _fasterKV.SystemState;


        // This is a struct to allow JIT to inline calls (and bypass default interface call mechanism)
        private struct LegacyFasterSession : IFasterSession<Key, Value, Input, Output, Context>
        {
            private readonly FasterKV<Key, Value, Input, Output, Context, Functions> _fasterKV;

            public LegacyFasterSession(FasterKV<Key, Value, Input, Output, Context, Functions> fasterKV)
            {
                _fasterKV = fasterKV;
            }

            public void CheckpointCompletionCallback(string guid, CommitPoint commitPoint)
            {
                _fasterKV._functions.CheckpointCompletionCallback(guid, commitPoint);
            }

            public void ConcurrentReader(ref Key key, ref Input input, ref Value value, ref Output dst)
            {
                _fasterKV._functions.ConcurrentReader(ref key, ref input, ref value, ref dst);
            }

            public bool ConcurrentWriter(ref Key key, ref Value src, ref Value dst)
            {
                return _fasterKV._functions.ConcurrentWriter(ref key, ref src, ref dst);
            }

            public void CopyUpdater(ref Key key, ref Input input, ref Value oldValue, ref Value newValue)
            {
                _fasterKV._functions.CopyUpdater(ref key, ref input, ref oldValue, ref newValue);
            }

            public void DeleteCompletionCallback(ref Key key, Context ctx)
            {
                _fasterKV._functions.DeleteCompletionCallback(ref key, ctx);
            }

            public int GetInitialLength(ref Input input)
            {
                return _fasterKV._variableLengthStructForInput.GetInitialLength(ref input);
            }

            public int GetLength(ref Value t, ref Input input)
            {
                return _fasterKV._variableLengthStructForInput.GetLength(ref t, ref input);
            }

            public void InitialUpdater(ref Key key, ref Input input, ref Value value)
            {
                _fasterKV._functions.InitialUpdater(ref key, ref input, ref value);
            }

            public bool InPlaceUpdater(ref Key key, ref Input input, ref Value value)
            {
                return _fasterKV._functions.InPlaceUpdater(ref key, ref input, ref value);
            }

            public void ReadCompletionCallback(ref Key key, ref Input input, ref Output output, Context ctx, Status status)
            {
                _fasterKV._functions.ReadCompletionCallback(ref key, ref input, ref output, ctx, status);
            }

            public void RMWCompletionCallback(ref Key key, ref Input input, Context ctx, Status status)
            {
                _fasterKV._functions.RMWCompletionCallback(ref key, ref input, ctx, status);
            }

            public void SingleReader(ref Key key, ref Input input, ref Value value, ref Output dst)
            {
                _fasterKV._functions.SingleReader(ref key, ref input, ref value, ref dst);
            }

            public void SingleWriter(ref Key key, ref Value src, ref Value dst)
            {
                _fasterKV._functions.SingleWriter(ref key, ref src, ref dst);
            }

            public void UnsafeResumeThread()
            {
            }

            public void UnsafeSuspendThread()
            {
            }

            public void UpsertCompletionCallback(ref Key key, ref Value value, Context ctx)
            {
                _fasterKV._functions.UpsertCompletionCallback(ref key, ref value, ctx);
            }
        }
    }
}
