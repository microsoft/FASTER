// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma warning disable 0162

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

namespace FASTER.core
{
    
    public partial class FasterKV<Key, Value, Input, Output, Context, Functions> : FasterBase, IFasterKV<Key, Value, Input, Output, Context, Functions>
        where Key : new()
        where Value : new()
        where Functions : IFunctions<Key, Value, Input, Output, Context>
    {
        private FastThreadLocal<FasterExecutionContext> threadCtx;

        /// <summary>
        /// Dispose FASTER instance - legacy items
        /// </summary>
        private void LegacyDispose()
        {
            threadCtx?.Dispose();
        }

        private bool InLegacySession()
        {
            return threadCtx != null;
        }


        /// <summary>
        /// Legacy API: Start session with FASTER - call once per thread before using FASTER
        /// </summary>
        /// <returns></returns>
        [Obsolete("Use NewSession() instead.")]
        public Guid StartSession()
        {
            if (threadCtx == null)
                threadCtx = new FastThreadLocal<FasterExecutionContext>();

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

            var cp = InternalContinue(guid.ToString(), out FasterExecutionContext ctx);
            threadCtx.Value = ctx;

            return cp;
        }

        /// <summary>
        ///  Legacy API: Stop session with FASTER
        /// </summary>
        [Obsolete("Use and dispose NewSession() instead.")]
        public void StopSession()
        {
            InternalRelease(this.threadCtx.Value);
        }

        /// <summary>
        ///  Legacy API: Refresh epoch (release memory pins)
        /// </summary>
        [Obsolete("Use NewSession(), where Refresh() is not required by default.")]
        public void Refresh()
        {
            InternalRefresh(threadCtx.Value);
        }


        /// <summary>
        ///  Legacy API: Complete all pending operations issued by this session
        /// </summary>
        /// <param name="wait">Whether we spin-wait for pending operations to complete</param>
        /// <returns>Whether all pending operations have completed</returns>
        [Obsolete("Use NewSession() and invoke CompletePending() on the session.")]
        public bool CompletePending(bool wait = false)
        {
            return InternalCompletePending(threadCtx.Value, wait);
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
            return ContextRead(ref key, ref input, ref output, context, serialNo, threadCtx.Value);
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
            return ContextUpsert(ref key, ref value, context, serialNo, threadCtx.Value);
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
            return ContextRMW(ref key, ref input, context, serialNo, threadCtx.Value);
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
            return ContextDelete(ref key, context, serialNo, threadCtx.Value);
        }

        /// <summary>
        /// Legacy API: Complete the ongoing checkpoint (if any)
        /// </summary>
        /// <param name="wait">Spin-wait for completion</param>
        /// <returns></returns>
        [Obsolete("Use NewSession() and CompleteCheckpointAsync() instead.")]
        public bool CompleteCheckpoint(bool wait = false)
        {
            if (!InLegacySession())
            {
                CompleteCheckpointAsync().GetAwaiter().GetResult();
                return true;
            }

            // the thread has an active legacy session
            // so we need to constantly complete pending 
            // and refresh (done inside CompletePending)
            // for the checkpoint to be proceed
            do
            {
                CompletePending();
                if (_systemState.phase == Phase.REST)
                {
                    CompletePending();
                    return true;
                }
            } while (wait);

            return false;
        }

        private Guid InternalAcquire()
        {
            epoch.Resume();
            threadCtx.InitializeThread();
            Phase phase = _systemState.phase;
            if (phase != Phase.REST)
            {
                throw new FasterException("Can acquire only in REST phase!");
            }
            Guid guid = Guid.NewGuid();
            threadCtx.Value = new FasterExecutionContext();
            InitContext(threadCtx.Value, guid.ToString());

            threadCtx.Value.prevCtx = new FasterExecutionContext();
            InitContext(threadCtx.Value.prevCtx, guid.ToString());
            threadCtx.Value.prevCtx.version--;
            InternalRefresh(threadCtx.Value);
            return guid;
        }

        private void InternalRelease(FasterExecutionContext ctx)
        {
            Debug.Assert(ctx.retryRequests.Count == 0 && ctx.ioPendingRequests.Count == 0);
            if (ctx.prevCtx != null)
            {
                Debug.Assert(ctx.prevCtx.retryRequests.Count == 0 && ctx.prevCtx.ioPendingRequests.Count == 0);
            }
            Debug.Assert(ctx.phase == Phase.REST);

            epoch.Suspend();
        }
    }
}
