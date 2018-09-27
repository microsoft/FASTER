// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma warning disable 0162

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;

namespace FASTER.core
{
    public unsafe partial class FasterKV : FasterBase, IFasterKV
    { 
        public PersistentMemoryMalloc hlog;

        public static int numPendingReads = 0;

        private const bool kCopyReadsToTail = false;
        private const bool breakWhenClassIsLoaded = false;

        public long LogTailAddress => hlog.GetTailAddress();

        public long EntryCount => GetEntryCount();

        public enum CheckpointType
        {
            INDEX_ONLY,
            HYBRID_LOG_ONLY,
            FULL,
            NONE
        }

        protected CheckpointType _checkpointType;
        protected Guid _indexCheckpointToken;
        protected Guid _hybridLogCheckpointToken;
        protected SystemState _systemState;

        protected HybridLogCheckpointInfo _hybridLogCheckpoint;

        [ThreadStatic]
        protected static ExecutionContext prevThreadCtx = default(ExecutionContext);

        [ThreadStatic]
        protected static ExecutionContext threadCtx = default(ExecutionContext);


        static FasterKV()
        {
            if (breakWhenClassIsLoaded)
            {
                if (System.Diagnostics.Debugger.IsAttached)
                    System.Diagnostics.Debugger.Break();
                else
                    System.Diagnostics.Debugger.Launch();
            }
        }
        
        public FasterKV(long size, IDevice logDevice, IDevice objectLogDevice, string checkpointDir = null)
        {
            if (checkpointDir != null)
                Config.CheckpointDirectory = checkpointDir;

            hlog = new PersistentMemoryMalloc(logDevice, objectLogDevice);
            var recordSize = Layout.EstimatePhysicalSize(null, null);
            Initialize(size, hlog.GetSectorSize());

            _systemState = default(SystemState);
            _systemState.phase = Phase.REST;
            _systemState.version = 1;
            _checkpointType = CheckpointType.HYBRID_LOG_ONLY;
        }

        public bool TakeFullCheckpoint(out Guid token)
        {
            var success = InternalTakeCheckpoint(CheckpointType.FULL);
            if(success)
            {
                token = _indexCheckpointToken;
            }
            else
            {
                token = default(Guid);
            }
            return success;
        }

        public bool TakeIndexCheckpoint(out Guid token)
        {
            var success = InternalTakeCheckpoint(CheckpointType.INDEX_ONLY);
            if (success)
            {
                token = _indexCheckpointToken;
            }
            else
            {
                token = default(Guid);
            }
            return success;
        }

        public bool TakeHybridLogCheckpoint(out Guid token)
        {
            var success = InternalTakeCheckpoint(CheckpointType.HYBRID_LOG_ONLY);
            if (success)
            {
                token = _hybridLogCheckpointToken;
            }
            else
            {
                token = default(Guid);
            }
            return success;
        }

        public void Recover(Guid fullCheckpointToken)
        {
            InternalRecover(fullCheckpointToken, fullCheckpointToken);
        } 

        public void Recover(Guid indexCheckpointToken, Guid hybridLogCheckpointToken)
        {
            InternalRecover(indexCheckpointToken, hybridLogCheckpointToken);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Guid StartSession()
        {
            return InternalAcquire();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long ContinueSession(Guid guid)
        {
            return InternalContinue(guid);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void StopSession()
        {
            InternalRelease();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Refresh()
        {
            InternalRefresh();
        }

        

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool CompletePending(bool wait = false)
        {
            return InternalCompletePending(wait);
        }

        public bool CompleteCheckpoint(bool wait = false)
        {
            do
            {
                CompletePending();
                if (_systemState.phase == Phase.REST)
                    return true;
            } while (wait);
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Read(Key* key, Input* input, Output* output, Context* userContext, long monotonicSerialNum)
        {
            var context = default(PendingContext);
            var internalStatus = InternalRead(key, input, output, userContext, ref context);
            var status = default(Status);
            if (internalStatus == OperationStatus.SUCCESS || internalStatus == OperationStatus.NOTFOUND)
            {
                
                status = (Status)internalStatus;
            }
            else
            {
                status = HandleOperationStatus(threadCtx, context, internalStatus);
            }
            threadCtx.serialNum = monotonicSerialNum;
            return status;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Upsert(Key* key, Value* desiredValue, Context* userContext, long monotonicSerialNum)
        {
            var context = default(PendingContext);
            var internalStatus = InternalUpsert(key, desiredValue, userContext, ref context);
            var status = default(Status);

            if (internalStatus == OperationStatus.SUCCESS || internalStatus == OperationStatus.NOTFOUND)
            {
                status = (Status)internalStatus;
            }
            else
            {
                status = HandleOperationStatus(threadCtx, context, internalStatus);
            }
            threadCtx.serialNum = monotonicSerialNum;
            return status;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status RMW(Key* key, Input* input, Context* userContext, long monotonicSerialNum)
        {
            var context = default(PendingContext);
            var internalStatus = InternalRMW(key, input, userContext, ref context);
            var status = default(Status);
            if (internalStatus == OperationStatus.SUCCESS || internalStatus == OperationStatus.NOTFOUND)
            {
                 status = (Status)internalStatus;
            }
            else
            {
                status = HandleOperationStatus(threadCtx, context, internalStatus);
            }
            threadCtx.serialNum = monotonicSerialNum;
            return status;
        }

        /// <summary>
        /// Truncate the log until, but not including, untilAddress
        /// </summary>
        /// <param name="untilAddress"></param>
        public bool ShiftBeginAddress(long untilAddress)
        {
            return InternalShiftBeginAddress(untilAddress);
        }

        /// <summary>
        /// Grow the hash index
        /// </summary>
        /// <returns></returns>
        public bool GrowIndex()
        {
            return InternalGrowIndex();
        }

        
        internal bool GetLogicalAddress(Key* key, out long logicalAddress)
        {
            var bucket = default(HashBucket*);
            var slot = default(int);
            logicalAddress = Constants.kInvalidAddress;
            var physicalAddress = default(long);
            var info = default(RecordInfo*);

            var hash = Key.GetHashCode(key);
            var tag = (ushort)((ulong)hash >> Constants.kHashTagShift);

            var entry = default(HashBucketEntry);
            var tagExists = FindTag(hash, tag, ref bucket, ref slot, ref entry);

            if (tagExists)
            {
                logicalAddress = entry.word & Constants.kAddressMask;
                Debug.Assert(logicalAddress != 0);
                if (logicalAddress >= hlog.HeadAddress)
                {
                    physicalAddress = hlog.GetPhysicalAddress(logicalAddress);
                    if (!Key.Equals(key, Layout.GetKey(physicalAddress)))
                    {
                        logicalAddress = Layout.GetInfo(physicalAddress)->PreviousAddress;
                        TraceBackForKeyMatch(key, logicalAddress, hlog.HeadAddress, out logicalAddress, out physicalAddress);
                    }
                }
            }

            if (logicalAddress < hlog.HeadAddress && logicalAddress != Constants.kInvalidAddress)
            {
                return false;
            }
            return true;
        }

        public void Dispose()
        {
            hlog.Dispose();
        }
    }
}
