// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma warning disable 0162

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;

namespace FASTER.core
{
    public unsafe partial class FasterKV : FasterBase, IFasterKV, IPageHandlers
    {
        private PersistentMemoryMalloc hlog;

        private static int numPendingReads = 0;

        private const bool kCopyReadsToTail = false;
        private const bool breakWhenClassIsLoaded = false;

        /// <summary>
        /// Tail address of log
        /// </summary>
        public long LogTailAddress => hlog.GetTailAddress();

        /// <summary>
        /// Read-only address of log
        /// </summary>
        public long LogReadOnlyAddress => hlog.SafeReadOnlyAddress;

        /// <summary>
        /// Number of used entries in hash index
        /// </summary>
        public long EntryCount => GetEntryCount();

        private enum CheckpointType
        {
            INDEX_ONLY,
            HYBRID_LOG_ONLY,
            FULL,
            NONE
        }

        private CheckpointType _checkpointType;
        private Guid _indexCheckpointToken;
        private Guid _hybridLogCheckpointToken;
        private SystemState _systemState;

        private HybridLogCheckpointInfo _hybridLogCheckpoint;

        [ThreadStatic]
        private static ExecutionContext prevThreadCtx = default(ExecutionContext);

        [ThreadStatic]
        private static ExecutionContext threadCtx = default(ExecutionContext);


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

        /// <summary>
        /// Create FASTER instance
        /// </summary>
        /// <param name="size"></param>
        /// <param name="logDevice"></param>
        /// <param name="objectLogDevice"></param>
        /// <param name="checkpointDir"></param>
        public FasterKV(long size, IDevice logDevice, IDevice objectLogDevice, string checkpointDir = null)
        {
            if (checkpointDir != null)
                Config.CheckpointDirectory = checkpointDir;

            hlog = new PersistentMemoryMalloc(logDevice, objectLogDevice, this);
            var recordSize = Layout.EstimatePhysicalSize(null, null);
            Initialize(size, hlog.GetSectorSize());

            _systemState = default(SystemState);
            _systemState.phase = Phase.REST;
            _systemState.version = 1;
            _checkpointType = CheckpointType.HYBRID_LOG_ONLY;
        }


        /// <summary>
        /// Take full checkpoint
        /// </summary>
        /// <param name="token"></param>
        /// <returns></returns>
        public bool TakeFullCheckpoint(out Guid token)
        {
            var success = InternalTakeCheckpoint(CheckpointType.FULL);
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

        /// <summary>
        /// Take index checkpoint
        /// </summary>
        /// <param name="token"></param>
        /// <returns></returns>
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

        /// <summary>
        /// Take hybrid log checkpoint
        /// </summary>
        /// <param name="token"></param>
        /// <returns></returns>
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

        /// <summary>
        /// Recover
        /// </summary>
        /// <param name="fullCheckpointToken"></param>
        public void Recover(Guid fullCheckpointToken)
        {
            InternalRecover(fullCheckpointToken, fullCheckpointToken);
        }

        /// <summary>
        /// Recover
        /// </summary>
        /// <param name="indexCheckpointToken"></param>
        /// <param name="hybridLogCheckpointToken"></param>
        public void Recover(Guid indexCheckpointToken, Guid hybridLogCheckpointToken)
        {
            InternalRecover(indexCheckpointToken, hybridLogCheckpointToken);
        }

        /// <summary>
        /// Start session with FASTER
        /// </summary>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Guid StartSession()
        {
            return InternalAcquire();
        }


        /// <summary>
        /// Continue session with FASTER
        /// </summary>
        /// <param name="guid"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long ContinueSession(Guid guid)
        {
            return InternalContinue(guid);
        }

        /// <summary>
        /// Stop session with FASTER
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void StopSession()
        {
            InternalRelease();
        }

        /// <summary>
        /// Refresh epoch (release memory pins)
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Refresh()
        {
            InternalRefresh();
        }


        /// <summary>
        /// Complete outstanding pending operations
        /// </summary>
        /// <param name="wait"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool CompletePending(bool wait = false)
        {
            return InternalCompletePending(wait);
        }

        /// <summary>
        /// Complete the ongoing checkpoint (if any)
        /// </summary>
        /// <param name="wait"></param>
        /// <returns></returns>
        public bool CompleteCheckpoint(bool wait = false)
        {
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

        /// <summary>
        /// Read
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="userContext"></param>
        /// <param name="monotonicSerialNum"></param>
        /// <returns></returns>
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

        /// <summary>
        /// Upsert
        /// </summary>
        /// <param name="key"></param>
        /// <param name="desiredValue"></param>
        /// <param name="userContext"></param>
        /// <param name="monotonicSerialNum"></param>
        /// <returns></returns>
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

        /// <summary>
        /// Read-modify-write
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="userContext"></param>
        /// <param name="monotonicSerialNum"></param>
        /// <returns></returns>
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

        /// <summary>
        /// Dispose FASTER instance
        /// </summary>
        public void Dispose()
        {
            hlog.Dispose();
        }

        /// <summary>
        /// Clear page
        /// </summary>
        /// <param name="ptr">From pointer</param>
        /// <param name="endptr">Until pointer</param>
        public void ClearPage(long ptr, long endptr)
        {

            while (ptr < endptr)
            {
                if (!Layout.GetInfo(ptr)->Invalid)
                {
                    if (Key.HasObjectsToSerialize())
                    {
                        Key* key = Layout.GetKey(ptr);
                        Key.Free(key);
                    }
                    if (Value.HasObjectsToSerialize())
                    {
                        Value* value = Layout.GetValue(ptr);
                        Value.Free(value);
                    }
                }
                ptr += Layout.GetPhysicalSize(ptr);
            }
        }

        /// <summary>
        /// Deseialize part of page from stream
        /// </summary>
        /// <param name="ptr">From pointer</param>
        /// <param name="untilptr">Until pointer</param>
        /// <param name="stream">Stream</param>
        public void Deserialize(long ptr, long untilptr, Stream stream)
        {
            while (ptr < untilptr)
            {
                if (!Layout.GetInfo(ptr)->Invalid)
                {
                    if (Key.HasObjectsToSerialize())
                    {
                        Key.Deserialize(Layout.GetKey(ptr), stream);
                    }

                    if (Value.HasObjectsToSerialize())
                    {
                        Value.Deserialize(Layout.GetValue(ptr), stream);
                    }
                }
                ptr += Layout.GetPhysicalSize(ptr);
            }
        }

        /// <summary>
        /// Serialize part of page to stream
        /// </summary>
        /// <param name="ptr">From pointer</param>
        /// <param name="untilptr">Until pointer</param>
        /// <param name="stream">Stream</param>
        /// <param name="objectBlockSize">Size of blocks to serialize in chunks of</param>
        /// <param name="addr">List of addresses that need to be updated with offsets</param>
        public void Serialize(ref long ptr, long untilptr, Stream stream, int objectBlockSize, out List<long> addr)
        {
            addr = new List<long>();
            while (ptr < untilptr)
            {
                if (!Layout.GetInfo(ptr)->Invalid)
                {
                    long pos = stream.Position;

                    if (Key.HasObjectsToSerialize())
                    {
                        Key* key = Layout.GetKey(ptr);
                        Key.Serialize(key, stream);
                        ((AddressInfo*)key)->Address = pos;
                        ((AddressInfo*)key)->Size = (int)(stream.Position - pos);
                        addr.Add((long)key);
                    }

                    if (Value.HasObjectsToSerialize())
                    {
                        pos = stream.Position;
                        Value* value = Layout.GetValue(ptr);
                        Value.Serialize(value, stream);
                        ((AddressInfo*)value)->Address = pos;
                        ((AddressInfo*)value)->Size = (int)(stream.Position - pos);
                        addr.Add((long)value);
                    }

                }
                ptr += Layout.GetPhysicalSize(ptr);

                if (stream.Position > objectBlockSize)
                    return;
            }
        }

        /// <summary>
        /// Get location and range of object log addresses for specified log page
        /// </summary>
        /// <param name="ptr"></param>
        /// <param name="untilptr"></param>
        /// <param name="objectBlockSize"></param>
        /// <param name="startptr"></param>
        /// <param name="size"></param>
        public void GetObjectInfo(ref long ptr, long untilptr, int objectBlockSize, out long startptr, out long size)
        {
            long minObjAddress = long.MaxValue;
            long maxObjAddress = long.MinValue;

            while (ptr < untilptr)
            {
                if (!Layout.GetInfo(ptr)->Invalid)
                {

                    if (Key.HasObjectsToSerialize())
                    {
                        Key* key = Layout.GetKey(ptr);
                        var addr = ((AddressInfo*)key)->Address;

                        // If object pointer is greater than kObjectSize from starting object pointer
                        if (minObjAddress != long.MaxValue && (addr - minObjAddress > objectBlockSize))
                        {
                            break;
                        }

                        if (addr < minObjAddress) minObjAddress = addr;
                        addr += ((AddressInfo*)key)->Size;
                        if (addr > maxObjAddress) maxObjAddress = addr;
                    }


                    if (Value.HasObjectsToSerialize())
                    {
                        Value* value = Layout.GetValue(ptr);
                        var addr = ((AddressInfo*)value)->Address;

                        // If object pointer is greater than kObjectSize from starting object pointer
                        if (minObjAddress != long.MaxValue && (addr - minObjAddress > objectBlockSize))
                        {
                            break;
                        }

                        if (addr < minObjAddress) minObjAddress = addr;
                        addr += ((AddressInfo*)value)->Size;
                        if (addr > maxObjAddress) maxObjAddress = addr;
                    }
                }
                ptr += Layout.GetPhysicalSize(ptr);
            }

            // Handle the case where no objects are to be written
            if (minObjAddress == long.MaxValue && maxObjAddress == long.MinValue)
            {
                minObjAddress = 0;
                maxObjAddress = 0;
            }

            startptr = minObjAddress;
            size = maxObjAddress - minObjAddress;
        }

        /// <summary>
        /// Whether KVS has objects to serialize/deserialize
        /// </summary>
        /// <returns></returns>
        public bool HasObjects()
        {
            return Key.HasObjectsToSerialize() || Value.HasObjectsToSerialize();
        }
    }
}
