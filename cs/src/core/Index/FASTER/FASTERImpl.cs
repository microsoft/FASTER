// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma warning disable 0162
#define CPR

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;

namespace FASTER.core
{
    public unsafe partial class FasterKV<Key, Value, Input, Output, Context, Functions> : FasterBase, IPageHandlers, IFasterKV<Key, Value, Input, Output, Context>
        where Key : IKey<Key>
        where Value : IValue<Value>
        where Input : IMoveToContext<Input>
        where Output : IMoveToContext<Output>
        where Context : IMoveToContext<Context>
        where Functions : IFunctions<Key, Value, Input, Output, Context>
    {
        enum LatchOperation : byte
        {
            None,
            ReleaseShared,
            ReleaseExclusive
        }

        #region Read Operation

        /// <summary>
        /// Read operation. Computes the 'output' from 'input' and current value corresponding to 'key'.
        /// When the read operation goes pending, once the record is retrieved from disk, InternalContinuePendingRead
        /// function is used to complete the operation.
        /// </summary>
        /// <param name="key">Key of the record.</param>
        /// <param name="input">Input required to compute output from value.</param>
        /// <param name="output">Location to store output computed from input and value.</param>
        /// <param name="userContext">User context for the operation, in case it goes pending.</param>
        /// <param name="pendingContext">Pending context used internally to store the context of the operation.</param>
        /// <returns>
        /// <list type="table">
        ///     <listheader>
        ///     <term>Value</term>
        ///     <term>Description</term>
        ///     </listheader>
        ///     <item>
        ///     <term>SUCCESS</term>
        ///     <term>The output has been computed using current value of 'key' and 'input'; and stored in 'output'.</term>
        ///     </item>
        ///     <item>
        ///     <term>RECORD_ON_DISK</term>
        ///     <term>The record corresponding to 'key' is on disk and the operation.</term>
        ///     </item>
        ///     <item>
        ///     <term>CPR_SHIFT_DETECTED</term>
        ///     <term>A shift in version has been detected. Synchronize immediately to avoid violating CPR consistency.</term>
        ///     </item>
        /// </list>
        /// </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal OperationStatus InternalRead(
                                    ref Key key, 
                                    ref Input input, 
                                    ref Output output, 
                                    ref Context userContext,
                                    ref PendingContext pendingContext)
        {
            var status = default(OperationStatus);
            var bucket = default(HashBucket*);
            var slot = default(int);
            var logicalAddress = Constants.kInvalidAddress;
            var latestRecordVersion = -1;

            var hash = key.GetHashCode64();
            var tag = (ushort)((ulong)hash >> Constants.kHashTagShift);

            if (threadCtx.phase != Phase.REST)
                HeavyEnter(hash);

            #region Trace back for record in in-memory HybridLog
            HashBucketEntry entry = default(HashBucketEntry);
            var tagExists = FindTag(hash, tag, ref bucket, ref slot, ref entry);
            if (tagExists)
            {
                logicalAddress = entry.Address;
                if (logicalAddress >= hlog.HeadAddress)
                {
                    latestRecordVersion = hlog.GetInfo(logicalAddress).Version;
                    if (!key.Equals(ref hlog.GetKey(logicalAddress)))
                    {
                        logicalAddress = hlog.GetInfo(logicalAddress).PreviousAddress;
                        TraceBackForKeyMatch(ref key, 
                                             logicalAddress, 
                                             hlog.HeadAddress, 
                                             out logicalAddress);
                    }
                }
            }
            else
            {
                // no tag found
                return OperationStatus.NOTFOUND;
            }
            #endregion

            if (threadCtx.phase != Phase.REST)
            {
                switch(threadCtx.phase)
                {
                    case Phase.PREPARE:
                        {
                            if (latestRecordVersion != -1 && latestRecordVersion > threadCtx.version)
                            {
                                status = OperationStatus.CPR_SHIFT_DETECTED;
                                goto CreatePendingContext; // Pivot thread
                            }
                            break; // Normal processing
                        }
                    case Phase.GC:
                        {
                            GarbageCollectBuckets(hash);
                            break;
                        }
                    default:
                        {
                            break;
                        }
                }
            }

            #region Normal processing

            // Mutable region (even fuzzy region is included here)
            if (logicalAddress >= hlog.SafeReadOnlyAddress)
            {
                functions.ConcurrentReader(ref key, ref input, ref hlog.GetValue(logicalAddress), ref output);
                return OperationStatus.SUCCESS;
            }

            // Immutable region
            else if (logicalAddress >= hlog.HeadAddress)
            {
                functions.SingleReader(ref key, ref input, ref hlog.GetValue(logicalAddress), ref output);
                return OperationStatus.SUCCESS;
            }

            // On-Disk Region
            else if (logicalAddress >= hlog.BeginAddress)
            {
                status = OperationStatus.RECORD_ON_DISK;

                if (threadCtx.phase == Phase.PREPARE)
                {
                    if(! HashBucket.TryAcquireSharedLatch(bucket))
                    {
                        status = OperationStatus.CPR_SHIFT_DETECTED;
                    }
                }

                goto CreatePendingContext;
            }

            // No record found
            else
            {
                return OperationStatus.NOTFOUND;
            }

            #endregion

            #region Create pending context
            CreatePendingContext:
            {
                pendingContext.type = OperationType.READ;
                pendingContext.key = key.MoveToContext(ref key);
                pendingContext.input = input.MoveToContext(ref input);
                pendingContext.output = output.MoveToContext(ref output);
                pendingContext.userContext = userContext.MoveToContext(ref userContext);
                pendingContext.entry.word = entry.word;
                pendingContext.logicalAddress = logicalAddress;
                pendingContext.version = threadCtx.version;
                pendingContext.serialNum = threadCtx.serialNum + 1;
            }
            #endregion

            return status;
        }

        /// <summary>
        /// Continue a pending read operation. Computes 'output' from 'input' and value corresponding to 'key'
        /// obtained from disk. Optionally, it copies the value to tail to serve future read/write requests quickly.
        /// </summary>
        /// <param name="ctx">The thread (or session) context to execute operation in.</param>
        /// <param name="request">Async response from disk.</param>
        /// <param name="pendingContext">Pending context corresponding to operation.</param>
        /// <returns>
        /// <list type = "table" >
        ///     <listheader>
        ///     <term>Value</term>
        ///     <term>Description</term>
        ///     </listheader>
        ///     <item>
        ///     <term>SUCCESS</term>
        ///     <term>The output has been computed and stored in 'output'.</term>
        ///     </item>
        /// </list>
        /// </returns>
        internal OperationStatus InternalContinuePendingRead(
                            ExecutionContext ctx,
                            AsyncIOContext<Key> request,
                            ref PendingContext pendingContext)
        {
            Debug.Assert(pendingContext.version == ctx.version);

            if (request.logicalAddress >= hlog.BeginAddress)
            {
                var physicalAddress = (long)request.record.GetValidPointer();
                Debug.Assert(hlog.GetInfoFromPhysical(physicalAddress).Version <= ctx.version);
                functions.SingleReader(ref pendingContext.key,
                                       ref pendingContext.input,
                                       ref hlog.GetValueFromPhysical(physicalAddress),
                                       ref pendingContext.output);

                if (kCopyReadsToTail)
                {
                    InternalContinuePendingReadCopyToTail(ctx, request, ref pendingContext);
                }
            }
            else
                return OperationStatus.NOTFOUND;

            return OperationStatus.SUCCESS;
        }

        /// <summary>
        /// Copies the record read from disk to tail of the HybridLog. 
        /// </summary>
        /// <param name="ctx"> The thread(or session) context to execute operation in.</param>
        /// <param name="request">Async response from disk.</param>
        /// <param name="pendingContext">Pending context corresponding to operation.</param>
        internal void InternalContinuePendingReadCopyToTail(
                                    ExecutionContext ctx,
                                    AsyncIOContext<Key> request,
                                    ref PendingContext pendingContext)
        {
            Debug.Assert(pendingContext.version == ctx.version);

            var recordSize = default(int);
            var bucket = default(HashBucket*);
            var slot = default(int);
            var logicalAddress = Constants.kInvalidAddress;

            var hash = pendingContext.key.GetHashCode64();
            var tag = (ushort)((ulong)hash >> Constants.kHashTagShift);

            #region Trace back record in in-memory HybridLog
            var entry = default(HashBucketEntry);
            FindOrCreateTag(hash, tag, ref bucket, ref slot, ref entry);
            logicalAddress = entry.word & Constants.kAddressMask;
            if (logicalAddress >= hlog.HeadAddress)
            {
                if (!pendingContext.key.Equals(ref hlog.GetKey(logicalAddress)))
                {
                    logicalAddress = hlog.GetInfo(logicalAddress).PreviousAddress;
                    TraceBackForKeyMatch(ref pendingContext.key,
                                            logicalAddress,
                                            hlog.HeadAddress,
                                            out logicalAddress);
                }
            }
            #endregion

            if (logicalAddress > pendingContext.entry.Address)
            {
                // Give up early
                return;
            }

            #region Create new copy in mutable region
            var physicalAddress = (long)request.record.GetValidPointer();
            recordSize = hlog.GetRecordSize(physicalAddress);
            BlockAllocate(recordSize, out long newLogicalAddress);
            ref RecordInfo recordInfo = ref hlog.GetInfo(newLogicalAddress);
            RecordInfo.WriteInfo(ref recordInfo, ctx.version,
                                 true, false, false,
                                 entry.Address);
            request.key.ShallowCopy(ref hlog.GetKey(newLogicalAddress));
            functions.SingleWriter(ref request.key,
                                   ref hlog.GetValue(logicalAddress),
                                   ref hlog.GetValue(newLogicalAddress));

            var updatedEntry = default(HashBucketEntry);
            updatedEntry.Tag = tag;
            updatedEntry.Address = newLogicalAddress & Constants.kAddressMask;
            updatedEntry.Pending = entry.Pending;
            updatedEntry.Tentative = false;

            var foundEntry = default(HashBucketEntry);
            foundEntry.word = Interlocked.CompareExchange(
                                            ref bucket->bucket_entries[slot],
                                            updatedEntry.word,
                                            entry.word);
            if (foundEntry.word != entry.word)
            {
                hlog.GetInfo(newLogicalAddress).Invalid = true;
                // We don't retry, just give up
            }
            #endregion
        }

        #endregion

        #region Upsert Operation

        /// <summary>
        /// Upsert operation. Replaces the value corresponding to 'key' with provided 'value', if one exists 
        /// else inserts a new record with 'key' and 'value'.
        /// </summary>
        /// <param name="key">key of the record.</param>
        /// <param name="value">value to be updated to (or inserted if key does not exist).</param>
        /// <param name="userContext">User context for the operation, in case it goes pending.</param>
        /// <param name="pendingContext">Pending context used internally to store the context of the operation.</param>
        /// <returns>
        /// <list type="table">
        ///     <listheader>
        ///     <term>Value</term>
        ///     <term>Description</term>
        ///     </listheader>
        ///     <item>
        ///     <term>SUCCESS</term>
        ///     <term>The value has been successfully replaced(or inserted)</term>
        ///     </item>
        ///     <item>
        ///     <term>RETRY_LATER</term>
        ///     <term>Cannot  be processed immediately due to system state. Add to pending list and retry later</term>
        ///     </item>
        ///     <item>
        ///     <term>CPR_SHIFT_DETECTED</term>
        ///     <term>A shift in version has been detected. Synchronize immediately to avoid violating CPR consistency.</term>
        ///     </item>
        /// </list>
        /// </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal OperationStatus InternalUpsert(
                            ref Key key, ref Value value,
                            ref Context userContext,
                            ref PendingContext pendingContext)
        {
            var status = default(OperationStatus);
            var bucket = default(HashBucket*);
            var slot = default(int);
            var logicalAddress = Constants.kInvalidAddress;
            var latchOperation = default(LatchOperation);
            var version = default(int);
            var latestRecordVersion = -1;

            var hash = key.GetHashCode64();
            var tag = (ushort)((ulong)hash >> Constants.kHashTagShift);

            if (threadCtx.phase != Phase.REST)
                HeavyEnter(hash);

            #region Trace back for record in in-memory HybridLog
            var entry = default(HashBucketEntry);
            FindOrCreateTag(hash, tag, ref bucket, ref slot, ref entry);
            logicalAddress = entry.Address;
            if (logicalAddress >= hlog.ReadOnlyAddress)
            {
                latestRecordVersion = hlog.GetInfo(logicalAddress).Version;
                if (!key.Equals(ref hlog.GetKey(logicalAddress)))
                {
                    logicalAddress = hlog.GetInfo(logicalAddress).PreviousAddress;
                    TraceBackForKeyMatch(ref key,
                                        logicalAddress,
                                        hlog.ReadOnlyAddress,
                                        out logicalAddress);
                }
            }
            #endregion

            // Optimization for most common case
            if (threadCtx.phase == Phase.REST && logicalAddress >= hlog.ReadOnlyAddress)
            {
                functions.ConcurrentWriter(ref key, ref value, ref hlog.GetValue(logicalAddress));
                return OperationStatus.SUCCESS;
            }

            #region Entry latch operation
            if (threadCtx.phase != Phase.REST)
            {
                switch (threadCtx.phase)
                {
                    case Phase.PREPARE:
                        {
                            version = threadCtx.version;
                            if (HashBucket.TryAcquireSharedLatch(bucket))
                            {
                                // Set to release shared latch (default)
                                latchOperation = LatchOperation.ReleaseShared;
                                if (latestRecordVersion != -1 && latestRecordVersion > version)
                                {
                                    status = OperationStatus.CPR_SHIFT_DETECTED;
                                    goto CreatePendingContext; // Pivot Thread
                                }
                                break; // Normal Processing
                            }
                            else
                            {
                                status = OperationStatus.CPR_SHIFT_DETECTED;
                                goto CreatePendingContext; // Pivot Thread
                            }
                        }
                    case Phase.IN_PROGRESS:
                        {
                            version = (threadCtx.version - 1);
                            if (latestRecordVersion != -1 && latestRecordVersion <= version)
                            {
                                if (HashBucket.TryAcquireExclusiveLatch(bucket))
                                {
                                    // Set to release exclusive latch (default)
                                    latchOperation = LatchOperation.ReleaseExclusive;
                                    goto CreateNewRecord; // Create a (v+1) record
                                }
                                else
                                {
                                    status = OperationStatus.RETRY_LATER;
                                    goto CreatePendingContext; // Go Pending
                                }
                            }
                            break; // Normal Processing
                        }
                    case Phase.WAIT_PENDING:
                        {
                            version = (threadCtx.version - 1);
                            if (latestRecordVersion != -1 && latestRecordVersion <= version)
                            {
                                if (HashBucket.NoSharedLatches(bucket))
                                {
                                    goto CreateNewRecord; // Create a (v+1) record
                                }
                                else
                                {
                                    status = OperationStatus.RETRY_LATER;
                                    goto CreatePendingContext; // Go Pending
                                }
                            }
                            break; // Normal Processing
                        }
                    case Phase.WAIT_FLUSH:
                        {
                            version = (threadCtx.version - 1);
                            if (latestRecordVersion != -1 && latestRecordVersion <= version)
                            {
                                goto CreateNewRecord; // Create a (v+1) record
                            }
                            break; // Normal Processing
                        }
                    default:
                        break;
                }
            }
            #endregion

            Debug.Assert(latestRecordVersion <= threadCtx.version);

            #region Normal processing

            // Mutable Region: Update the record in-place
            if (logicalAddress >= hlog.ReadOnlyAddress)
            {
                functions.ConcurrentWriter(ref key, ref value, ref hlog.GetValue(logicalAddress));
                status = OperationStatus.SUCCESS;
                goto LatchRelease; // Release shared latch (if acquired)
            }

            // All other regions: Create a record in the mutable region
            #endregion

            #region Create new record in the mutable region
            CreateNewRecord:
            {
                // Immutable region or new record
                var recordSize = GetRecordSize(ref key, ref value);
                BlockAllocate(recordSize, out long newLogicalAddress);
                RecordInfo.WriteInfo(ref hlog.GetInfo(newLogicalAddress),
                                        threadCtx.version,
                                        true, false, false,
                                        entry.Address);
                key.ShallowCopy(ref hlog.GetKey(newLogicalAddress));
                functions.SingleWriter(ref key, ref value,
                                        ref hlog.GetValue(newLogicalAddress));

                var updatedEntry = default(HashBucketEntry);
                updatedEntry.Tag = tag;
                updatedEntry.Address = newLogicalAddress & Constants.kAddressMask;
                updatedEntry.Pending = entry.Pending;
                updatedEntry.Tentative = false;

                var foundEntry = default(HashBucketEntry);
                foundEntry.word = Interlocked.CompareExchange(
                                        ref bucket->bucket_entries[slot],
                                        updatedEntry.word, entry.word);

                if (foundEntry.word == entry.word)
                {
                    status = OperationStatus.SUCCESS;
                    goto LatchRelease;
                }
                else
                {
                    hlog.GetInfo(newLogicalAddress).Invalid = true;
                    status = OperationStatus.RETRY_NOW;
                    goto LatchRelease;
                }
            }
            #endregion

            #region Create pending context
            CreatePendingContext:
            {
                pendingContext.type = OperationType.UPSERT;
                pendingContext.key = key.MoveToContext(ref key);
                pendingContext.value = value.MoveToContext(ref value);
                pendingContext.userContext = userContext.MoveToContext(ref userContext);
                pendingContext.entry.word = entry.word;
                pendingContext.logicalAddress = logicalAddress;
                pendingContext.version = threadCtx.version;
                pendingContext.serialNum = threadCtx.serialNum + 1;
            }
            #endregion

            #region Latch release
            LatchRelease:
            {
                switch (latchOperation)
                {
                    case LatchOperation.ReleaseShared:
                        HashBucket.ReleaseSharedLatch(bucket);
                        break;
                    case LatchOperation.ReleaseExclusive:
                        HashBucket.ReleaseExclusiveLatch(bucket);
                        break;
                    default:
                        break;
                }
            }
            #endregion

            if(status == OperationStatus.RETRY_NOW)
            {
                return InternalUpsert(ref key, ref value, ref userContext, ref pendingContext);
            }
            else
            {
                return status;
            }
        }

        #endregion

        #region RMW Operation

        /// <summary>
        /// Read-Modify-Write Operation. Updates value of 'key' using 'input' and current value.
        /// Pending operations are processed either using InternalRetryPendingRMW or 
        /// InternalContinuePendingRMW.
        /// </summary>
        /// <param name="key">key of the record.</param>
        /// <param name="input">input used to update the value.</param>
        /// <param name="userContext">user context corresponding to operation used during completion callback.</param>
        /// <param name="pendingContext">pending context created when the operation goes pending.</param>
        /// <returns>
        /// <list type="table">
        ///     <listheader>
        ///     <term>Value</term>
        ///     <term>Description</term>
        ///     </listheader>
        ///     <item>
        ///     <term>SUCCESS</term>
        ///     <term>The value has been successfully updated(or inserted).</term>
        ///     </item>
        ///     <item>
        ///     <term>RECORD_ON_DISK</term>
        ///     <term>The record corresponding to 'key' is on disk. Issue async IO to retrieve record and retry later.</term>
        ///     </item>
        ///     <item>
        ///     <term>RETRY_LATER</term>
        ///     <term>Cannot  be processed immediately due to system state. Add to pending list and retry later.</term>
        ///     </item>
        ///     <item>
        ///     <term>CPR_SHIFT_DETECTED</term>
        ///     <term>A shift in version has been detected. Synchronize immediately to avoid violating CPR consistency.</term>
        ///     </item>
        /// </list>
        /// </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal OperationStatus InternalRMW(
                                   ref Key key, ref Input input,
                                   ref Context userContext,
                                   ref PendingContext pendingContext)
        {
            var recordSize = default(int);
            var bucket = default(HashBucket*);
            var slot = default(int);
            var logicalAddress = Constants.kInvalidAddress;
            var version = default(int);
            var latestRecordVersion = -1;
            var status = default(OperationStatus);
            var latchOperation = LatchOperation.None;

            var hash = key.GetHashCode64();
            var tag = (ushort)((ulong)hash >> Constants.kHashTagShift);

            if (threadCtx.phase != Phase.REST)
                HeavyEnter(hash);

            #region Trace back for record in in-memory HybridLog
            var entry = default(HashBucketEntry);
            FindOrCreateTag(hash, tag, ref bucket, ref slot, ref entry);
            logicalAddress = entry.Address;
            if (logicalAddress >= hlog.HeadAddress)
            {
                latestRecordVersion = hlog.GetInfo(logicalAddress).Version;
                if (!key.Equals(ref hlog.GetKey(logicalAddress)))
                {
                    logicalAddress = hlog.GetInfo(logicalAddress).PreviousAddress;
                    TraceBackForKeyMatch(ref key, logicalAddress,
                                            hlog.HeadAddress,
                                            out logicalAddress);
                }
            }
            #endregion

            // Optimization for the most common case
            if (threadCtx.phase == Phase.REST && logicalAddress >= hlog.ReadOnlyAddress)
            {
                functions.InPlaceUpdater(ref key, ref input, ref hlog.GetValue(logicalAddress));
                return OperationStatus.SUCCESS;
            }

            #region Entry latch operation
            if (threadCtx.phase != Phase.REST)
            {
                switch (threadCtx.phase)
                {
                    case Phase.PREPARE:
                        {
                            version = threadCtx.version;
                            if (HashBucket.TryAcquireSharedLatch(bucket))
                            {
                                // Set to release shared latch (default)
                                latchOperation = LatchOperation.ReleaseShared;
                                if (latestRecordVersion != -1 && latestRecordVersion > version)
                                {
                                    status = OperationStatus.CPR_SHIFT_DETECTED;
                                    goto CreateFailureContext; // Pivot Thread
                                }
                                break; // Normal Processing
                            }
                            else
                            {
                                status = OperationStatus.CPR_SHIFT_DETECTED;
                                goto CreateFailureContext; // Pivot Thread
                            }
                        }
                    case Phase.IN_PROGRESS:
                        {
                            version = (threadCtx.version - 1);
                            if (latestRecordVersion <= version)
                            {
                                if (HashBucket.TryAcquireExclusiveLatch(bucket))
                                {
                                    // Set to release exclusive latch (default)
                                    latchOperation = LatchOperation.ReleaseExclusive;
                                    goto CreateNewRecord; // Create a (v+1) record
                                }
                                else
                                {
                                    status = OperationStatus.RETRY_LATER;
                                    goto CreateFailureContext; // Go Pending
                                }
                            }
                            break; // Normal Processing
                        }
                    case Phase.WAIT_PENDING:
                        {
                            version = (threadCtx.version - 1);
                            if (latestRecordVersion != -1 && latestRecordVersion <= version)
                            {
                                if (HashBucket.NoSharedLatches(bucket))
                                {
                                    goto CreateNewRecord; // Create a (v+1) record
                                }
                                else
                                {
                                    status = OperationStatus.RETRY_LATER;
                                    goto CreateFailureContext; // Go Pending
                                }
                            }
                            break; // Normal Processing
                        }
                    case Phase.WAIT_FLUSH:
                        {
                            version = (threadCtx.version - 1);
                            if (latestRecordVersion != -1 && latestRecordVersion <= version)
                            {
                                goto CreateNewRecord; // Create a (v+1) record
                            }
                            break; // Normal Processing
                        }
                    default:
                        break;
                }
            }
            #endregion

            Debug.Assert(latestRecordVersion <= threadCtx.version);

            #region Normal processing

            // Mutable Region: Update the record in-place
            if (logicalAddress >= hlog.ReadOnlyAddress)
            {
                if(FoldOverSnapshot)
                {
                    Debug.Assert(hlog.GetInfo(logicalAddress).Version == threadCtx.version);
                }
                functions.InPlaceUpdater(ref key, ref input, ref hlog.GetValue(logicalAddress));
                status = OperationStatus.SUCCESS;
                goto LatchRelease; // Release shared latch (if acquired)
            }

            // Fuzzy Region: Must go pending due to lost-update anomaly
            else if (logicalAddress >= hlog.SafeReadOnlyAddress)
            {
                status = OperationStatus.RETRY_LATER;
                // Retain the shared latch (if acquired)
                if (latchOperation == LatchOperation.ReleaseShared)
                {
                    latchOperation = LatchOperation.None;
                }
                goto CreateFailureContext; // Go pending
            }

            // Safe Read-Only Region: Create a record in the mutable region
            else if (logicalAddress >= hlog.HeadAddress)
            {
                goto CreateNewRecord; 
            }

            // Disk Region: Need to issue async io requests
            else if (logicalAddress >= hlog.BeginAddress)
            {
                status = OperationStatus.RECORD_ON_DISK;
                // Retain the shared latch (if acquired)
                if (latchOperation == LatchOperation.ReleaseShared)
                {
                    latchOperation = LatchOperation.None;
                }
                goto CreateFailureContext; // Go pending
            }

            // No record exists - create new
            else
            {
                goto CreateNewRecord; 
            }

            #endregion

            #region Create new record
            CreateNewRecord:
            {
                recordSize = (logicalAddress < hlog.BeginAddress) ?
                                hlog.GetInitialRecordSize(ref key, functions.InitialValueLength(ref key, ref input)) :
                                hlog.GetRecordSizeFromLogical(logicalAddress);
                BlockAllocate(recordSize, out long newLogicalAddress);
                ref RecordInfo recordInfo = ref hlog.GetInfo(newLogicalAddress);
                RecordInfo.WriteInfo(ref recordInfo, threadCtx.version,
                                        true, false, false,
                                        entry.Address);
                key.ShallowCopy(ref hlog.GetKey(newLogicalAddress));
                if (logicalAddress < hlog.BeginAddress)
                {
                    functions.InitialUpdater(ref key, ref input, ref hlog.GetValue(newLogicalAddress));
                    status = OperationStatus.NOTFOUND;
                }
                else if (logicalAddress >= hlog.HeadAddress)
                {
                    functions.CopyUpdater(ref key, ref input,
                                            ref hlog.GetValue(logicalAddress),
                                            ref hlog.GetValue(newLogicalAddress));
                    status = OperationStatus.SUCCESS;
                }
                else
                {
                    // ah, old record slipped onto disk
                    hlog.GetInfo(newLogicalAddress).Invalid = true;
                    status = OperationStatus.RETRY_NOW;
                    goto LatchRelease;
                }

                var updatedEntry = default(HashBucketEntry);
                updatedEntry.Tag = tag;
                updatedEntry.Address = newLogicalAddress & Constants.kAddressMask;
                updatedEntry.Pending = entry.Pending;
                updatedEntry.Tentative = false;

                var foundEntry = default(HashBucketEntry);
                foundEntry.word = Interlocked.CompareExchange(
                                        ref bucket->bucket_entries[slot],
                                        updatedEntry.word, entry.word);

                if (foundEntry.word == entry.word)
                {
                    goto LatchRelease;
                }
                else
                {
                    // ah, CAS failed
                    hlog.GetInfo(newLogicalAddress).Invalid = true;
                    status = OperationStatus.RETRY_NOW;
                    goto LatchRelease;
                }
            }
            #endregion

            #region Create failure context
            CreateFailureContext:
            {
                pendingContext.type = OperationType.RMW;
                pendingContext.key = key.MoveToContext(ref key);
                pendingContext.input = input.MoveToContext(ref input);
                pendingContext.userContext = userContext.MoveToContext(ref userContext);
                pendingContext.entry.word = entry.word;
                pendingContext.logicalAddress = logicalAddress;
                pendingContext.version = threadCtx.version;
                pendingContext.serialNum = threadCtx.serialNum + 1;
            }
            #endregion

            #region Latch release
            LatchRelease:
            {
                switch (latchOperation)
                {
                    case LatchOperation.ReleaseShared:
                        HashBucket.ReleaseSharedLatch(bucket);
                        break;
                    case LatchOperation.ReleaseExclusive:
                        HashBucket.ReleaseExclusiveLatch(bucket);
                        break;
                    default:
                        break;
                }
            }
            #endregion

            if(status == OperationStatus.RETRY_NOW)
            {
                return InternalRMW(ref key, ref input, ref userContext, ref pendingContext);
            }
            else
            {
                return status;
            }
        }

        /// <summary>
        /// Retries a pending RMW operation. 
        /// </summary>
        /// <param name="ctx">Thread (or session) context under which operation must be executed.</param>
        /// <param name="pendingContext">Internal context of the RMW operation.</param>
        /// <returns>
        /// <list type="table">
        ///     <listheader>
        ///     <term>Value</term>
        ///     <term>Description</term>
        ///     </listheader>
        ///     <item>
        ///     <term>SUCCESS</term>
        ///     <term>The value has been successfully updated(or inserted).</term>
        ///     </item>
        ///     <item>
        ///     <term>RECORD_ON_DISK</term>
        ///     <term>The record corresponding to 'key' is on disk. Issue async IO to retrieve record and retry later.</term>
        ///     </item>
        ///     <item>
        ///     <term>RETRY_LATER</term>
        ///     <term>Cannot  be processed immediately due to system state. Add to pending list and retry later.</term>
        ///     </item>
        /// </list>
        /// </returns>
        internal OperationStatus InternalRetryPendingRMW(
                            ExecutionContext ctx,
                            ref PendingContext pendingContext)
        {
            var recordSize = default(int);
            var bucket = default(HashBucket*);
            var slot = default(int);
            var logicalAddress = Constants.kInvalidAddress;
            var version = default(int);
            var latestRecordVersion = -1;
            var status = default(OperationStatus);
            var latchOperation = LatchOperation.None;
            var key = pendingContext.key;

            var hash = key.GetHashCode64();
            var tag = (ushort)((ulong)hash >> Constants.kHashTagShift);

            if (threadCtx.phase != Phase.REST)
                HeavyEnter(hash);

            #region Trace back for record in in-memory HybridLog
            var entry = default(HashBucketEntry);
            FindOrCreateTag(hash, tag, ref bucket, ref slot, ref entry);
            logicalAddress = entry.Address;
            if (logicalAddress >= hlog.HeadAddress)
            {
                latestRecordVersion = hlog.GetInfo(logicalAddress).Version;
                if (!key.Equals(ref hlog.GetKey(logicalAddress)))
                {
                    logicalAddress = hlog.GetInfo(logicalAddress).PreviousAddress;
                    TraceBackForKeyMatch(ref key, logicalAddress,
                                            hlog.HeadAddress,
                                            out logicalAddress);
                }
            }
            #endregion

            #region Entry latch operation
            if (threadCtx.phase != Phase.REST)
            {
                if (!((ctx.version < threadCtx.version) 
                      ||
                      (threadCtx.phase == Phase.PREPARE))) 
                {
                    // Processing a pending (v+1) request
                    version = (threadCtx.version - 1);
                    switch (threadCtx.phase)
                    {
                        case Phase.IN_PROGRESS:
                            {
                                if (latestRecordVersion != -1 && latestRecordVersion <= version)
                                {
                                    if (HashBucket.TryAcquireExclusiveLatch(bucket))
                                    {
                                        // Set to release exclusive latch (default)
                                        latchOperation = LatchOperation.ReleaseExclusive;
                                        goto CreateNewRecord; // Create a (v+1) record
                                    }
                                    else
                                    {
                                        status = OperationStatus.RETRY_LATER;
                                        goto UpdateFailureContext; // Go Pending
                                    }
                                }
                                break; // Normal Processing
                            }
                        case Phase.WAIT_PENDING:
                            {
                                if (latestRecordVersion != -1 && latestRecordVersion <= version)
                                {
                                    if (HashBucket.NoSharedLatches(bucket))
                                    {
                                        goto CreateNewRecord; // Create a (v+1) record
                                    }
                                    else
                                    {
                                        status = OperationStatus.RETRY_LATER;
                                        goto UpdateFailureContext; // Go Pending
                                    }
                                }
                                break; // Normal Processing
                            }
                        case Phase.WAIT_FLUSH:
                            {
                                if (latestRecordVersion != -1 && latestRecordVersion <= version)
                                {
                                    goto CreateNewRecord; // Create a (v+1) record
                                }
                                break; // Normal Processing
                            }
                        default:
                            break;
                    }
                }
            }
            #endregion

            #region Normal processing

            // Mutable Region: Update the record in-place
            if (logicalAddress >= hlog.ReadOnlyAddress)
            {
                if (FoldOverSnapshot)
                {
                    Debug.Assert(hlog.GetInfo(logicalAddress).Version == threadCtx.version);
                }
                functions.InPlaceUpdater(ref pendingContext.key, ref pendingContext.input, ref hlog.GetValue(logicalAddress));
                status = OperationStatus.SUCCESS;
                goto LatchRelease; 
            }

            // Fuzzy Region: Must go pending due to lost-update anomaly
            else if (logicalAddress >= hlog.SafeReadOnlyAddress)
            {
                status = OperationStatus.RETRY_LATER;
                goto UpdateFailureContext; // Go pending
            }

            // Safe Read-Only Region: Create a record in the mutable region
            else if (logicalAddress >= hlog.HeadAddress)
            {
                goto CreateNewRecord;
            }

            // Disk Region: Need to issue async io requests
            else if (logicalAddress >= hlog.BeginAddress)
            {
                status = OperationStatus.RECORD_ON_DISK;
                goto UpdateFailureContext; // Go pending
            }

            // No record exists - create new
            else
            {
                goto CreateNewRecord;
            }

            #endregion

            #region Create new record in mutable region
            CreateNewRecord:
            {
                recordSize = (logicalAddress < hlog.BeginAddress) ?
                                hlog.GetInitialRecordSize(ref pendingContext.key, functions.InitialValueLength(ref key, ref pendingContext.input)) :
                                hlog.GetRecordSizeFromLogical(logicalAddress);
                BlockAllocate(recordSize, out long newLogicalAddress);
                ref RecordInfo recordInfo = ref hlog.GetInfo(newLogicalAddress);
                RecordInfo.WriteInfo(ref recordInfo, pendingContext.version,
                                        true, false, false,
                                        entry.Address);
                key.ShallowCopy(ref hlog.GetKey(newLogicalAddress));
                if (logicalAddress < hlog.BeginAddress)
                {
                    functions.InitialUpdater(ref pendingContext.key, 
                                             ref pendingContext.input,
                                             ref hlog.GetValue(newLogicalAddress));
                    status = OperationStatus.NOTFOUND;
                }
                else if (logicalAddress >= hlog.HeadAddress)
                {
                    functions.CopyUpdater(ref pendingContext.key, 
                                            ref pendingContext.input,
                                            ref hlog.GetValue(logicalAddress),
                                            ref hlog.GetValue(newLogicalAddress));
                    status = OperationStatus.SUCCESS;
                }
                else
                {
                    // record slipped onto disk
                    hlog.GetInfo(newLogicalAddress).Invalid = true;
                    status = OperationStatus.RETRY_NOW;
                    goto LatchRelease;
                }

                var updatedEntry = default(HashBucketEntry);
                updatedEntry.Tag = tag;
                updatedEntry.Address = newLogicalAddress & Constants.kAddressMask;
                updatedEntry.Pending = entry.Pending;
                updatedEntry.Tentative = false;

                var foundEntry = default(HashBucketEntry);
                foundEntry.word = Interlocked.CompareExchange(
                                        ref bucket->bucket_entries[slot],
                                        updatedEntry.word, entry.word);

                if (foundEntry.word == entry.word)
                {
                    goto LatchRelease;
                }
                else
                {
                    // ah, CAS failed
                    hlog.GetInfo(newLogicalAddress).Invalid = true;
                    status = OperationStatus.RETRY_NOW;
                    goto LatchRelease;
                }
            }
            #endregion

            #region Update failure context
            UpdateFailureContext:
            {
                pendingContext.entry.word = entry.word;
                pendingContext.logicalAddress = logicalAddress;
            }
            #endregion

            #region Latch release
            LatchRelease:
            {
                switch (latchOperation)
                {
                    case LatchOperation.ReleaseExclusive:
                        HashBucket.ReleaseExclusiveLatch(bucket);
                        break;
                    case LatchOperation.ReleaseShared:
                        throw new Exception("Should not release shared latch here!");
                    default:
                        break;
                }
            }
            #endregion

            if(status == OperationStatus.RETRY_NOW)
            {
                return InternalRetryPendingRMW(ctx, ref pendingContext);
            }
            else
            {
                return status;
            }
        }

        /// <summary>
        /// Continue a pending RMW operation with the record retrieved from disk.
        /// </summary>
        /// <param name="ctx">thread (or session) context under which operation must be executed.</param>
        /// <param name="request">record read from the disk.</param>
        /// <param name="pendingContext">internal context for the pending RMW operation</param>
        /// <returns>
        /// <list type="table">
        ///     <listheader>
        ///     <term>Value</term>
        ///     <term>Description</term>
        ///     </listheader>
        ///     <item>
        ///     <term>SUCCESS</term>
        ///     <term>The value has been successfully updated(or inserted).</term>
        ///     </item>
        ///     <item>
        ///     <term>RECORD_ON_DISK</term>
        ///     <term>The record corresponding to 'key' is on disk. Issue async IO to retrieve record and retry later.</term>
        ///     </item>
        ///     <item>
        ///     <term>RETRY_LATER</term>
        ///     <term>Cannot  be processed immediately due to system state. Add to pending list and retry later.</term>
        ///     </item>
        /// </list>
        /// </returns>
        internal OperationStatus InternalContinuePendingRMW(
                                    ExecutionContext ctx,
                                    AsyncIOContext<Key> request,
                                    ref PendingContext pendingContext)
        {
            var recordSize = default(int);
            var bucket = default(HashBucket*);
            var slot = default(int);
            var logicalAddress = Constants.kInvalidAddress;
            var status = default(OperationStatus);

            var hash = pendingContext.key.GetHashCode64();
            var tag = (ushort)((ulong)hash >> Constants.kHashTagShift);

            #region Trace Back for Record on In-Memory HybridLog
            var entry = default(HashBucketEntry);
            FindOrCreateTag(hash, tag, ref bucket, ref slot, ref entry);
            logicalAddress = entry.Address;
            if (logicalAddress >= hlog.HeadAddress)
            {
                if (!pendingContext.key.Equals(ref hlog.GetKey(logicalAddress)))
                {
                    logicalAddress = hlog.GetInfo(logicalAddress).PreviousAddress;
                    TraceBackForKeyMatch(ref pendingContext.key,
                                            logicalAddress,
                                            hlog.HeadAddress,
                                            out logicalAddress);
                }
            }
            #endregion

            var previousFirstRecordAddress = pendingContext.entry.Address;
            if (logicalAddress > previousFirstRecordAddress)
            {
                goto Retry;
            }

            #region Create record in mutable region
            if (request.logicalAddress < hlog.BeginAddress)
            {
                recordSize = hlog.GetInitialRecordSize(ref pendingContext.key, 
                    functions.InitialValueLength(ref pendingContext.key, ref pendingContext.input));
            }
            else
            {
                var physicalAddress = (long)request.record.GetValidPointer();
                recordSize = hlog.GetRecordSize(physicalAddress);
            }
            BlockAllocate(recordSize, out long newLogicalAddress);
            ref RecordInfo recordInfo = ref hlog.GetInfo(newLogicalAddress);
            RecordInfo.WriteInfo(ref recordInfo, ctx.version,
                                true, false, false,
                                entry.Address);
            pendingContext.key.ShallowCopy(ref hlog.GetKey(newLogicalAddress));
            if (request.logicalAddress < hlog.BeginAddress)
            {
                functions.InitialUpdater(ref pendingContext.key,
                                         ref pendingContext.input,
                                         ref hlog.GetValue(newLogicalAddress));
                status = OperationStatus.NOTFOUND;
            }
            else
            {
                functions.CopyUpdater(ref pendingContext.key,
                                      ref pendingContext.input,
                                      ref hlog.GetValue(logicalAddress),
                                      ref hlog.GetValue(newLogicalAddress));
                status = OperationStatus.SUCCESS;
            }

            request.record.Return();

            var updatedEntry = default(HashBucketEntry);
            updatedEntry.Tag = tag;
            updatedEntry.Address = newLogicalAddress & Constants.kAddressMask;
            updatedEntry.Pending = entry.Pending;
            updatedEntry.Tentative = false;

            var foundEntry = default(HashBucketEntry);
            foundEntry.word = Interlocked.CompareExchange(
                                        ref bucket->bucket_entries[slot],
                                        updatedEntry.word, entry.word);

            if (foundEntry.word == entry.word)
            {
                return status;
            }
            else
            {
                hlog.GetInfo(newLogicalAddress).Invalid = true;
                goto Retry;
            }
            #endregion

            Retry:
            return InternalRetryPendingRMW(ctx, ref pendingContext);
        }

        #endregion

        #region Helper Functions

        /// <summary>
        /// Performs appropriate handling based on the internal failure status of the trial.
        /// </summary>
        /// <param name="ctx">Thread (or session) context under which operation was tried to execute.</param>
        /// <param name="pendingContext">Internal context of the operation.</param>
        /// <param name="status">Internal status of the trial.</param>
        /// <returns>
        /// <list type="table">
        ///     <listheader>
        ///     <term>Value</term>
        ///     <term>Description</term>
        ///     </listheader>
        ///     <item>
        ///     <term>OK</term>
        ///     <term>The operation has been completed successfully.</term>
        ///     </item>
        ///     <item>
        ///     <term>PENDING</term>
        ///     <term>The operation is still pending and will callback when done.</term>
        ///     </item>
        /// </list>
        /// </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status HandleOperationStatus(
                    ExecutionContext ctx,
                    PendingContext pendingContext,
                    OperationStatus status)
        {
            if (status == OperationStatus.CPR_SHIFT_DETECTED)
            {
                #region Epoch Synchronization
                var version = ctx.version;
                Debug.Assert(threadCtx.version == version);
                Debug.Assert(threadCtx.phase == Phase.PREPARE);
                Refresh();
                Debug.Assert(threadCtx.version == version + 1);
                Debug.Assert(threadCtx.phase == Phase.IN_PROGRESS);

                pendingContext.version = threadCtx.version;
                #endregion

                #region Retry as (v+1) Operation
                var internalStatus = default(OperationStatus);
                switch (pendingContext.type)
                {
                    case OperationType.READ:
                        internalStatus = InternalRead(ref pendingContext.key,
                                                      ref pendingContext.input,
                                                      ref pendingContext.output,
                                                      ref pendingContext.userContext,
                                                      ref pendingContext);
                        break;
                    case OperationType.UPSERT:
                        internalStatus = InternalUpsert(ref pendingContext.key,
                                                        ref pendingContext.value,
                                                        ref pendingContext.userContext,
                                                        ref pendingContext);
                        break;
                    case OperationType.RMW:
                        internalStatus = InternalRetryPendingRMW(threadCtx, ref pendingContext);
                        break;
                }

                Debug.Assert(internalStatus != OperationStatus.CPR_SHIFT_DETECTED);
                status = internalStatus;
                #endregion
            }

            if (status == OperationStatus.SUCCESS || status == OperationStatus.NOTFOUND)
            {
                return (Status)status;
            }
            else if (status == OperationStatus.RECORD_ON_DISK)
            {
                //Add context to dictionary
                pendingContext.id = ctx.totalPending++;
                ctx.ioPendingRequests.Add(pendingContext.id, pendingContext);

                // Issue asynchronous I/O request
                AsyncIOContext<Key> request = default(AsyncIOContext<Key>);
                request.id = pendingContext.id;
                request.key = pendingContext.key;
                request.logicalAddress = pendingContext.logicalAddress;
                request.callbackQueue = ctx.readyResponses;
                request.record = default(SectorAlignedMemory);
                AsyncGetFromDisk(pendingContext.logicalAddress,
                                 hlog.GetAverageRecordSize(),
                                 AsyncGetFromDiskCallback,
                                 request);

                return Status.PENDING;
            }
            else if (status == OperationStatus.RETRY_LATER)
            {
                ctx.retryRequests.Enqueue(pendingContext);
                return Status.PENDING;
            }
            else
            {
                return Status.ERROR;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void AcquireSharedLatch(Key key)
        {
            var bucket = default(HashBucket*);
            var slot = default(int);
            var hash = key.GetHashCode64();
            var tag = (ushort)((ulong)hash >> Constants.kHashTagShift);
            var entry = default(HashBucketEntry);
            FindOrCreateTag(hash, tag, ref bucket, ref slot, ref entry);
            HashBucket.TryAcquireSharedLatch(bucket);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ReleaseSharedLatch(Key key)
        {
            var bucket = default(HashBucket*);
            var slot = default(int);
            var hash = key.GetHashCode64();
            var tag = (ushort)((ulong)hash >> Constants.kHashTagShift);
            var entry = default(HashBucketEntry);
            FindOrCreateTag(hash, tag, ref bucket, ref slot, ref entry);
            HashBucket.ReleaseSharedLatch(bucket);
        }

        private void HeavyEnter(long hash)
        {
            if (threadCtx.phase == Phase.GC)
                GarbageCollectBuckets(hash);
            if (threadCtx.phase == Phase.PREPARE_GROW)
            {
                // We spin-wait as a simplification
                // Could instead do a "heavy operation" here
                while (_systemState.phase != Phase.IN_PROGRESS_GROW)
                    Thread.SpinWait(100);
                Refresh();
            }
            if (threadCtx.phase == Phase.IN_PROGRESS_GROW)
            {
                SplitBuckets(hash);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void BlockAllocate(int recordSize, out long logicalAddress)
        {
            logicalAddress = hlog.Allocate(recordSize);
            if (logicalAddress >= 0) return;

            while (logicalAddress < 0 && -logicalAddress >= hlog.ReadOnlyAddress)
            {
                InternalRefresh();
                hlog.CheckForAllocateComplete(ref logicalAddress);
                if (logicalAddress < 0)
                {
                    Thread.Sleep(10);
                }
            }

            logicalAddress = logicalAddress < 0 ? -logicalAddress : logicalAddress;

            if (logicalAddress < hlog.ReadOnlyAddress)
            {
                Debug.WriteLine("Allocated address is read-only, retrying");
                BlockAllocate(recordSize, out logicalAddress);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool TraceBackForKeyMatch(
                                    ref Key key,
                                    long fromLogicalAddress,
                                    long minOffset,
                                    out long foundLogicalAddress)
        {
            foundLogicalAddress = fromLogicalAddress;
            while (foundLogicalAddress >= minOffset)
            {
                if (key.Equals(hlog.GetKey(foundLogicalAddress)))
                {
                    return true;
                }
                else
                {
                    foundLogicalAddress = hlog.GetInfo(foundLogicalAddress).PreviousAddress;
                    Debug.WriteLine("Tracing back");
                    continue;
                }
            }
            return false;
        }
        #endregion

        #region Garbage Collection
        private long[] gcStatus;
        private long numPendingChunksToBeGCed;

        private void GarbageCollectBuckets(long hash, bool force = false)
        {
            if (numPendingChunksToBeGCed == 0) return;

            long masked_bucket_index = hash & state[resizeInfo.version].size_mask;
            int offset = (int)(masked_bucket_index >> Constants.kSizeofChunkBits);

            int numChunks = (int)(state[resizeInfo.version].size / Constants.kSizeofChunk);
            if (numChunks == 0) numChunks = 1; // at least one chunk

            if (!Utility.IsPowerOfTwo(numChunks))
            {
                throw new Exception("Invalid number of chunks: " + numChunks);
            }

            for (int i = offset; i < offset + numChunks; i++)
            {
                if (0 == Interlocked.CompareExchange(ref gcStatus[i & (numChunks - 1)], 1, 0))
                {
                    int version = resizeInfo.version;
                    long chunkSize = state[version].size / numChunks;
                    long ptr = chunkSize * (i & (numChunks - 1));

                    HashBucket* src_start = state[version].tableAligned + ptr;
                    CleanBucket(src_start, chunkSize);

                    // GC for chunk is done
                    gcStatus[i & (numChunks - 1)] = 2;

                    if (Interlocked.Decrement(ref numPendingChunksToBeGCed) == 0)
                    {
                        long context = 0;
                        GlobalMoveToNextState(_systemState, SystemState.Make(Phase.REST, _systemState.version), ref context);
                        return;
                    }
                    if (!force)
                        break;

                    InternalRefresh();
                }
            }
        }

        private void CleanBucket(HashBucket* _src_start, long chunkSize)
        {
            HashBucketEntry entry = default(HashBucketEntry);

            for (int i = 0; i < chunkSize; i++)
            {
                var src_start = _src_start + i;

                do
                {
                    for (int index = 0; index < Constants.kOverflowBucketIndex; ++index)
                    {
                        entry.word = *(((long*)src_start) + index);
                        if (entry.Address != Constants.kInvalidAddress && entry.Address < hlog.BeginAddress)
                        {
                            Interlocked.CompareExchange(ref *(((long*)src_start) + index), Constants.kInvalidAddress, entry.word);
                        }
                    }

                    if (*(((long*)src_start) + Constants.kOverflowBucketIndex) == 0) break;
                    src_start = (HashBucket*)overflowBucketsAllocator.GetPhysicalAddress(*(((long*)src_start) + Constants.kOverflowBucketIndex));
                } while (true);
            }
        }
        #endregion

        #region Split Index
        private void SplitBuckets(long hash)
        {
            long masked_bucket_index = hash & state[1 - resizeInfo.version].size_mask;
            int offset = (int)(masked_bucket_index >> Constants.kSizeofChunkBits);

            int numChunks = (int)(state[1 - resizeInfo.version].size / Constants.kSizeofChunk);
            if (numChunks == 0) numChunks = 1; // at least one chunk


            if (!Utility.IsPowerOfTwo(numChunks))
            {
                throw new Exception("Invalid number of chunks: " + numChunks);
            }
            for (int i = offset; i < offset + numChunks; i++)
            {
                if (0 == Interlocked.CompareExchange(ref splitStatus[i & (numChunks - 1)], 1, 0))
                {
                    long chunkSize = state[1 - resizeInfo.version].size / numChunks;
                    long ptr = chunkSize * (i & (numChunks - 1));

                    HashBucket* src_start = state[1 - resizeInfo.version].tableAligned + ptr;
                    HashBucket* dest_start0 = state[resizeInfo.version].tableAligned + ptr;
                    HashBucket* dest_start1 = state[resizeInfo.version].tableAligned + state[1 - resizeInfo.version].size + ptr;

                    SplitChunk(src_start, dest_start0, dest_start1, chunkSize);

                    // split for chunk is done
                    splitStatus[i & (numChunks - 1)] = 2;

                    if (Interlocked.Decrement(ref numPendingChunksToBeSplit) == 0)
                    {
                        // GC old version of hash table
                        state[1 - resizeInfo.version] = default(InternalHashTable);

                        long context = 0;
                        GlobalMoveToNextState(_systemState, SystemState.Make(Phase.REST, _systemState.version), ref context);
                        return;
                    }
                    break;
                }
            }

            while (Interlocked.Read(ref splitStatus[offset & (numChunks - 1)]) == 1)
            {

            }

        }

        private void SplitChunk(
                    HashBucket* _src_start,
                    HashBucket* _dest_start0,
                    HashBucket* _dest_start1,
                    long chunkSize)
        {
            for (int i = 0; i < chunkSize; i++)
            {
                var src_start = _src_start + i;

                long* left = (long*)(_dest_start0 + i);
                long* right = (long*)(_dest_start1 + i);
                long* left_end = left + Constants.kOverflowBucketIndex;
                long* right_end = right + Constants.kOverflowBucketIndex;

                HashBucketEntry entry = default(HashBucketEntry);
                do
                {
                    for (int index = 0; index < Constants.kOverflowBucketIndex; ++index)
                    {
                        entry.word = *(((long*)src_start) + index);
                        if (Constants.kInvalidEntry == entry.word)
                        {
                            continue;
                        }

                        var logicalAddress = entry.Address;
                        if (logicalAddress >= hlog.HeadAddress)
                        {
                            var hash = hlog.GetKey(logicalAddress).GetHashCode64();
                            if ((hash & state[resizeInfo.version].size_mask) >> (state[resizeInfo.version].size_bits - 1) == 0)
                            {
                                // Insert in left
                                if (left == left_end)
                                {
                                    var new_bucket = (HashBucket*)overflowBucketsAllocator.Allocate();
                                    *left = (long)new_bucket;
                                    left = (long*)new_bucket;
                                    left_end = left + Constants.kOverflowBucketIndex;
                                }

                                *left = entry.word;
                                left++;

                                // Insert previous address in right
                                entry.Address = TraceBackForOtherChainStart(hlog.GetInfo(logicalAddress).PreviousAddress, 1);
                                if (entry.Address != Constants.kInvalidAddress)
                                {
                                    if (right == right_end)
                                    {
                                        var new_bucket = (HashBucket*)overflowBucketsAllocator.Allocate();
                                        *right = (long)new_bucket;
                                        right = (long*)new_bucket;
                                        right_end = right + Constants.kOverflowBucketIndex;
                                    }

                                    *right = entry.word;
                                    right++;
                                }
                            }
                            else
                            {
                                // Insert in right
                                if (right == right_end)
                                {
                                    var new_bucket = (HashBucket*)overflowBucketsAllocator.Allocate();
                                    *right = (long)new_bucket;
                                    right = (long*)new_bucket;
                                    right_end = right + Constants.kOverflowBucketIndex;
                                }

                                *right = entry.word;
                                right++;

                                // Insert previous address in left
                                entry.Address = TraceBackForOtherChainStart(hlog.GetInfo(logicalAddress).PreviousAddress, 0);
                                if (entry.Address != Constants.kInvalidAddress)
                                {
                                    if (left == left_end)
                                    {
                                        var new_bucket = (HashBucket*)overflowBucketsAllocator.Allocate();
                                        *left = (long)new_bucket;
                                        left = (long*)new_bucket;
                                        left_end = left + Constants.kOverflowBucketIndex;
                                    }

                                    *left = entry.word;
                                    left++;
                                }
                            }
                        }
                        else
                        {
                            // Insert in both new locations

                            // Insert in left
                            if (left == left_end)
                            {
                                var new_bucket = (HashBucket*)overflowBucketsAllocator.Allocate();
                                *left = (long)new_bucket;
                                left = (long*)new_bucket;
                                left_end = left + Constants.kOverflowBucketIndex;
                            }

                            *left = entry.word;
                            left++;

                            // Insert in right
                            if (right == right_end)
                            {
                                var new_bucket = (HashBucket*)overflowBucketsAllocator.Allocate();
                                *right = (long)new_bucket;
                                right = (long*)new_bucket;
                                right_end = right + Constants.kOverflowBucketIndex;
                            }

                            *right = entry.word;
                            right++;
                        }
                    }

                    if (*(((long*)src_start) + Constants.kOverflowBucketIndex) == 0) break;
                    src_start = (HashBucket*)overflowBucketsAllocator.GetPhysicalAddress(*(((long*)src_start) + Constants.kOverflowBucketIndex));
                } while (true);
            }
        }

        private long TraceBackForOtherChainStart(long logicalAddress, int bit)
        {
            while (logicalAddress >= hlog.HeadAddress)
            {
                var hash = hlog.GetKey(logicalAddress).GetHashCode64();
                if ((hash & state[resizeInfo.version].size_mask) >> (state[resizeInfo.version].size_bits - 1) == bit)
                {
                    return logicalAddress;
                }
                logicalAddress = hlog.GetInfo(logicalAddress).PreviousAddress;
            }
            return logicalAddress;
        }
        #endregion

        #region Compute sizes

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int GetRecordSize(ref Key key, ref Value value)
        {
            return RecordInfo.GetLength() + key.GetLength() + value.GetLength();
        }
        #endregion

    }
}
