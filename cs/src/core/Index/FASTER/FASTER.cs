// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma warning disable 0162

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

namespace FASTER.core
{
    public partial class FasterKV<Key, Value, Input, Output, Context, Functions> : FasterBase, IFasterKV<Key, Value, Input, Output, Context>
        where Key : new()
        where Value : new()
        where Functions : IFunctions<Key, Value, Input, Output, Context>
    {
        private readonly Functions functions;
        private readonly AllocatorBase<Key, Value> hlog;
        private readonly AllocatorBase<Key, Value> readcache;
        private readonly IFasterEqualityComparer<Key> comparer;

        private readonly bool UseReadCache = false;
        private readonly bool CopyReadsToTail = false;
        private readonly bool FoldOverSnapshot = false;
        private readonly int sectorSize;

        /// <summary>
        /// Number of used entries in hash index
        /// </summary>
        public long EntryCount => GetEntryCount();

        /// <summary>
        /// Size of index in #cache lines (64 bytes each)
        /// </summary>
        public long IndexSize => state[resizeInfo.version].size;

        /// <summary>
        /// Comparer used by FASTER
        /// </summary>
        public IFasterEqualityComparer<Key> Comparer => comparer;

        /// <summary>
        /// Hybrid log used by this FASTER instance
        /// </summary>
        public LogAccessor<Key, Value, Input, Output, Context> Log { get; }

        /// <summary>
        /// Read cache used by this FASTER instance
        /// </summary>
        public LogAccessor<Key, Value, Input, Output, Context> ReadCache { get; }


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
        

        private ConcurrentDictionary<Guid, long> _recoveredSessions;

        private readonly FastThreadLocal<FasterExecutionContext> prevThreadCtx;
        private readonly FastThreadLocal<FasterExecutionContext> threadCtx;


        /// <summary>
        /// Create FASTER instance
        /// </summary>
        /// <param name="size">Size of core index (#cache lines)</param>
        /// <param name="comparer">FASTER equality comparer for key</param>
        /// <param name="variableLengthStructSettings"></param>
        /// <param name="functions">Callback functions</param>
        /// <param name="logSettings">Log settings</param>
        /// <param name="checkpointSettings">Checkpoint settings</param>
        /// <param name="serializerSettings">Serializer settings</param>
        public FasterKV(long size, Functions functions, LogSettings logSettings, CheckpointSettings checkpointSettings = null, SerializerSettings<Key, Value> serializerSettings = null, IFasterEqualityComparer<Key> comparer = null, VariableLengthStructSettings<Key, Value> variableLengthStructSettings = null)
        {
            threadCtx = new FastThreadLocal<FasterExecutionContext>();
            prevThreadCtx = new FastThreadLocal<FasterExecutionContext>();

            if (comparer != null)
                this.comparer = comparer;
            else
            {
                if (typeof(IFasterEqualityComparer<Key>).IsAssignableFrom(typeof(Key)))
                {
                    this.comparer = new Key() as IFasterEqualityComparer<Key>;
                }
                else
                {
                    Console.WriteLine("***WARNING*** Creating default FASTER key equality comparer based on potentially slow EqualityComparer<Key>.Default. To avoid this, provide a comparer (IFasterEqualityComparer<Key>) as an argument to FASTER's constructor, or make Key implement the interface IFasterEqualityComparer<Key>");
                    this.comparer = FasterEqualityComparer<Key>.Default;
                }
            }

            if (checkpointSettings == null)
                checkpointSettings = new CheckpointSettings();

            if (checkpointSettings.CheckpointDir != null && checkpointSettings.CheckpointManager != null)
                throw new Exception("Specify either CheckpointManager or CheckpointDir for CheckpointSettings, not both");

            checkpointManager = checkpointSettings.CheckpointManager ?? new LocalCheckpointManager(checkpointSettings.CheckpointDir ?? "");

            FoldOverSnapshot = checkpointSettings.CheckPointType == core.CheckpointType.FoldOver;
            CopyReadsToTail = logSettings.CopyReadsToTail;
            this.functions = functions;

            if (logSettings.ReadCacheSettings != null)
            {
                CopyReadsToTail = false;
                UseReadCache = true;
            }

            if (Utility.IsBlittable<Key>() && Utility.IsBlittable<Value>())
            {
                if (variableLengthStructSettings != null)
                {
                    hlog = new VariableLengthBlittableAllocator<Key, Value>
                        (logSettings, variableLengthStructSettings, this.comparer, null, epoch);
                    Log = new LogAccessor<Key, Value, Input, Output, Context>(this, hlog);
                    if (UseReadCache)
                    {
                        readcache = new VariableLengthBlittableAllocator<Key, Value>(
                            new LogSettings
                            {
                                PageSizeBits = logSettings.ReadCacheSettings.PageSizeBits,
                                MemorySizeBits = logSettings.ReadCacheSettings.MemorySizeBits,
                                SegmentSizeBits = logSettings.ReadCacheSettings.MemorySizeBits,
                                MutableFraction = logSettings.ReadCacheSettings.SecondChanceFraction
                            }, variableLengthStructSettings, this.comparer, ReadCacheEvict, epoch);
                        readcache.Initialize();
                        ReadCache = new LogAccessor<Key, Value, Input, Output, Context>(this, readcache);
                    }
                }
                else
                {
                    hlog = new BlittableAllocator<Key, Value>(logSettings, this.comparer, null, epoch);
                    Log = new LogAccessor<Key, Value, Input, Output, Context>(this, hlog);
                    if (UseReadCache)
                    {
                        readcache = new BlittableAllocator<Key, Value>(
                            new LogSettings
                            {
                                PageSizeBits = logSettings.ReadCacheSettings.PageSizeBits,
                                MemorySizeBits = logSettings.ReadCacheSettings.MemorySizeBits,
                                SegmentSizeBits = logSettings.ReadCacheSettings.MemorySizeBits,
                                MutableFraction = logSettings.ReadCacheSettings.SecondChanceFraction
                            }, this.comparer, ReadCacheEvict, epoch);
                        readcache.Initialize();
                        ReadCache = new LogAccessor<Key, Value, Input, Output, Context>(this, readcache);
                    }
                }
            }
            else
            {
                hlog = new GenericAllocator<Key, Value>(logSettings, serializerSettings, this.comparer, null, epoch);
                Log = new LogAccessor<Key, Value, Input, Output, Context>(this, hlog);
                if (UseReadCache)
                {
                    readcache = new GenericAllocator<Key, Value>(
                        new LogSettings
                        {
                            PageSizeBits = logSettings.ReadCacheSettings.PageSizeBits,
                            MemorySizeBits = logSettings.ReadCacheSettings.MemorySizeBits,
                            SegmentSizeBits = logSettings.ReadCacheSettings.MemorySizeBits,
                            MutableFraction = logSettings.ReadCacheSettings.SecondChanceFraction
                        }, serializerSettings, this.comparer, ReadCacheEvict, epoch);
                    readcache.Initialize();
                    ReadCache = new LogAccessor<Key, Value, Input, Output, Context>(this, readcache);
                }
            }

            hlog.Initialize();

            sectorSize = (int)logSettings.LogDevice.SectorSize;
            Initialize(size, sectorSize);

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
        /// Recover from the latest checkpoints
        /// </summary>
        public void Recover()
        {
            InternalRecoverFromLatestCheckpoints();
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
        /// Start session with FASTER - call once per thread before using FASTER
        /// </summary>
        /// <returns></returns>
        public Guid StartSession()
        {
            return InternalAcquire();
        }


        /// <summary>
        /// Continue session with FASTER
        /// </summary>
        /// <param name="guid"></param>
        /// <returns></returns>
        public long ContinueSession(Guid guid)
        {
            return InternalContinue(guid);
        }

        /// <summary>
        /// Stop session with FASTER
        /// </summary>
        public void StopSession()
        {
            InternalRelease();
        }

        /// <summary>
        /// Refresh epoch (release memory pins)
        /// </summary>
        public void Refresh()
        {
            InternalRefresh();
        }


        /// <summary>
        /// Complete outstanding pending operations
        /// </summary>
        /// <param name="wait"></param>
        /// <returns></returns>
        public bool CompletePending(bool wait = false)
        {
            return InternalCompletePending(wait);
        }

        /// <summary>
        /// Get list of pending requests (for local session)
        /// </summary>
        /// <returns></returns>
        public IEnumerable<long> GetPendingRequests()
        {

            foreach (var kvp in prevThreadCtx.Value.ioPendingRequests)
                yield return kvp.Value.serialNum;

            foreach (var val in prevThreadCtx.Value.retryRequests)
                yield return val.serialNum;

            foreach (var kvp in threadCtx.Value.ioPendingRequests)
                yield return kvp.Value.serialNum;

            foreach (var val in threadCtx.Value.retryRequests)
                yield return val.serialNum;
        }

        /// <summary>
        /// Complete outstanding pending operations
        /// </summary>
        /// <returns></returns>
        public async ValueTask CompletePendingAsync()
        {
            await InternalCompletePendingAsync();
        }

        /// <summary>
        /// Complete the ongoing checkpoint (if any)
        /// </summary>
        /// <param name="wait"></param>
        /// <returns></returns>
        public bool CompleteCheckpoint(bool wait = false)
        {
            if(threadCtx == null)
            {
                // the thread does not have an active session
                // we can wait until system state becomes REST
                do
                {
                    if(_systemState.phase == Phase.REST)
                    {
                        return true;
                    }
                } while (wait);
            }
            else
            {
                // the thread does has an active session and 
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
            }
            return false;
        }

        /// <summary>
        /// Complete the ongoing checkpoint (if any)
        /// </summary>
        /// <returns></returns>
        public async ValueTask CompleteCheckpointAsync()
        {
            // Thread has an active session.
            // So we need to constantly complete pending 
            // and refresh (done inside CompletePending)
            // for the checkpoint to be proceed
            do
            {
                await CompletePendingAsync();
                if (_systemState.phase == Phase.REST)
                {
                    await CompletePendingAsync();
                    return;
                }
            } while (true);
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
        public Status Read(ref Key key, ref Input input, ref Output output, Context userContext, long monotonicSerialNum)
        {
            var context = default(PendingContext);
            var internalStatus = InternalRead(ref key, ref input, ref output, ref userContext, ref context);
            Status status;
            if (internalStatus == OperationStatus.SUCCESS || internalStatus == OperationStatus.NOTFOUND)
            {
                status = (Status)internalStatus;
            }
            else
            {
                status = HandleOperationStatus(threadCtx.Value, context, internalStatus);
            }
            threadCtx.Value.serialNum = monotonicSerialNum;
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
        public Status Upsert(ref Key key, ref Value desiredValue, Context userContext, long monotonicSerialNum)
        {
            var context = default(PendingContext);
            var internalStatus = InternalUpsert(ref key, ref desiredValue, ref userContext, ref context);
            Status status;

            if (internalStatus == OperationStatus.SUCCESS || internalStatus == OperationStatus.NOTFOUND)
            {
                status = (Status)internalStatus;
            }
            else
            {
                status = HandleOperationStatus(threadCtx.Value, context, internalStatus);
            }
            threadCtx.Value.serialNum = monotonicSerialNum;
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
        public Status RMW(ref Key key, ref Input input, Context userContext, long monotonicSerialNum)
        {
            var context = default(PendingContext);
            var internalStatus = InternalRMW(ref key, ref input, ref userContext, ref context);
            Status status;
            if (internalStatus == OperationStatus.SUCCESS || internalStatus == OperationStatus.NOTFOUND)
            {
                status = (Status)internalStatus;
            }
            else
            {
                status = HandleOperationStatus(threadCtx.Value, context, internalStatus);
            }
            threadCtx.Value.serialNum = monotonicSerialNum;
            return status;
        }

        /// <summary>
        /// Delete entry (use tombstone if necessary)
        /// Hash entry is removed as a best effort (if key is in memory and at 
        /// the head of hash chain.
        /// Value is set to null (using ConcurrentWrite) if it is in mutable region
        /// </summary>
        /// <param name="key"></param>
        /// <param name="userContext"></param>
        /// <param name="monotonicSerialNum"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Status Delete(ref Key key, Context userContext, long monotonicSerialNum)
        {
            var context = default(PendingContext);
            var internalStatus = InternalDelete(ref key, ref userContext, ref context);
            var status = default(Status);
            if (internalStatus == OperationStatus.SUCCESS || internalStatus == OperationStatus.NOTFOUND)
            {
                status = (Status)internalStatus;
            }
            threadCtx.Value.serialNum = monotonicSerialNum;
            return status;
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
            base.Free();
            threadCtx.Dispose();
            prevThreadCtx.Dispose();
            hlog.Dispose();
            readcache?.Dispose();
        }
    }
}
