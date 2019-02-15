// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma warning disable 0162

using System;
using System.Runtime.CompilerServices;

namespace FASTER.core
{
    public unsafe partial class FasterKV<Key, Value, Input, Output, Context, Functions> : FasterBase, IFasterKV<Key, Value, Input, Output, Context>
        where Key : new()
        where Value : new()
        where Functions : IFunctions<Key, Value, Input, Output, Context>
    {
        private readonly Functions functions;
        private readonly AllocatorBase<Key, Value> hlog;
        private readonly IFasterEqualityComparer<Key> comparer;

        private readonly bool CopyReadsToTail = false;
        private readonly bool FoldOverSnapshot = false;
        private readonly int sectorSize;

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
        

        private SafeConcurrentDictionary<Guid, long> _recoveredSessions;

        [ThreadStatic]
        private static FasterExecutionContext prevThreadCtx = default(FasterExecutionContext);

        [ThreadStatic]
        private static FasterExecutionContext threadCtx = default(FasterExecutionContext);


        /// <summary>
        /// Create FASTER instance
        /// </summary>
        /// <param name="size">Size of core index (#cache lines)</param>
        /// <param name="comparer">FASTER equality comparer for key</param>
        /// <param name="functions">Callback functions</param>
        /// <param name="logSettings">Log settings</param>
        /// <param name="checkpointSettings">Checkpoint settings</param>
        /// <param name="serializerSettings">Serializer settings</param>
        public FasterKV(long size, Functions functions, LogSettings logSettings, CheckpointSettings checkpointSettings = null, SerializerSettings<Key, Value> serializerSettings = null, IFasterEqualityComparer<Key> comparer = null)
        {
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

            directoryConfiguration = new DirectoryConfiguration(checkpointSettings.CheckpointDir);

            FoldOverSnapshot = checkpointSettings.CheckPointType == core.CheckpointType.FoldOver;
            CopyReadsToTail = logSettings.CopyReadsToTail;
            this.functions = functions;

            if (Utility.IsBlittable<Key>() && Utility.IsBlittable<Value>())
                hlog = new BlittableAllocator<Key, Value>(logSettings, this.comparer);
            else
                hlog = new GenericAllocator<Key, Value>(logSettings, serializerSettings, this.comparer);

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
        public Status Upsert(ref Key key, ref Value desiredValue, Context userContext, long monotonicSerialNum)
        {
            var context = default(PendingContext);
            var internalStatus = InternalUpsert(ref key, ref desiredValue, ref userContext, ref context);
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
        public Status RMW(ref Key key, ref Input input, Context userContext, long monotonicSerialNum)
        {
            var context = default(PendingContext);
            var internalStatus = InternalRMW(ref key, ref input, ref userContext, ref context);
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
        /// Shift log head address to prune memory foorprint of hybrid log
        /// </summary>
        /// <param name="newHeadAddress">Address to shift head until</param>
        /// <param name="wait">Wait to ensure shift is registered (may involve page flushing)</param>
        public bool ShiftHeadAddress(long newHeadAddress, bool wait = false)
        {
            return InternalShiftHeadAddress(newHeadAddress, wait);
        }

        /// <summary>
        /// Shift log read-only address
        /// </summary>
        /// <param name="newReadOnlyAddress">Address to shift read-only until</param>
        public bool ShiftReadOnlyAddress(long newReadOnlyAddress)
        {
            return hlog.ShiftReadOnlyAddress(newReadOnlyAddress);
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
            MallocFixedPageSize<HashBucket>.Instance?.Dispose();
            MallocFixedPageSize<HashBucket>.Instance = null;
            MallocFixedPageSize<HashBucket>.PhysicalInstance?.Dispose();
            MallocFixedPageSize<HashBucket>.PhysicalInstance = null;
            hlog.Dispose();
        }

        /// <summary>
        /// Scan the log underlying FASTER, for address range
        /// </summary>
        /// <param name="beginAddress"></param>
        /// <param name="endAddress"></param>
        /// <param name="scanBufferingMode"></param>
        /// <returns></returns>
        public IFasterScanIterator<Key, Value> LogScan(long beginAddress, long endAddress, ScanBufferingMode scanBufferingMode = ScanBufferingMode.DoublePageBuffering)
        {
            return hlog.Scan(beginAddress, endAddress, scanBufferingMode);
        }
    }
}
