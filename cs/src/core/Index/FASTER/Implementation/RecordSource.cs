// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;
using static FASTER.core.Utility;

namespace FASTER.core
{
    /// <summary>
    /// Carries various addresses and accomanying values corresponding to source records for the current InternalXxx or InternalContinuePendingR*
    /// operations, where "source" is a copy source for RMW and/or a locked record. This is passed to functions that create records, such as 
    /// FasterKV.CreateNewRecord*() or FasterKV.InternalTryCopyToTail(), and to unlocking utilities.
    /// </summary>
    internal struct RecordSource<Key, Value>
    {
        /// <summary>
        /// If valid, this is the logical address of a record. As "source", it may be copied from for RMW or pending Reads,
        /// or is locked. This address lives in one of the following places:
        /// <list type="bullet">
        ///     <item>In the in-memory portion of the main log (<see cref="HasMainLogSrc"/>). In this case, it may be a source for RMW CopyUpdater, or simply used for locking.</item>
        ///     <item>In the readcache (<see cref="HasReadCacheSrc"/>). In this case, it may be a source for RMW CopyUpdater, or simply used for locking.</item>
        ///     <item>In the on-disk portion of the main log. In this case, the current call comes from a completed I/O request</item>
        /// </list>
        /// </summary>
        internal long LogicalAddress;

        /// <summary>
        /// If <see cref="HasInMemorySrc"/> this is the physical address of <see cref="LogicalAddress"/>.
        /// </summary>
        internal long PhysicalAddress;

        /// <summary>
        /// The highest logical address in the main log (i.e. below readcache) for this key; if we have a readcache prefix chain, this is the splice point.
        /// </summary>
        internal long LatestLogicalAddress;

        /// <summary>
        /// If valid, the lowest readcache logical address for this key; used to splice records between readcache and main log.
        /// </summary>
        internal long LowestReadCacheLogicalAddress;

        /// <summary>
        /// The physical address of <see cref="LowestReadCacheLogicalAddress"/>.
        /// </summary>
        internal long LowestReadCachePhysicalAddress;

        /// <summary>
        /// If <see cref="HasInMemorySrc"/>, this is the allocator (hlog or readcache) that <see cref="LogicalAddress"/> is in.
        /// </summary>
        internal AllocatorBase<Key, Value> Log;

        /// <summary>
        /// Set by caller to indicate whether the <see cref="LogicalAddress"/> is an in-memory record in the main log, being used as a copy source and/or a lock.
        /// </summary>
        internal bool HasMainLogSrc;

        /// <summary>
        /// Set by caller to indicate whether the <see cref="LogicalAddress"/> is an in-memory record in the readcache, being used as a copy source and/or a lock.
        /// </summary>
        internal bool HasReadCacheSrc;

        /// <summary>
        /// Set by caller to indicate whether it has an transient lock in the LockTable for the operation Key.
        /// </summary>
        internal bool HasTransientLock;

        /// <summary>
        /// Status of RecordIsolation RecordInfo lock, if applicable.
        /// </summary>
        internal RecordIsolationResult recordIsolationResult;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal ref RecordInfo GetInfo() => ref Log.GetInfo(PhysicalAddress);
        internal ref Key GetKey() => ref Log.GetKey(PhysicalAddress);
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal ref Value GetValue() => ref Log.GetValue(PhysicalAddress);
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal long SetPhysicalAddress() => this.PhysicalAddress = Log.GetPhysicalAddress(LogicalAddress);

        internal bool HasInMemorySrc => HasMainLogSrc || HasReadCacheSrc;

        internal bool HasLock => HasTransientLock || recordIsolationResult == RecordIsolationResult.Success;

        /// <summary>
        /// Initialize to the latest logical address from the caller.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void Set(long latestLogicalAddress, AllocatorBase<Key, Value> srcLog)
        {
            PhysicalAddress = default;
            LowestReadCacheLogicalAddress = default;
            LowestReadCachePhysicalAddress = default;
            HasMainLogSrc = false;
            HasReadCacheSrc = default;

            // HasTransientLock = ...;   Do not clear this; it is in the LockTable and must be preserved until unlocked
            // recordIsolationResult = ...; Do not clear this either

            this.LatestLogicalAddress = this.LogicalAddress = AbsoluteAddress(latestLogicalAddress);
            this.Log = srcLog;
        }

        public override string ToString()
        {
            var isRC = "(rc)";
            var llaRC = IsReadCache(LatestLogicalAddress) ? isRC : string.Empty;
            var laRC = IsReadCache(LogicalAddress) ? isRC : string.Empty;
            static string bstr(bool value) => value ? "T" : "F";
            string ephLockResult = this.recordIsolationResult switch
            {
                RecordIsolationResult.Success => "S",
                RecordIsolationResult.Failed => "F",
                RecordIsolationResult.HoldForSeal => "H",
                _ => "none"
            };
            return $"lla {AbsoluteAddress(LatestLogicalAddress)}{llaRC}, la {AbsoluteAddress(LogicalAddress)}{laRC}, lrcla {AbsoluteAddress(LowestReadCacheLogicalAddress)},"
                 + $" logSrc {bstr(HasMainLogSrc)}, rcSrc {bstr(HasReadCacheSrc)}, tLock {bstr(HasTransientLock)}, eLock {ephLockResult}";
        }
    }
}
