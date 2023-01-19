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
        /// The highest logical address (in the main log, i.e. below readcache) for this key; if we have a readcache prefix chain, this is the splice point.
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
        /// Set by caller to indicate whether it has an ephemeral lock on the InMemorySrc record's <see cref="RecordInfo"/> (this may be mainlog or readcache).
        /// </summary>
        internal bool HasInMemoryLock;

        /// <summary>
        /// Set by caller to indicate whether it has an ephemeral lock in the LockTable for the operation Key.
        /// </summary>
        internal bool HasLockTableLock;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void ClearSrc()
        {
            this.LogicalAddress = Constants.kInvalidAddress;
            this.PhysicalAddress = 0;
            this.Log = default;
            this.HasMainLogSrc = false;
            this.HasReadCacheSrc = false;
            this.HasInMemoryLock = false;
            this.HasLockTableLock = false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal ref RecordInfo GetSrcRecordInfo() => ref Log.GetInfo(PhysicalAddress);
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal ref Value GetSrcValue() => ref Log.GetValue(PhysicalAddress);

        internal bool HasSrc => HasInMemorySrc || HasLockTableLock;
        internal bool HasInMemorySrc => HasMainLogSrc || HasReadCacheSrc;
        internal bool HasLock => HasInMemoryLock || HasLockTableLock;

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
            this.HasInMemoryLock = false;
            HasLockTableLock = false;

            this.LatestLogicalAddress = this.LogicalAddress = AbsoluteAddress(latestLogicalAddress);
            this.Log = srcLog;
        }

        /// <summary>
        /// After a successful CopyUpdate or other replacement of a source record, this marks the source record as Sealed or Invalid.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void MarkSourceRecordAfterSuccessfulCopy(ref RecordInfo srcRecordInfo)
        {
            if (this.HasInMemorySrc)
            {
                System.Diagnostics.Debug.Assert(this.LogicalAddress >= this.Log.ClosedUntilAddress, "Should not have evicted the source record while we held the epoch");

                // Note that records cannot be evicted even if they are now below HeadAddress of the log or readcache, because we hold the epoch.
                if (this.HasReadCacheSrc)
                {
                    // Even though we should be called with an XLock (unless ephemeral locking is disabled) we need to do this atomically;
                    // otherwise this could race with ReadCacheEvict unlinking records.
                    srcRecordInfo.SetInvalidAtomic();
                }
                else
                {
                    // Another thread may come along to do this update in-place, or use this record as a copy source, once we've released our lock;
                    // Seal it to prevent that. This will cause the other thread to RETRY_NOW (unlike Invalid which ignores the record).
                    // Because we have an XLock (unless ephemeral locking is disabled), we don't need an atomic operation here.
                    srcRecordInfo.Seal();
                }

                // if fasterSession.DisableEphemeralLocking, the "finally" handler won't unlock it, so we do that here.
                // For ephemeral locks, we don't clear the locks here (defer that to the "finally").
                if (fasterSession.DisableEphemeralLocking)
                    srcRecordInfo.ClearLocks();
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool InMemorySourceWasEvicted() => this.HasInMemorySrc && this.LogicalAddress < this.Log.HeadAddress;

        public override string ToString()
        {
            var isRC = "(rc)";
            var llaRC = IsReadCache(LatestLogicalAddress) ? isRC : string.Empty;
            var laRC = IsReadCache(LogicalAddress) ? isRC : string.Empty;
            static string bstr(bool value) => value ? "T" : "F";
            return $"lla {AbsoluteAddress(LatestLogicalAddress)}{llaRC}, la {AbsoluteAddress(LogicalAddress)}{laRC}, lrcla {AbsoluteAddress(LowestReadCacheLogicalAddress)},"
                 + $" hasMLsrc {bstr(HasMainLogSrc)}, hasRCsrc {bstr(HasReadCacheSrc)}, hasIMlock {bstr(HasInMemoryLock)}, hasLTlock {bstr(HasLockTableLock)}";
        }
    }
}
