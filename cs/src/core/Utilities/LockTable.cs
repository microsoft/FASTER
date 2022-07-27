// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;

namespace FASTER.core
{
    // We need to duplicate the Key because we can't get to the key object of the dictionary to Return() it.
    // This is a class rather than a struct because a struct would update a copy.
    internal class LockTableEntry<TKey>
    {
        internal IHeapContainer<TKey> key;
        internal RecordInfo logRecordInfo;  // in main log
        internal RecordInfo lockRecordInfo; // in lock table; we have to Lock/Tentative the LockTable entry separately from logRecordInfo

        internal LockTableEntry(IHeapContainer<TKey> key, RecordInfo logRecordInfo, RecordInfo lockRecordInfo)
        {
            this.key = key;
            this.logRecordInfo = logRecordInfo;
            this.lockRecordInfo = lockRecordInfo;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void XLock() => this.lockRecordInfo.LockExclusiveRaw();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void XUnlock() { this.lockRecordInfo.UnlockExclusive();}

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void SLock() => this.lockRecordInfo.LockSharedRaw();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void SUnlock() { this.lockRecordInfo.UnlockShared(); }

        public override string ToString() => $"{key}";
    }

    // TODO: abstract base or interface for additional implementations (or "DI" a test wrapper)
    // TODO: internal IHeapContainer<TKey> key; causes an object allocation each time. we need:
    //      Non-Varlen: a specialized LockTableEntry that just uses Key directly
    //      Varlen: a shared heap container abstraction that shares a single buffer pool allocator and allocates, frees into it, returning a struct wrapper.

    internal class LockTable<TKey>
    {
        class KeyComparer : IEqualityComparer<IHeapContainer<TKey>>
        {
            readonly internal IFasterEqualityComparer<TKey> comparer;

            internal KeyComparer(IFasterEqualityComparer<TKey> comparer) => this.comparer = comparer;

            public bool Equals(IHeapContainer<TKey> k1, IHeapContainer<TKey> k2) => comparer.Equals(ref k1.Get(), ref k2.Get());

            public int GetHashCode(IHeapContainer<TKey> k) => (int)comparer.GetHashCode64(ref k.Get());
        }

        readonly internal SafeConcurrentDictionary<IHeapContainer<TKey>, LockTableEntry<TKey>> dict;
        readonly IVariableLengthStruct<TKey> keyLen;
        readonly KeyComparer keyComparer;
        readonly SectorAlignedBufferPool bufferPool;

        // dict.Empty takes locks on all tables ("snapshot semantics"), which is too much of a perf hit. So we track this
        // separately. It is not atomic when items are added/removed, but by incrementing it before and decrementing it after
        // we add or remove items, respectively, we achieve the desired goal of IsActive.
        long approxNumItems = 0;

        internal LockTable(IVariableLengthStruct<TKey> keyLen, IFasterEqualityComparer<TKey> comparer, SectorAlignedBufferPool bufferPool)
        {
            this.keyLen = keyLen;
            this.keyComparer = new(comparer);
            this.bufferPool = bufferPool;
            this.dict = new(this.keyComparer);
        }

        internal bool IsActive => this.approxNumItems > 0;

        IHeapContainer<TKey> GetKeyContainer(ref TKey key) 
            => bufferPool is null ? new StandardHeapContainer<TKey>(ref key) : new VarLenHeapContainer<TKey>(ref key, keyLen, bufferPool);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool Unlock(ref TKey key, LockType lockType, out bool exists)
        {
            var lookupKey = GetKeyContainer(ref key);
            exists = dict.TryGetValue(lookupKey, out var lte);
            if (exists)
                return Unlock(lookupKey, lte, lockType);
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool Unlock(IHeapContainer<TKey> lookupKey, LockTableEntry<TKey> lte, LockType lockType)
        {
            bool result = false;
            lte.SLock();
            if (!lte.lockRecordInfo.Invalid)
            {
                lte.logRecordInfo.Unlock(lockType);
                result = true;
            }
            lte.SUnlock();

            if (!lte.logRecordInfo.IsLocked)
            {
                lte.XLock();
                if (!lte.logRecordInfo.IsLocked && !lte.lockRecordInfo.Invalid)
                    TryRemoveEntry(lookupKey);
                lte.XUnlock();
            }

            return result;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void TransferFromLogRecord(ref TKey key, RecordInfo logRecordInfo)
        {
            var lookupKey = GetKeyContainer(ref key);
            RecordInfo newRec = default;
            newRec.SetValid();
            newRec.CopyLocksFrom(logRecordInfo);
            RecordInfo lockRec = default;
            lockRec.SetValid();
            Interlocked.Increment(ref this.approxNumItems);
            if (!dict.TryAdd(lookupKey, new(lookupKey, newRec, lockRec)))
            {
                Interlocked.Decrement(ref this.approxNumItems);
                lookupKey.Dispose();
                Debug.Fail("Trying to Transfer to an existing key");
                return;
            }
        }

        // Lock the LockTable record for the key if it exists, else add a Tentative record for it.
        // Returns true if the record was locked or tentative; else false (an already-Tentative record was encountered)
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool LockOrTentative(ref TKey key, LockType lockType, out bool tentative)
        {
            var keyContainer = GetKeyContainer(ref key);
            bool existingConflict = false;
            var lte = dict.AddOrUpdate(keyContainer,
                key => {            // New Value
                    RecordInfo lockRecordInfo = default;
                    lockRecordInfo.Tentative = true;
                    lockRecordInfo.SetValid();
                    RecordInfo logRecordInfo = default;
                    logRecordInfo.SetValid();
                    existingConflict = !logRecordInfo.Lock(lockType);
                    Interlocked.Increment(ref this.approxNumItems);
                    return new(key, logRecordInfo, lockRecordInfo);
                }, (key, lte) => {  // Update Value
                    if (lte.lockRecordInfo.Tentative)
                        existingConflict = true;
                    else
                    {
                        lte.XLock();
                        existingConflict = lte.lockRecordInfo.Invalid || !lte.logRecordInfo.Lock(lockType);
                        lte.XUnlock();
                    }
                    return lte;
                });
            tentative = lte.lockRecordInfo.Tentative;
            return !existingConflict;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool Get(TKey key, out RecordInfo recordInfo) => Get(ref key, out recordInfo);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool ContainsKey(ref TKey key)
        {
            if (!IsActive)
                return false;
            using var lookupKey = GetKeyContainer(ref key);
            return dict.ContainsKey(lookupKey);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool Get(ref TKey key, out RecordInfo recordInfo)
        {
            if (IsActive)
            {
                using var lookupKey = GetKeyContainer(ref key);
                if (dict.TryGetValue(lookupKey, out var lte))
                {
                    recordInfo = lte.logRecordInfo;
                    return !lte.lockRecordInfo.Invalid;
                }
            }

            // !IsActive is still valid; the record is not locked.
            recordInfo = default;
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool ClearTentative(ref TKey key)
        {
            if (!IsActive)
                return false;
            using var lookupKey = GetKeyContainer(ref key);

            // False is legit, as other operations may have removed it.
            if (!dict.TryGetValue(lookupKey, out var lte))
                return false;
            bool cleared = false;
            lte.XLock();
            if (lte.lockRecordInfo.Tentative && !lte.lockRecordInfo.Invalid)
            {
                lte.lockRecordInfo.SetTentativeAtomic(false);
                cleared = true;
            }
            lte.XUnlock();
            return cleared;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void UnlockOrRemoveTentative(ref TKey key, LockType lockType, bool wasTentative)
        {
            if (IsActive)
            {
                using var lookupKey = GetKeyContainer(ref key);
                if (dict.TryGetValue(lookupKey, out var lte))
                {
                    Debug.Assert(wasTentative == lte.lockRecordInfo.Tentative, "lockRecordInfo.Tentative was not as expected");

                    // We assume that we own the lock or placed the Tentative record, and a Tentative record may have legitimately been removed.
                    if (lte.lockRecordInfo.Tentative)
                        RemoveIfTentative(lookupKey, lte);
                    else
                        Unlock(lookupKey, lte, lockType);
                    return;
                }
            }

            // A tentative record may have been removed by the other side of the 2-phase process.
            if (!wasTentative)
                Debug.Fail("Trying to UnlockOrRemoveTentative on nonexistent nonTentative key");
        }

        bool TryRemoveEntry(IHeapContainer<TKey> lookupKey)
        {
            if (dict.TryRemove(lookupKey, out var lte))
            {
                Interlocked.Decrement(ref this.approxNumItems);
                lte.lockRecordInfo.SetInvalid();
                lte.key.Dispose();
                return true;
            }
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool RemoveIfTentative(IHeapContainer<TKey> lookupKey, LockTableEntry<TKey> lte)
        {
            if (lte.lockRecordInfo.Dirty)
                if (lte.lockRecordInfo.Filler) return false;
            if (lte.lockRecordInfo.IsLocked)
                if (lte.lockRecordInfo.Filler) return false;
            if (!lte.lockRecordInfo.Tentative || lte.lockRecordInfo.Invalid)
                return false;
            lte.XLock();
            if (lte.lockRecordInfo.Dirty)
                if (lte.lockRecordInfo.Filler) return false;
            
            // If the record is Invalid, it was already removed.
            var removed = lte.lockRecordInfo.Invalid || (lte.lockRecordInfo.Tentative && TryRemoveEntry(lookupKey));
            lte.XUnlock();
            return removed;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void TransferToLogRecord(ref TKey key, ref RecordInfo logRecord)
        {
            // This is called after the record has been CAS'd into the log or readcache, so this should not be allowed to fail.
            using var lookupKey = GetKeyContainer(ref key);
            if (dict.TryGetValue(lookupKey, out var lte))
            {
                // If it's a Tentative record, wait for it to no longer be tentative.
                while (lte.lockRecordInfo.Tentative)
                    Thread.Yield();
                
                // If invalid, then the Lock thread called TryRemoveEntry and will retry, which will add the locks after the main-log record is no longer tentative.
                if (lte.lockRecordInfo.Invalid)
                    return;

                lte.XLock();
                if (!lte.lockRecordInfo.Invalid)
                {
                    logRecord.CopyLocksFrom(lte.logRecordInfo);
                    TryRemoveEntry(lookupKey);
                }
                lte.XUnlock();
            }

            // If we're here, there were no locks to apply, or we applied them all.
        }

        public override string ToString() => this.dict.Count.ToString();
    }
}
