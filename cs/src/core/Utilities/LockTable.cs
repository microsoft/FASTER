// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;

namespace FASTER.core
{
    // We need to duplicate the Key because we can't get to the key object of the dictionary to Return() it.
    // This is a class rather than a struct because a struct would update a copy.
    internal class LockTableEntry<TKey> : IEqualityComparer<LockTableEntry<TKey>>
    {
        internal IHeapContainer<TKey> key;
        internal RecordInfo logRecordInfo;  // in main log
        internal RecordInfo lockRecordInfo; // in lock table; we have to Lock/Seal/Tentative the LockTable entry separately from logRecordInfo

        internal LockTableEntry(IHeapContainer<TKey> key, RecordInfo logRecordInfo, RecordInfo lockRecordInfo)
        {
            this.key = key;
            this.logRecordInfo = logRecordInfo;
            this.lockRecordInfo = lockRecordInfo;
        }

        public bool Equals(LockTableEntry<TKey> k1, LockTableEntry<TKey> k2) => k1.logRecordInfo.Equals(k2.logRecordInfo);

        public int GetHashCode(LockTableEntry<TKey> k) => (int)k.logRecordInfo.GetHashCode64();
    }

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

        // Provide our own implementation of "Update by lambda"
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool Update(ref TKey key, Func<LockTableEntry<TKey>, LockTableEntry<TKey>> updateFactory)
        {
            using var keyContainer = GetKeyContainer(ref key);
            while (dict.TryGetValue(keyContainer, out var lte))
            {
                if (dict.TryUpdate(keyContainer, updateFactory(lte), lte))
                    return true;
            }
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void Unlock(ref TKey key, LockType lockType)
        {
            if (Update(ref key, lte => { lte.logRecordInfo.Unlock(lockType); return lte; }))
                TryRemoveIfNoLocks(ref key);
            else
                Debug.Fail("Trying to unlock a nonexistent key");
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void TransferFrom(ref TKey key, RecordInfo logRecordInfo)
        {
            var keyContainer = GetKeyContainer(ref key);
            RecordInfo newRec = default;
            newRec.CopyLocksFrom(logRecordInfo);
            if (!dict.TryAdd(keyContainer, new(keyContainer, newRec, default)))
            {
                keyContainer.Dispose();
                Debug.Fail("Trying to Transfer to an existing key");
                return;
            }
            Interlocked.Increment(ref this.approxNumItems);
        }

        // Lock the LockTable record for the key if it exists, else add a Tentative record for it.
        // Returns true if the record was locked or tentative; else false (a Sealed or already-Tentative record was encountered)
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool LockOrTentative(ref TKey key, LockType lockType, out bool tentative)
        {
            var keyContainer = GetKeyContainer(ref key);
            bool existingConflict = false;
            var lte = dict.AddOrUpdate(keyContainer,
                key => {
                    RecordInfo lockRecordInfo = default;
                    lockRecordInfo.Tentative = true;
                    RecordInfo logRecordInfo = default;
                    existingConflict = !logRecordInfo.Lock(lockType);
                    Interlocked.Increment(ref this.approxNumItems);
                    return new(key, logRecordInfo, lockRecordInfo);
                }, (key, lte) => {
                    existingConflict = !lte.logRecordInfo.Lock(lockType);
                    if (lte.lockRecordInfo.Sealed)
                    {
                        existingConflict = true;
                        lte.logRecordInfo.Unlock(lockType);
                    }
                    return lte;
                });
            tentative = lte.lockRecordInfo.Tentative;
            return !existingConflict;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void ClearTentative(ref TKey key)
        {
            if (!Update(ref key, lte => { lte.lockRecordInfo.Tentative = false; return lte; }))
                Debug.Fail("Trying to remove Tentative bit from nonexistent locktable entry");
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void TryRemoveIfNoLocks(ref TKey key)
        {
            using var lookupKey = GetKeyContainer(ref key);

            // From https://devblogs.microsoft.com/pfxteam/little-known-gems-atomic-conditional-removals-from-concurrentdictionary/
            while (dict.TryGetValue(lookupKey, out var lte))
            {
                if (lte.lockRecordInfo.IsLocked || lte.lockRecordInfo.Sealed || lte.logRecordInfo.IsLocked)
                    return;
                if (dict.TryRemoveConditional(lookupKey, lte))
                {
                    Interlocked.Decrement(ref this.approxNumItems);
                    lte.key.Dispose();
                    return;
                }
            }
            // If we make it here, the key was already removed.
        }

        // False is legit, as the record may have been removed between the time it was known to be here and the time Seal was called.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool TrySeal(ref TKey key, out bool exists)
        {
            using var lookupKey = GetKeyContainer(ref key);
            if (!dict.ContainsKey(lookupKey))
            {
                exists = false;
                return true;
            }
            exists = true;
            return Update(ref key, lte => { lte.lockRecordInfo.Seal(); return lte; });
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void Unseal(ref TKey key)
        {
            if (!Update(ref key, lte => { lte.lockRecordInfo.Unseal(); return lte; }))
                Debug.Fail("Trying to Unseal nonexistent key");
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool Get(ref TKey key, out RecordInfo recordInfo)
        {
            using var lookupKey = GetKeyContainer(ref key);
            if (dict.TryGetValue(lookupKey, out var lte))
            {
                recordInfo = lte.logRecordInfo;
                return true;
            }
            recordInfo = default;
            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool Get(TKey key, out RecordInfo recordInfo) => Get(ref key, out recordInfo);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool ContainsKey(ref TKey key)
        {
            using var lookupKey = GetKeyContainer(ref key);
            return dict.ContainsKey(lookupKey);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool ApplyToLogRecord(ref TKey key, ref RecordInfo logRecord)
        {
            // This is called after the record has been CAS'd into the log or readcache, so this should not be allowed to fail.
            using var lookupKey = GetKeyContainer(ref key);
            if (dict.TryGetValue(lookupKey, out var lte))
            {
                Debug.Assert(lte.lockRecordInfo.Sealed, "lockRecordInfo should have been Sealed already");

                // If it's a Tentative record, ignore it--it will be removed by Lock() and retried against the inserted log record.
                if (lte.lockRecordInfo.Tentative)
                    return true;

                logRecord.CopyLocksFrom(lte.logRecordInfo);
                lte.lockRecordInfo.SetInvalid();
                lte.lockRecordInfo.Unseal();
                if (dict.TryRemove(lookupKey, out _))
                {
                    Interlocked.Decrement(ref this.approxNumItems);
                    lte.key.Dispose();
                }
                lte.lockRecordInfo.Tentative = false;
            }

            // No locks to apply, or we applied them all.
            return true;
        }

        public override string ToString() => this.dict.Count.ToString();
    }
}
