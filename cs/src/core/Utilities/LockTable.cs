// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

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

        readonly SafeConcurrentDictionary<IHeapContainer<TKey>, LockTableEntry<TKey>> dict;
        readonly IVariableLengthStruct<TKey> keyLen;
        readonly KeyComparer keyComparer;
        readonly SectorAlignedBufferPool bufferPool;

        internal LockTable(IVariableLengthStruct<TKey> keyLen, IFasterEqualityComparer<TKey> comparer, SectorAlignedBufferPool bufferPool)
        {
            this.keyLen = keyLen;
            this.keyComparer = new(comparer);
            this.bufferPool = bufferPool;
            this.dict = new(this.keyComparer);
        }

        internal bool IsActive => this.dict.Count > 0;

        IHeapContainer<TKey> GetKeyContainer(ref TKey key)
        {
            if (bufferPool is null)
                return new StandardHeapContainer<TKey>(ref key);
            return new VarLenHeapContainer<TKey>(ref key, keyLen, bufferPool);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void Lock(ref TKey key, LockType lockType)
        {
            var keyContainer = GetKeyContainer(ref key);
            _ = dict.AddOrUpdate(keyContainer,
                key => {
                    RecordInfo logRecordInfo = default;
                    logRecordInfo.Lock(lockType);
                    return new(key, logRecordInfo, default);
                }, (key, lte) => {
                    lte.logRecordInfo.Lock(lockType); 
                    return lte;
                });
        }

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
            if (Update(ref key, lte => { lte.lockRecordInfo.Unlock(lockType); return lte; }))
            {
                TryRemoveIfNoLocks(ref key);
                return;
            }
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
            }
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
                    logRecordInfo.Lock(lockType);
                    return new(key, logRecordInfo, lockRecordInfo);
                }, (key, lte) => {
                    if (lte.lockRecordInfo.Tentative || lte.lockRecordInfo.Sealed)
                        existingConflict = true;
                    lte.logRecordInfo.Lock(lockType);
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
        internal void UnlockOrClearTentative(ref TKey key, LockType lockType, bool wasTentative)
        {
            using var lookupKey = GetKeyContainer(ref key);
            if (dict.TryGetValue(lookupKey, out var lte))
            {
                Debug.Assert(wasTentative || !lte.lockRecordInfo.Tentative, "lockRecordInfo.Tentative was not expected");
                Debug.Assert(!lte.lockRecordInfo.Sealed, "lockRecordInfo.Sealed was not expected");

                // We assume that we own the lock or placed the Tentative record.
                if (!lte.lockRecordInfo.Tentative)
                    lte.lockRecordInfo.Unlock(lockType);
                if (!dict.TryRemove(lookupKey, out _))
                    Debug.Fail("Could not remove Tentative record");
                return;
            }
            Debug.Fail("Trying to UnlockOrClearTentative on nonexistent key");
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
                    lte.key.Dispose();
                    return;
                }
            }
            // If we make it here, the key was already removed.
        }

        // False is legit, as the record may have been removed between the time it was known to be here and the time Seal was called,
        // or this may be called by SealOrTentative.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool TrySeal(ref TKey key, out bool exists)
        {
            using var lookupKey = GetKeyContainer(ref key);
            if (!dict.ContainsKey(lookupKey))
            {
                exists = false;
                return true;
            }
            exists = false;
            return Update(ref key, lte => { lte.lockRecordInfo.Seal(); return lte; });
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void Unseal(ref TKey key)
        {
            if (!Update(ref key, lte => { lte.lockRecordInfo.Unseal(); return lte; }))
                Debug.Fail("Trying to remove Unseal nonexistent key");
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool TrySealOrTentative(ref TKey key, out bool tentative)
        {
            tentative = false;
            if (this.TrySeal(ref key, out bool exists))
                return true;
            if (exists)
                return false;

            var keyContainer = GetKeyContainer(ref key);
            RecordInfo lockRecordInfo = default;
            lockRecordInfo.Tentative = tentative = true;
            if (dict.TryAdd(keyContainer, new(keyContainer, default, lockRecordInfo)))
                return true;

            // Someone else already inserted a tentative record
            keyContainer.Dispose();
            return false;
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
        internal bool ContainsKey(ref TKey key)
        {
            using var lookupKey = GetKeyContainer(ref key);
            return dict.ContainsKey(lookupKey);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool ApplyToLogRecord(ref TKey key, ref RecordInfo logRecord)
        {
            using var lookupKey = GetKeyContainer(ref key);
            if (dict.TryGetValue(lookupKey, out var lte))
            {
                // If it's a Tentative record, ignore it--it will be removed by Lock() and retried against the inserted log record.
                if (lte.lockRecordInfo.Tentative)
                    return true;

                // If Sealing fails, we have to retry; it could mean that a pending read (readcache or copytotail) grabbed the locks
                // before the Upsert/etc. got to them. In that case, the upsert must retry so those locks will be drained from the
                // read entry. Note that Seal() momentarily xlocks the record being sealed, which in this case is the LockTable record;
                // this does not affect the lock count of the contained record.
                if (!lte.lockRecordInfo.Seal())
                    return false;

                logRecord.CopyLocksFrom(lte.logRecordInfo);
                lte.lockRecordInfo.SetInvalid();
                if (dict.TryRemove(lookupKey, out _))
                    lte.key.Dispose();
                lte.lockRecordInfo.Tentative = false;
            }

            // No locks to apply, or we applied them all.
            return true;
        }

        public override string ToString() => this.dict.Count.ToString();
    }
}
