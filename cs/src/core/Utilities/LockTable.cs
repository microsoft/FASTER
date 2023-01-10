// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core.Utilities;
using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;

namespace FASTER.core
{
    internal class LockTable<TKey> : IDisposable
    {
        #region IInMemKVUserFunctions implementation
        internal class LockTableFunctions : IInMemKVUserFunctions<TKey, IHeapContainer<TKey>, RecordInfo>, IDisposable
        {
            private readonly IFasterEqualityComparer<TKey> keyComparer;
            private readonly IVariableLengthStruct<TKey> keyLen;
            private readonly SectorAlignedBufferPool bufferPool;

            internal LockTableFunctions(IFasterEqualityComparer<TKey> keyComparer, IVariableLengthStruct<TKey> keyLen)
            {
                this.keyComparer = keyComparer;
                this.keyLen = keyLen;
                if (keyLen is not null)
                    this.bufferPool = new SectorAlignedBufferPool(1, 1);
            }

            public IHeapContainer<TKey> CreateHeapKey(ref TKey key)
                => bufferPool is null ? new StandardHeapContainer<TKey>(ref key) : new VarLenHeapContainer<TKey>(ref key, keyLen, bufferPool);

            public ref TKey GetHeapKeyRef(IHeapContainer<TKey> heapKey) => ref heapKey.Get();

            public bool Equals(ref TKey key, IHeapContainer<TKey> heapKey) => keyComparer.Equals(ref key, ref heapKey.Get());

            public long GetHashCode64(ref TKey key) => keyComparer.GetHashCode64(ref key);

            public bool IsActive(ref RecordInfo recordInfo) => recordInfo.IsLocked;

            public void Dispose(ref IHeapContainer<TKey> key, ref RecordInfo recordInfo)
            {
                key?.Dispose();
                key = default;
                recordInfo = default;
            }

            public void Dispose()
            {
                this.bufferPool?.Free();
            }
        }
        #endregion IInMemKVUserFunctions implementation

        internal readonly InMemKV<TKey, IHeapContainer<TKey>, RecordInfo, LockTableFunctions> kv;
        internal readonly LockTableFunctions functions;

        internal LockTable(int numBuckets, IFasterEqualityComparer<TKey> keyComparer, IVariableLengthStruct<TKey> keyLen)
        {
            this.functions = new(keyComparer, keyLen);
            this.kv = new InMemKV<TKey, IHeapContainer<TKey>, RecordInfo, LockTableFunctions>(numBuckets, numBuckets >> 4, this.functions);
        }

        public bool IsActive => kv.IsActive;

        /// <summary>
        /// Try to lock the key for an ephemeral operation; if there is no lock, return without locking and let 2p insert handle it.
        /// </summary>
        /// <param name="key">The key to lock</param>
        /// <param name="hash">The hash code of the key to lock, to avoid recalculating</param>
        /// <param name="lockType">The lock type to acquire, if the key is found</param>
        /// <param name="gotLock">Returns true if we got the requested lock; the caller must unlock</param>
        /// <returns>True if either lock was acquired or the entry was not found; false if lock acquisition failed</returns>
        /// <remarks>Ephemeral locks only lock if an entry exists; two-phase insertion/locking will ensure we don't have a conflict 
        ///     if there is not already an entry there.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryLockEphemeral(ref TKey key, long hash, LockType lockType, out bool gotLock)
        {
            var funcs = new FindEntryFunctions_EphemeralLock(lockType);
            kv.FindEntry(ref key, hash, ref funcs);
            gotLock = funcs.gotLock;
            return funcs.success;
        }

        internal struct FindEntryFunctions_EphemeralLock : InMemKV<TKey, IHeapContainer<TKey>, RecordInfo, LockTableFunctions>.IFindEntryFunctions
        {
            readonly LockType lockType;
            internal bool success, gotLock;

            internal FindEntryFunctions_EphemeralLock(LockType lockType)
            {
                this.lockType = lockType;
                this.success = this.gotLock = false;
            }

            public void NotFound(ref TKey key) => success = true;

            public void FoundEntry(ref TKey key, ref RecordInfo recordInfo) => success = gotLock = recordInfo.TryLock(lockType);
        }

        /// <summary>
        /// Try to acquire a manual lock, either by locking existing LockTable record or adding a new record.
        /// </summary>
        /// <param name="key">The key to lock</param>
        /// <param name="hash">The hash code of the key to lock, to avoid recalculating</param>
        /// <param name="lockType">The lock type to acquire, if the key is found</param>
        /// <param name="isTentative">If true, the lock should be acquired tentatively, as part of two-phase insertion/locking</param>
        /// <returns>True if the lock was acquired; false if lock acquisition failed</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TryLockManual(ref TKey key, long hash, LockType lockType, out bool isTentative)
        {
            var funcs = new FindOrAddEntryFunctions_ManualLock(lockType, isTentative: true);
            kv.FindOrAddEntry(ref key, hash, ref funcs);
            isTentative = funcs.isTentative;    // funcs.isTentative is in/out
            return funcs.success;
        }

        internal struct FindOrAddEntryFunctions_ManualLock : InMemKV<TKey, IHeapContainer<TKey>, RecordInfo, LockTableFunctions>.IFindOrAddEntryFunctions
        {
            readonly LockType lockType;
            internal bool success, isTentative; // isTentative is in/out

            internal FindOrAddEntryFunctions_ManualLock(LockType lockType, bool isTentative)
            {
                this.lockType = lockType;
                this.isTentative = isTentative;
                this.success = false;
            }

            public void FoundEntry(ref TKey key, ref RecordInfo recordInfo)
            {
                // If the entry is already there, we lock it non-tentatively
                success = recordInfo.TryLock(lockType);
                isTentative = false;
            }

            public void AddedEntry(ref TKey key, ref RecordInfo recordInfo)
            {
                recordInfo.InitializeLock(lockType);
                recordInfo.Valid = true;
                recordInfo.Tentative = isTentative;
                success = true;
            }
        }

        /// <summary>
        /// Unlock the key with the specified lock type
        /// </summary>
        /// <param name="key">The key to unlock</param>
        /// <param name="hash">The hash code of the key to lock, to avoid recalculating</param>
        /// <param name="lockType">The lock type--shared or exclusive</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Unlock(ref TKey key, long hash, LockType lockType)
        {
            var funcs = new FindEntryFunctions_Unlock(lockType);
            kv.FindEntry(ref key, hash, ref funcs);

            // success is false if the key was not found, or if the record was marked invalid.
            return funcs.success;
        }

        internal struct FindEntryFunctions_Unlock : InMemKV<TKey, IHeapContainer<TKey>, RecordInfo, LockTableFunctions>.IFindEntryFunctions
        {
            readonly LockType lockType;
            readonly bool wasTentative;
            internal bool success;

            internal FindEntryFunctions_Unlock(LockType lockType, bool wasTentative = false)
            {
                this.lockType = lockType;
                this.wasTentative = wasTentative;
                success = false;
            }

            public void NotFound(ref TKey key) => success = false;

            public void FoundEntry(ref TKey key, ref RecordInfo recordInfo)
            {
                Debug.Assert(wasTentative == recordInfo.Tentative, "entry.recordInfo.Tentative was not as expected");
                Debug.Assert(!recordInfo.Tentative || recordInfo.IsLockedExclusive, "A Tentative entry should be X locked");
                success = recordInfo.TryUnlock(lockType);
            }
        }

        /// <summary>
        /// Remove the key from the lockTable, if it exists. Called after a record is transferred.
        /// </summary>
        /// <param name="key">The key to unlock</param>
        /// <param name="hash">The hash code of the key to lock, to avoid recalculating</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Remove(ref TKey key, long hash)
        {
            var funcs = new FindEntryFunctions_Remove();
            kv.FindEntry(ref key, hash, ref funcs);
            return funcs.wasFound;
        }

        internal struct FindEntryFunctions_Remove : InMemKV<TKey, IHeapContainer<TKey>, RecordInfo, LockTableFunctions>.IFindEntryFunctions
        {
            internal bool wasFound;

            public void NotFound(ref TKey key) => wasFound = false;

            public void FoundEntry(ref TKey key, ref RecordInfo recordInfo)
            {
                recordInfo.ClearLocks();
                wasFound = true;
            }
        }

        /// <summary>
        /// Transfer locks from the record into the lock table.
        /// </summary>
        /// <param name="key">The key to unlock</param>
        /// <param name="logRecordInfo">The log record to copy from</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void TransferFromLogRecord(ref TKey key, RecordInfo logRecordInfo)
        {
            // This is called from record eviction, which doesn't have a hashcode available, so we have to calculate it here.
            long hash = functions.GetHashCode64(ref key);
            var funcs = new AddEntryFunctions_TransferFromLogRecord(logRecordInfo);
            kv.AddEntry(ref key, hash, ref funcs);
        }

        internal struct AddEntryFunctions_TransferFromLogRecord : InMemKV<TKey, IHeapContainer<TKey>, RecordInfo, LockTableFunctions>.IAddEntryFunctions
        {
            internal RecordInfo fromRecordInfo;

            internal AddEntryFunctions_TransferFromLogRecord(RecordInfo fromRI) => fromRecordInfo = fromRI;

            public void AddedEntry(ref TKey key, ref RecordInfo recordInfo)
            {
                recordInfo.TransferLocksFrom(ref fromRecordInfo);
                recordInfo.Valid = true;
            }
        }

        /// <summary>
        /// Transfer locks from the record into the lock table.
        /// </summary>
        /// <param name="key">The key to unlock</param>
        /// <param name="hash">The hash code of the key to lock, to avoid recalculating</param>
        /// <param name="logRecordInfo">The log record to copy from</param>
        /// <returns>Returns whether the entry was found</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool TransferToLogRecord(ref TKey key, long hash, ref RecordInfo logRecordInfo)
        {
            Debug.Assert(logRecordInfo.Tentative, "Must retain tentative flag until locks are transferred");
            var funcs = new FindEntryFunctions_TransferToLogRecord(logRecordInfo);
            kv.FindEntry(ref key, hash, ref funcs);
            logRecordInfo = funcs.toRecordInfo;
            return funcs.wasFound;
        }

        internal struct FindEntryFunctions_TransferToLogRecord : InMemKV<TKey, IHeapContainer<TKey>, RecordInfo, LockTableFunctions>.IFindEntryFunctions
        {
            internal RecordInfo toRecordInfo;
            internal bool wasFound;

            internal FindEntryFunctions_TransferToLogRecord(RecordInfo toRI)
            {
                this.toRecordInfo = toRI;
                wasFound = false;
            }

            public void NotFound(ref TKey key) { }

            public void FoundEntry(ref TKey key, ref RecordInfo logRecordInfo)
            {
                toRecordInfo.TransferLocksFrom(ref logRecordInfo);
                wasFound = true;
            }
        }

        /// <summary>
        /// Clear the Tentative bit from the key's lock--make it "real"
        /// </summary>
        /// <param name="key">The key to unlock</param>
        /// <param name="hash">The hash code of the key to lock, to avoid recalculating</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool ClearTentativeBit(ref TKey key, long hash)
        {
            var funcs = new FindEntryFunctions_ClearTentative();
            kv.FindEntry(ref key, hash, ref funcs);
            return funcs.success;
        }

        internal struct FindEntryFunctions_ClearTentative : InMemKV<TKey, IHeapContainer<TKey>, RecordInfo, LockTableFunctions>.IFindEntryFunctions
        {
            internal bool success;

            public void NotFound(ref TKey key) => success = false;

            public void FoundEntry(ref TKey key, ref RecordInfo recordInfo)
            {
                Debug.Assert(recordInfo.Tentative, "ClearTentative should only be called for a tentative record");
                recordInfo.ClearTentativeBitAtomic();
                success = true;
            }
        }

        /// <summary>
        /// Unlock the key, or remove a tentative entry that was added. This is called when the caller is abandoning the current attempt and will retry.
        /// </summary>
        /// <param name="key">The key to unlock</param>
        /// <param name="hash">The hash code of the key to lock, to avoid recalculating</param>
        /// <param name="lockType">The lock type--shared or exclusive</param>
        /// <param name="wasTentative">Whether or not we set a Tentative lock for it</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void UnlockOrRemoveTentativeEntry(ref TKey key, long hash, LockType lockType, bool wasTentative)
        {
            var funcs = new FindEntryFunctions_Unlock(lockType, wasTentative);
            kv.FindEntry(ref key, hash, ref funcs);
            Debug.Assert(funcs.success, "UnlockOrRemoveTentative should always find the entry");
        }

        /// <summary>
        /// Returns whether the two-phase Update protocol completes successfully: wait for any tentative lock on the key (it will either be cleared by
        /// the owner or the record will be removed), and return false if a non-tentative lock was found.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool CompleteTwoPhaseUpdate(ref TKey key, long hash)
        {
            var funcs = new FindEntryFunctions_CompleteTwoPhaseUpdate();
            while (true)
            {
                funcs.tentativeFound = false;
                kv.FindEntry(ref key, hash, ref funcs);
                if (funcs.notFound)
                    return true;
                if (!funcs.tentativeFound)
                    break;
                Thread.Yield();
            }

            // A non-tentative lock was found.
            return false;
        }

        internal struct FindEntryFunctions_CompleteTwoPhaseUpdate : InMemKV<TKey, IHeapContainer<TKey>, RecordInfo, LockTableFunctions>.IFindEntryFunctions
        {
            internal bool notFound, tentativeFound;

            public void NotFound(ref TKey key) => notFound = true;

            public void FoundEntry(ref TKey key, ref RecordInfo recordInfo) => tentativeFound = recordInfo.Tentative;
        }

        /// <summary>
        /// Returns whether the two-phase CopyToTail protocol completes successfully: wait for any tentative lock on the key (it will either be cleared by
        /// the owner or the record will be removed), return false if an exclusive lock is found, or transfer any read locks.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool CompleteTwoPhaseCopyToTail(ref TKey key, long hash, ref RecordInfo logRecordInfo, bool allowXLock, bool removeEphemeralLock)
        {
            Debug.Assert(logRecordInfo.Tentative, "Must retain tentative flag until locks are transferred");
            var funcs = new FindEntryFunctions_CompleteTwoPhaseCopyToTail(logRecordInfo, allowXLock, removeEphemeralLock);
            kv.FindEntry(ref key, hash, ref funcs);
            logRecordInfo = funcs.toRecordInfo;
            return funcs.success;
        }

        internal struct FindEntryFunctions_CompleteTwoPhaseCopyToTail : InMemKV<TKey, IHeapContainer<TKey>, RecordInfo, LockTableFunctions>.IFindEntryFunctions
        {
            internal RecordInfo toRecordInfo;   // TODO: C# 11 will let this be a ref field
            private readonly bool allowXLock, removeEphemeralLock;
            internal bool success;

            internal FindEntryFunctions_CompleteTwoPhaseCopyToTail(RecordInfo toRecordInfo, bool allowXLock, bool removeEphemeralLock)
            {
                this.toRecordInfo = toRecordInfo;
                this.allowXLock = allowXLock;
                this.removeEphemeralLock = removeEphemeralLock;
                success = false;
            }

            public void NotFound(ref TKey key) => success = true;

            public void FoundEntry(ref TKey key, ref RecordInfo logRecordInfo) 
                => success = toRecordInfo.TransferReadLocksFromAndMarkSourceAtomic(ref logRecordInfo, allowXLock, seal: false, this.removeEphemeralLock);
        }

        /// <summary>
        /// Test whether a key is present in the Lock Table. In production code, this is used to implement 
        /// <see cref="FasterKV{Key, Value}.SpinWaitUntilRecordIsClosed(ref Key, long, long, AllocatorBase{Key, Value})"/>.
        /// </summary>
        /// <param name="key">The key to unlock</param>
        /// <param name="hash">The hash code of the key to lock, to avoid recalculating</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool ContainsKey(ref TKey key, long hash) => kv.ContainsKey(ref key, hash);

        public void Dispose()
        {
            kv.Dispose();
            functions.Dispose();
        }

        #region Internal methods for Test
        internal bool HasEntries(ref TKey key) => kv.HasEntries(ref key);
        internal bool HasEntries(long hash) => kv.HasEntries(hash);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool TryGet(ref TKey key, out RecordInfo recordInfo) => TryGet(ref key, this.functions.GetHashCode64(ref key), out recordInfo);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool TryGet(ref TKey key, long hash, out RecordInfo recordInfo) => kv.TryGet(ref key, hash, out recordInfo);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool IsLocked(ref TKey key, long hash) => TryGet(ref key, hash, out var recordInfo) && recordInfo.IsLocked;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool IsLockedShared(ref TKey key, long hash) => TryGet(ref key, hash, out var recordInfo) && recordInfo.IsLockedShared;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool IsLockedExclusive(ref TKey key, long hash) => TryGet(ref key, hash, out var recordInfo) && recordInfo.IsLockedExclusive;
        #endregion Internal methods for Test
    }
}
