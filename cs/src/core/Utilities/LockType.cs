// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

namespace FASTER.core
{
    /// <summary>
    /// Type of lock taken by FASTER on Read, Upsert, RMW, or Delete operations, either directly or within concurrent callback operations
    /// </summary>
    public enum LockType : byte
    {
        /// <summary>
        /// No lock
        /// </summary>
        None,

        /// <summary>
        /// Exclusive lock, taken on Upsert, RMW, or Delete
        /// </summary>
        Exclusive,

        /// <summary>
        /// Shared lock, taken on Read
        /// </summary>
        Shared
    }

    /// <summary>
    /// How FASTER should do record locking
    /// </summary>
    public enum ConcurrencyControlMode : byte
    {
        /// <summary>
        /// Keys are locked using a LockTable. Currently the implementation latches the hash index buckets. Supports manual and transient locking, based on the session type.
        /// </summary>
        LockTable,

        /// <summary>
        /// Records are locked only for the duration of a concurrent IFunctions call (one that operates on data in the mutable region of the log), using the RecordInfo header.
        /// </summary>
        RecordIsolation,

        /// <summary>
        /// Locking is not done in FASTER.
        /// </summary>
        None
    }

    internal enum EphemeralLockResult
    {
        None,           // Default; not tried
        Success,        // Lock succeeded
        Failed,         // Lock failed due to timeout; must do RETRY_LATER
        HoldForSeal     // Lock succeeded, but was not unlocked because the user's IFunctions method requires a read-copy-update
    }

    /// <summary>
    /// Interface that must be implemented to participate in keycode-based locking.
    /// </summary>
    public interface ILockableKey
    {
        /// <summary>
        /// The hash code for a specific key, obtained from <see cref="IFasterContext{TKey}.GetKeyHash(ref TKey)"/>
        /// </summary>
        public long KeyHash { get; }

        /// <summary>
        /// The lock type for a specific key
        /// </summary>
        public LockType LockType { get; }
    }

    /// <summary>
    /// A utility class to carry a fixed-length key (blittable or object type) and its assciated info for Locking
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    public struct FixedLengthLockableKeyStruct<TKey> : ILockableKey
    {
        /// <summary>
        /// The key that is acquiring or releasing a lock
        /// </summary>
        public TKey Key;

        #region ILockableKey
        /// <inheritdoc/>
        public long KeyHash { get; set; }

        /// <inheritdoc/>
        public LockType LockType { get; set; }
        #endregion ILockableKey

        /// <summary>
        /// Constructor
        /// </summary>
        public FixedLengthLockableKeyStruct(TKey key, LockType lockType, IFasterContext<TKey> context) : this(ref key, lockType, context) { }

        /// <summary>
        /// Constructor
        /// </summary>
        public FixedLengthLockableKeyStruct(ref TKey key, LockType lockType, IFasterContext<TKey> context)
        {
            Key = key;
            LockType = lockType;
            KeyHash = context.GetKeyHash(ref key);
        }
        /// <summary>
        /// Constructor
        /// </summary>
        public FixedLengthLockableKeyStruct(TKey key, long keyHash, LockType lockType, ILockableContext<TKey> context) : this(ref key, keyHash, lockType, context) { }

        /// <summary>
        /// Constructor
        /// </summary>
        public FixedLengthLockableKeyStruct(ref TKey key, long keyHash, LockType lockType, ILockableContext<TKey> context)
        {
            Key = key;
            KeyHash = keyHash;
            LockType = lockType;
        }

        /// <summary>
        /// Sort the passed key array for use in <see cref="ILockableContext{TKey}.Lock{TLockableKey}(TLockableKey[])"/>
        /// and <see cref="ILockableContext{TKey}.Unlock{TLockableKey}(TLockableKey[])"/>
        /// </summary>
        /// <param name="keys"></param>
        /// <param name="context"></param>
        public static void Sort(FixedLengthLockableKeyStruct<TKey>[] keys, ILockableContext<TKey> context) => context.SortKeyHashes(keys);

        /// <inheritdoc/>
        public override readonly string ToString()
        {
            var hashStr = Utility.GetHashString(this.KeyHash);
            return $"key {Key}, hash {hashStr}, {LockType}";
        }
    }

    /// <summary>
    /// Lock state of a record
    /// </summary>
    internal struct LockState
    {
        internal bool IsLockedExclusive;
        internal bool IsFound;
        internal ushort NumLockedShared;
        internal readonly bool IsLockedShared => NumLockedShared > 0;

        internal readonly bool IsLocked => IsLockedExclusive || NumLockedShared > 0;

        public override readonly string ToString()
        {
            var locks = $"{(this.IsLockedExclusive ? "x" : string.Empty)}{this.NumLockedShared}";
            return $"found {IsFound}, locks {locks}";
        }
    }

    internal enum LockOperationType : byte
    {
        None,
        Lock,
        Unlock
    }

    internal struct LockOperation
    {
        internal LockType LockType;
        internal LockOperationType LockOperationType;

        internal readonly bool IsSet => LockOperationType != LockOperationType.None;

        internal LockOperation(LockOperationType opType, LockType lockType)
        {
            this.LockType = lockType;
            this.LockOperationType = opType;
        }

        public override readonly string ToString() => $"{LockType}: opType {LockOperationType}";
    }
}
