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
        /// Shared lock, taken on Read
        /// </summary>
        Shared,

        /// <summary>
        /// Exclusive lock, taken on Upsert, RMW, or Delete
        /// </summary>
        Exclusive
    }

    /// <summary>
    /// How FASTER should do record locking
    /// </summary>
    public enum LockingMode : byte
    {
        /// <summary>
        /// Locking may be manual or ephemeral; a separate lock table is used rather than the <see cref="RecordInfo"/>.
        /// </summary>
        SessionControlled,

        /// <summary>
        /// Only ephemeral locking is done; all locks are in-memory via the <see cref="RecordInfo"/>.
        /// </summary>
        EphemeralOnly,

        /// <summary>
        /// Ephemeral locking is disabled in FASTER; only manual locking via a separate lock table is done.
        /// </summary>
        Disabled
    }

    /// <summary>
    /// Lock state of a record
    /// </summary>
    internal struct LockState
    {
        internal bool IsLockedExclusive;
        internal bool IsFound;
        internal ushort NumLockedShared;
        internal bool IsLockedShared => NumLockedShared > 0;

        internal bool IsLocked => IsLockedExclusive || NumLockedShared > 0;
    }

    internal enum LockOperationType : byte
    {
        None,
        Lock,
        Unlock,
        IsLocked
    }

    internal struct LockOperation
    {
        internal LockType LockType;
        internal LockOperationType LockOperationType;

        internal bool IsSet => LockOperationType != LockOperationType.None;

        internal LockOperation(LockOperationType opType, LockType lockType)
        {
            this.LockType = lockType;
            this.LockOperationType = opType;
        }

        public override string ToString() => $"{LockType}: opType {LockOperationType}";
    }
}
