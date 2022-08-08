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
        Exclusive,

        /// <summary>
        /// Watch Bit, taken on manually
        /// </summary>
        Watch,


        /// <summary>
        /// Promote a Shared lock to an Exclusive lock
        /// </summary>
        ExclusiveFromShared
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
