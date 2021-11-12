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
        /// Shared lock, taken on Read
        /// </summary>
        Shared,

        /// <summary>
        /// Exclusive lock, taken on Upsert, RMW, or Delete
        /// </summary>
        Exclusive,

        /// <summary>
        /// Promote a Shared lock to an Exclusive lock
        /// </summary>
        ExclusiveFromShared
    }

    internal enum LockOperationType : byte
    {
        None,
        Lock,
        Unlock 
    }

    internal struct LockOperation
    {
        internal LockOperationType LockOperationType;
        internal LockType LockType;
        internal long LockContext;

        internal bool IsSet => LockOperationType != LockOperationType.None;

        internal LockOperation(LockOperationType opType, LockType lockType)
        {
            this.LockOperationType = opType;
            this.LockType = lockType;
            this.LockContext = default;
        }
    }
}
