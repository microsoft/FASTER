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

    /// <summary>
    /// Information returned from <see cref="ManualFasterOperations{Key, Value, Input, Output, Context, Functions}.Lock(Key, LockType, bool, ref LockInfo)"/>
    /// </summary>
    public struct LockInfo
    {
        /// <summary>
        /// The type of lock that was acquired
        /// </summary>
        public LockType LockType;

        /// <summary>
        /// The address of the record that was locked. Useful for calling <see cref="ClientSession{Key, Value, Input, Output, Context, Functions}.ReadAtAddress(long, ref Input, ref Output, ReadFlags, Context, long)"/>
        /// </summary>
        public long Address;

        /// <inheritdoc/>
        public override string ToString() => $"{LockType}: addr {Address}";
    }

    internal enum LockOperationType : byte
    {
        None,
        LockRead,
        LockUpsert,
        Unlock 
    }

    internal struct LockOperation
    {
        internal LockType LockType;
        internal LockOperationType LockOperationType;
        internal bool IsStubPromotion;

        internal bool IsSet => LockOperationType != LockOperationType.None;

        internal LockOperation(LockOperationType opType, LockType lockType)
        {
            this.LockType = lockType;
            this.LockOperationType = opType;
            this.IsStubPromotion = false;
        }

        public override string ToString() => $"{LockType}: opType {LockOperationType}, isStubPromo {IsStubPromotion}";
    }
}
