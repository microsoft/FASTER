// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

namespace FASTER.core
{
    /// <summary>
    /// The type of session being used for this operation
    /// </summary>
    public enum SessionType : byte { 
        /// <summary>
        /// The standard client session, which does ephemeral locking and epoch protection on a per-operation basis.
        /// </summary>
        ClientSession,

        /// <summary>
        /// An unsafe context which does ephemeral locking but allows the user to do coarse-grained epoch protection,
        /// which can improve speed.
        /// </summary>
        UnsafeContext,

        /// <summary>
        /// An unsafe context that does no ephemeral locking; the application must lock and unlock records manually and 
        /// make its own epoch protection calls.
        /// </summary>
        LockableUnsafeContext
    }

    /// <summary>
    /// Information passed to <see cref="IFunctions{Key, Value, Input, Output, Context}"/> record-update callbacks. 
    /// </summary>
    public struct UpdateInfo
    {
        /// <summary>
        /// The type of session context executing the operation
        /// </summary>
        public SessionType SessionType { get; internal set; }

        /// <summary>
        /// The FASTER execution context version of the operation
        /// </summary>
        public long Version { get; internal set; }

        /// <summary>
        /// The length of data in the value that the is in use. Incoming, it is set by FASTER to the result of <see cref="IVariableLengthStruct{T, Input}.GetLength(ref T, ref Input)"/>.
        /// If an application wants to allow data to shrink and then grow again within the same record, it must set this to the correct length on output. 
        /// </summary>
        public int UsedValueLength { get; set; }

        /// <summary>
        /// The allocated length of the record value.
        /// </summary>
        public int FullValueLength { get; internal set; }

        /// <summary>
        /// If true, the IFunctions method is causing a Delete of the record.
        /// </summary>
        public bool IsDeleted { get; set; }
    }
}
