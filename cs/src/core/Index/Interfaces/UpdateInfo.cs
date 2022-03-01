// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

namespace FASTER.core
{
    /// <summary>
    /// The type of session being used for this operation
    /// </summary>
    public enum SessionType : byte
    {
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
    public struct UpsertInfo
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
        /// The logical address of the record being operated on
        /// </summary>
        public long Address { get; internal set; }

        /// <summary>
        /// Whether FASTER should cancel the operation
        /// </summary>
        public bool CancelOperation { get; set; }

        /// <summary>
        /// Utility ctor
        /// </summary>
        public UpsertInfo(ref RMWInfo rmwInfo)
        {
            this.SessionType = rmwInfo.SessionType;
            this.Version = rmwInfo.Version;
            this.Address = rmwInfo.Address;
            this.CancelOperation = false;
        }
    }

    /// <summary>
    /// Information passed to <see cref="IFunctions{Key, Value, Input, Output, Context}"/> record-update callbacks. 
    /// </summary>
    public struct RMWInfo
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
        /// The logical address of the record being operated on
        /// </summary>
        public long Address { get; internal set; }

        /// <summary>
        /// Whether FASTER should cancel the operation
        /// </summary>
        public bool CancelOperation { get; set; }

        /// <summary>
        /// Whether FASTER should perform a Delete on the record
        /// </summary>
        public bool DeleteRecord { get; set; }
    }

    /// <summary>
    /// Information passed to <see cref="IFunctions{Key, Value, Input, Output, Context}"/> record-update callbacks. 
    /// </summary>
    public struct DeleteInfo
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
        /// The logical address of the record being operated on
        /// </summary>
        public long Address { get; internal set; }

        /// <summary>
        /// Whether FASTER should cancel the operation
        /// </summary>
        public bool CancelOperation { get; set; }
    }

    /// <summary>
    /// Information passed to <see cref="IFunctions{Key, Value, Input, Output, Context}"/> record-read callbacks. 
    /// </summary>
    public struct ReadInfo
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
        /// The logical address of the record being operated on
        /// </summary>
        public long Address { get; internal set; }

        /// <summary>
        /// Whether FASTER should cancel the operation
        /// </summary>
        public bool CancelOperation { get; set; }

        /// <summary>
        /// Whether FASTER should perform a Delete on the record
        /// </summary>
        public bool DeleteRecord { get; set; }
    }
}
