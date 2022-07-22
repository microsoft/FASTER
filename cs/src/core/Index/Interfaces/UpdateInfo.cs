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
    /// What actions to take following the RMW IFunctions method call, such as cancellation or record expiration.
    /// </summary>
    public enum UpsertAction
    {
        /// <summary>
        /// Execute the default action for the method 'false' return.
        /// </summary>
        Default,

        /// <summary>
        /// The operation cannot not be completed in-place because it would exceed available space.
        /// </summary>
        NeedMoreSpace,

        /// <summary>
        /// Stop the operation immediately and return.
        /// </summary>
        CancelOperation
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
        /// The ID of session context executing the operation
        /// </summary>
        public int SessionID { get; internal set; }

        /// <summary>
        /// The header of the record.
        /// </summary>
        public RecordInfo RecordInfo { get; internal set; }

        /// <summary>
        /// What actions FASTER should perform on a false return from the IFunctions method
        /// </summary>
        public UpsertAction Action { get; set; }

        /// <summary>
        /// Utility ctor
        /// </summary>
        public UpsertInfo(ref RMWInfo rmwInfo)
        {
            this.SessionType = rmwInfo.SessionType;
            this.Version = rmwInfo.Version;
            this.SessionID = rmwInfo.SessionID;
            this.Address = rmwInfo.Address;
            this.RecordInfo = default;
            this.Action = UpsertAction.Default;
        }
    }

    /// <summary>
    /// What actions to take following the RMW IFunctions method call, such as cancellation or record expiration.
    /// </summary>
    public enum RMWAction
    {
        /// <summary>
        /// Execute the default action for the method 'false' return.
        /// </summary>
        Default,

        /// <summary>
        /// The operation cannot not be completed in-place because it would exceed available space.
        /// </summary>
        NeedMoreSpace,

        /// <summary>
        /// Expire the record, including continuing actions to reinsert a new record with initial state.
        /// </summary>
        ExpireAndResume,

        /// <summary>
        /// Expire the record, and do not attempt to insert a new record with initial state.
        /// </summary>
        ExpireAndStop,

        /// <summary>
        /// Stop the operation immediately and return.
        /// </summary>
        CancelOperation
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
        /// The ID of session context executing the operation
        /// </summary>
        public int SessionID { get; internal set; }

        /// <summary>
        /// The header of the record.
        /// </summary>
        public RecordInfo RecordInfo { get; internal set; }

        /// <summary>
        /// What actions FASTER should perform on a false return from the IFunctions method
        /// </summary>
        public RMWAction Action { get; set; }
    }

    /// <summary>
    /// What actions to take following the RMW IFunctions method call, such as cancellation or record expiration.
    /// </summary>
    public enum DeleteAction
    {
        /// <summary>
        /// Execute the default action for the method 'false' return.
        /// </summary>
        Default,

        /// <summary>
        /// Stop the operation immediately and return.
        /// </summary>
        CancelOperation
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
        /// The header of the record.
        /// </summary>
        public RecordInfo RecordInfo { get; internal set; }

        /// <summary>
        /// What actions FASTER should perform on a false return from the IFunctions method
        /// </summary>
        public DeleteAction Action { get; set; }
    }

    /// <summary>
    /// What actions to take following the RMW IFunctions method call, such as cancellation or record expiration.
    /// </summary>
    public enum ReadAction
    {
        /// <summary>
        /// Execute the default action for the method 'false' return.
        /// </summary>
        Default,

        /// <summary>
        /// Expire the record. No subsequent actions are available for Read.
        /// </summary>
        Expire,

        /// <summary>
        /// Stop the operation immediately and return.
        /// </summary>
        CancelOperation
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
        /// The header of the record.
        /// </summary>
        public RecordInfo RecordInfo { get; internal set; }

        /// <summary>
        /// What actions FASTER should perform on a false return from the IFunctions method
        /// </summary>
        public ReadAction Action { get; set; }
    }
}
