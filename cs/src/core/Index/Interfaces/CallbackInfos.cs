// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using static FASTER.core.Utility;
using System.Runtime.CompilerServices;
using System;
using System.Diagnostics;

namespace FASTER.core
{
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
        /// The FASTER execution context version of the operation
        /// </summary>
        public long Version { get; internal set; }

        /// <summary>
        /// The logical address of the record being operated on
        /// </summary>
        public long Address { get; internal set; }

        /// <summary>
        /// Hash code of key being operated on
        /// </summary>
        public long KeyHash { get; internal set; }

        /// <summary>
        /// The ID of session context executing the operation
        /// </summary>
        public int SessionID { get; internal set; }

        /// <summary>
        /// The header of the record.
        /// </summary>
        public readonly unsafe RecordInfo RecordInfo => *(RecordInfo*)recordInfoPtr;

        /// <summary>
        /// The length of data in the value that is in use. Incoming, it is set by FASTER to the result of <see cref="IVariableLengthStruct{T, Input}.GetLength(ref T, ref Input)"/>.
        /// If an application wants to allow data to shrink and then grow again within the same record, it must set this to the correct length on output. 
        /// </summary>
        public int UsedValueLength { get; set; }

        /// <summary>
        /// The allocated length of the record value.
        /// </summary>
        public int FullValueLength { get; internal set; }

        /// <summary>
        /// What actions FASTER should perform on a false return from the IFunctions method
        /// </summary>
        public UpsertAction Action { get; set; }

        // The physical address of the RecordInfo start. Do not use as a basis for key or value address; used only to produce 'ref recordInfo' for managing dynamic lengths,
        // and therefore must be a log address (with epoch protection), allocator-controlled, IO-record address, or pinned.
        internal IntPtr recordInfoPtr;
        internal readonly unsafe ref RecordInfo RecordInfoRef => ref Unsafe.AsRef<RecordInfo>((void*)recordInfoPtr);
        internal unsafe void SetRecordInfoAddress(ref RecordInfo recordInfo) => this.recordInfoPtr = (IntPtr)Unsafe.AsPointer(ref recordInfo);

        /// <summary>
        /// Utility ctor
        /// </summary>
        public UpsertInfo(ref RMWInfo rmwInfo)
        {
            this.Version = rmwInfo.Version;
            this.SessionID = rmwInfo.SessionID;
            this.Address = rmwInfo.Address;
            this.KeyHash = rmwInfo.KeyHash;
            this.recordInfoPtr = rmwInfo.recordInfoPtr;
            this.Action = UpsertAction.Default;
        }

        /// <summary>
        /// Retrieve the extra value length from the record, if present, and then clear it to ensure consistent log scan during in-place update.
        /// </summary>
        /// <param name="recordValue">Reference to the record value</param>
        /// <param name="usedValueLength">The currently-used length of the record value</param>
        /// <typeparam name="TValue">The type of the value</typeparam>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe readonly void ClearExtraValueLength<TValue>(ref TValue recordValue, int usedValueLength)
        {
            Debug.Assert(usedValueLength == this.UsedValueLength, $"UpsertInfo: usedValueLength ({usedValueLength}) != this.UsedValueLength ({this.UsedValueLength})");
            ClearExtraValueLength(recordInfoPtr, ref recordValue, usedValueLength);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static unsafe void ClearExtraValueLength<TValue>(IntPtr recordInfoPtr, ref TValue recordValue, int usedValueLength) 
            => ClearExtraValueLength(ref Unsafe.AsRef<RecordInfo>((void*)recordInfoPtr), ref recordValue, usedValueLength);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static unsafe void ClearExtraValueLength<TValue>(ref RecordInfo recordInfo, ref TValue recordValue, int usedValueLength)
        {
            if (!recordInfo.Filler)
                return;

            var valueAddress = (long)Unsafe.AsPointer(ref recordValue);
            int* extraLengthPtr = (int*)(valueAddress + RoundUp(usedValueLength, sizeof(int)));

            *extraLengthPtr = 0;
            recordInfo.Filler = false;
        }

        /// <summary>
        /// Set the extra value length, if any, into the record past the used value length.
        /// </summary>
        /// <param name="recordValue">Reference to the record value</param>
        /// <param name="usedValueLength">The currently-used length of the record value</param>
        /// <typeparam name="TValue">The type of the value</typeparam>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe void SetUsedValueLength<TValue>(ref TValue recordValue, int usedValueLength)
        {
            SetUsedValueLength(recordInfoPtr, (long)Unsafe.AsPointer(ref recordValue), usedValueLength, this.FullValueLength);
            this.UsedValueLength = usedValueLength;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static unsafe void SetUsedValueLength(IntPtr recordInfoPtr, long valueAddress, int usedValueLength, int fullValueLength)
        {
            Debug.Assert(!((RecordInfo*)recordInfoPtr)->Filler, "Filler should have been cleared by ClearExtraValueLength()");

            usedValueLength = RoundUp(usedValueLength, sizeof(int));
            int extraValueLength = fullValueLength - usedValueLength;
            if (extraValueLength >= sizeof(int))
            {
                int* extraValueLengthPtr = (int*)(valueAddress + usedValueLength);
                Debug.Assert(*extraValueLengthPtr == 0 || *extraValueLengthPtr == extraValueLength, "existing ExtraValueLength should be 0 or the same value");
                *extraValueLengthPtr = extraValueLength;
                ((RecordInfo*)recordInfoPtr)->Filler = true;
            }
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
        /// The FASTER execution context version of the operation
        /// </summary>
        public long Version { get; internal set; }

        /// <summary>
        /// The logical address of the record being operated on. For CopyUpdater, this is the source address,
        /// or <see cref="Constants.kInvalidAddress"/> if the source is the read cache.
        /// </summary>
        public long Address { get; internal set; }

        /// <summary>
        /// Hash code of key being operated on
        /// </summary>
        public long KeyHash { get; internal set; }

        /// <summary>
        /// The ID of session context executing the operation
        /// </summary>
        public int SessionID { get; internal set; }

        /// <summary>
        /// The header of the record.
        /// </summary>
        public readonly unsafe RecordInfo RecordInfo => *(RecordInfo*)recordInfoPtr;

        /// <summary>
        /// The length of data in the value that is in use. Incoming, it is set by FASTER to the result of <see cref="IVariableLengthStruct{T, Input}.GetLength(ref T, ref Input)"/>.
        /// If an application wants to allow data to shrink and then grow again within the same record, it must set this to the correct length on output. 
        /// </summary>
        public int UsedValueLength { get; set; }

        /// <summary>
        /// The allocated length of the record value.
        /// </summary>
        public int FullValueLength { get; internal set; }

        /// <summary>
        /// What actions FASTER should perform on a false return from the IFunctions method
        /// </summary>
        public RMWAction Action { get; set; }

        // The physical address of the RecordInfo start. See UpsertInfo for detailed comments.
        internal IntPtr recordInfoPtr;
        internal readonly unsafe ref RecordInfo RecordInfoRef => ref Unsafe.AsRef<RecordInfo>((void*)recordInfoPtr);
        internal unsafe void SetRecordInfoAddress(ref RecordInfo recordInfo) => this.recordInfoPtr = (IntPtr)Unsafe.AsPointer(ref recordInfo);
        internal void ClearRecordInfoAddress() => this.recordInfoPtr = IntPtr.Zero;

        /// <summary>
        /// Retrieve the extra value length from the record, if present, and then clear it to ensure consistent log scan during in-place update.
        /// </summary>
        /// <param name="recordValue">Reference to the record value</param>
        /// <param name="usedValueLength">The currently-used length of the record value</param>
        /// <typeparam name="TValue">The type of the value</typeparam>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe readonly void ClearExtraValueLength<TValue>(ref TValue recordValue, int usedValueLength)
        {
            Debug.Assert(usedValueLength == this.UsedValueLength, $"RMWInfo: usedValueLength ({usedValueLength}) != this.UsedValueLength ({this.UsedValueLength})");
            UpsertInfo.ClearExtraValueLength(recordInfoPtr, ref recordValue, usedValueLength);
        }

        /// <summary>
        /// Set the extra value length, if any, into the record past the used value length.
        /// </summary>
        /// <param name="recordValue">Reference to the record value</param>
        /// <param name="usedValueLength">The currently-used length of the record value</param>
        /// <typeparam name="TValue">The type of the value</typeparam>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe void SetUsedValueLength<TValue>(ref TValue recordValue, int usedValueLength)
        {
            UpsertInfo.SetUsedValueLength(recordInfoPtr, (long)Unsafe.AsPointer(ref recordValue), usedValueLength, this.FullValueLength);
            this.UsedValueLength = usedValueLength;
        }
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
        /// The FASTER execution context version of the operation
        /// </summary>
        public long Version { get; internal set; }

        /// <summary>
        /// The logical address of the record being operated on
        /// </summary>
        public long Address { get; internal set; }

        /// <summary>
        /// Hash code of key being operated on
        /// </summary>
        public long KeyHash { get; internal set; }

        /// <summary>
        /// The ID of session context executing the operation
        /// </summary>
        public int SessionID { get; internal set; }

        /// <summary>
        /// The header of the record.
        /// </summary>
        public readonly unsafe RecordInfo RecordInfo => *(RecordInfo*)recordInfoPtr;

        /// <summary>
        /// The length of data in the value that is in use. Incoming, it is set by FASTER to the result of <see cref="IVariableLengthStruct{T, Input}.GetLength(ref T, ref Input)"/>.
        /// If an application wants to allow data to shrink and then grow again within the same record, it must set this to the correct length on output. 
        /// </summary>
        public int UsedValueLength { get; set; }

        /// <summary>
        /// The allocated length of the record value.
        /// </summary>
        public int FullValueLength { get; internal set; }

        /// <summary>
        /// What actions FASTER should perform on a false return from the IFunctions method
        /// </summary>
        public DeleteAction Action { get; set; }

        // The physical address of the RecordInfo start. See UpsertInfo for detailed comments.
        private IntPtr recordInfoPtr;
        internal readonly unsafe ref RecordInfo RecordInfoRef => ref Unsafe.AsRef<RecordInfo>((void*)recordInfoPtr);
        internal unsafe void SetRecordInfoAddress(ref RecordInfo recordInfo) => this.recordInfoPtr = (IntPtr)Unsafe.AsPointer(ref recordInfo);
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
        public readonly unsafe RecordInfo RecordInfo => *(RecordInfo*)recordInfoPtr;

        /// <summary>
        /// What actions FASTER should perform on a false return from the IFunctions method
        /// </summary>
        public ReadAction Action { get; set; }

        // The physical address of the RecordInfo start. See UpsertInfo for detailed comments.
        private IntPtr recordInfoPtr;
        internal readonly unsafe ref RecordInfo RecordInfoRef => ref Unsafe.AsRef<RecordInfo>((void*)recordInfoPtr);
        internal unsafe void SetRecordInfoAddress(ref RecordInfo recordInfo) => this.recordInfoPtr = (IntPtr)Unsafe.AsPointer(ref recordInfo);
    }
}
