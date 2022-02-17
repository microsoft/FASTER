// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace FASTER.core
{
    /// <summary>
    /// Status result of operation on FASTER
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = 1)]
    public struct Status
    {
        [FieldOffset(0)]
        private readonly StatusCode statusCode;

        const StatusCode BasicMask = (StatusCode)0x0F;
        const StatusCode AdvancedMask = (StatusCode)0xF0;

        /// <summary>
        /// Create status from given status code
        /// </summary>
        /// <param name="statusCode"></param>
        internal Status(StatusCode statusCode) => this.statusCode = statusCode;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Status(OperationStatus operationStatus)
        {
            var basicOperationStatus = OperationStatusUtils.BasicOpCode(operationStatus);
            Debug.Assert(basicOperationStatus == OperationStatus.SUCCESS || basicOperationStatus == OperationStatus.NOTFOUND);
            statusCode = (StatusCode)basicOperationStatus | (StatusCode)((int)operationStatus >> OperationStatusUtils.OpStatusToStatusCodeShift);
        }

        /// <summary>
        /// Create an OK Status value. Use the Is* properties to query.
        /// </summary>
        public static Status CreateOK() => new(StatusCode.OK);

        /// <summary>
        /// Create a Pending Status value. Use the Is* properties to query.
        /// </summary>
        public static Status CreatePending() => new(StatusCode.Pending);

        /// <summary>
        /// The operation completed with a status of OK, and possibly an advanced value such as Append
        /// </summary>
        public bool IsOK => (statusCode & BasicMask) == StatusCode.OK;

        /// <summary>
        /// Whether operation has completed, i.e., it did not go pending (or pending has completed due to a CompletePending*() call)
        /// </summary>
        public bool IsCompleted => (statusCode & BasicMask) != StatusCode.Pending;

        /// <summary>
        /// Whether the operation completed successfully, i.e., it is not pending and did not error out
        /// </summary>
        public bool IsCompletedSuccessfully
        {
            get
            {
                var basicCode = statusCode & BasicMask;
                return basicCode != StatusCode.Pending && basicCode != StatusCode.Error;
            }
        }

        /// <summary>
        /// Whether an in-place update (due to Upsert, RMW, or Delete) completed successfully (is not pending and did not append).
        /// Note that Pending must be checked for and handled by the app, because CompletePendingWithOutputs() will return a non-Pending status.
        /// </summary>
        public bool IsInPlaceUpdate => IsOK && !IsAppendedRecord;

        /// <summary>
        /// Whether a Read or RMW completed successfully (is not currently pending). Either may have appended, especially if the operation completed after going Pending.
        /// Note that Pending must be checked for and handled by the app, because CompletePendingWithOutputs() will return a non-Pending status.
        /// </summary>
        public bool IsFound => (statusCode & BasicMask) == StatusCode.OK;

        /// <summary>
        /// Whether the operation successfully completed with a NotFound result--either Read or RMW did not find the key (and RMW will have apppended one).
        /// Note that this is not the same as !IsFound, which includes the possibility of IsPending or IsError.
        /// </summary>
        public bool IsNotFound => (statusCode & StatusCode.NotFound) != 0;

        /// <summary>
        /// Whether the operation went pending
        /// </summary>
        public bool IsPending => statusCode == StatusCode.Pending;

        /// <summary>
        /// Whether the operation is in an error state
        /// </summary>
        public bool IsError => statusCode == StatusCode.Error;

        #region Advanced status
        /// <summary>
        /// Whether an operation appended a record to the log.
        /// </summary>
        public bool IsAppendedRecord
        {
            get
            {
                var advancedCode = statusCode & AdvancedMask;
                return advancedCode == StatusCode.NewRecord || advancedCode == StatusCode.CopyRecord;
            }
        }

        /// <summary>
        /// Whether an operation appended a record for a new key to the log.
        /// </summary>
        public bool IsNewRecord => (statusCode & AdvancedMask) == StatusCode.NewRecord;

        /// <summary>
        /// Whether an operation appended a record to the log by copying (and possibly modifying) from ReadOnly or storage.
        /// </summary>
        public bool IsCopyRecord => (statusCode & AdvancedMask) == StatusCode.CopyRecord;
        #endregion

        /// <summary>
        /// Get the underlying status code value
        /// </summary>
        public byte Value => (byte)statusCode;

        /// <inheritdoc />
        public override string ToString() => this.statusCode.ToString();
    }
}
