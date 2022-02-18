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
        /// Create a <see cref="Found"/> Status value.
        /// </summary>
        public static Status CreateFound() => new(StatusCode.OK);

        /// <summary>
        /// Create a <see cref="Pending"/> Status value. Use the Is* properties to query.
        /// </summary>
        public static Status CreatePending() => new(StatusCode.Pending);

        /// <summary>
        /// Whether a Read or RMW completed successfully (is not currently pending). Either may have appended, especially if the operation completed after going Pending.
        /// Note that Pending must be checked for and handled by the app, because CompletePendingWithOutputs() will return a non-Pending status.
        /// </summary>
        public bool Found => (statusCode & StatusCode.BasicMask) == StatusCode.OK;

        /// <summary>
        /// Whether the operation went pending
        /// </summary>
        public bool Pending => statusCode == StatusCode.Pending;

        /// <summary>
        /// Whether the operation is in an error state
        /// </summary>
        public bool Faulted => statusCode == StatusCode.Error;

        /// <summary>
        /// Whether the operation completed successfully, i.e., it is not pending and did not error out
        /// </summary>
        public bool CompletedSuccessfully
        {
            get
            {
                var basicCode = statusCode & StatusCode.BasicMask;
                return basicCode != StatusCode.Pending && basicCode != StatusCode.Error;
            }
        }

        #region Advanced status
        /// <summary>
        /// Whether a new record for a previously non-existent key was appended to the log.
        /// Indicates that an existing record was updated in place.
        /// </summary>
        public bool CreatedRecord => (statusCode & StatusCode.AdvancedMask) == StatusCode.CreatedRecord;

        /// <summary>
        /// Whether existing record was updated in place.
        /// </summary>
        public bool InPlaceUpdatedRecord => (statusCode & StatusCode.AdvancedMask) == StatusCode.InPlaceUpdatedRecord;

        /// <summary>
        /// Whether an existing record key was copied, updated, and appended to the log.
        /// </summary>
        public bool CopyUpdatedRecord => (statusCode & StatusCode.AdvancedMask) == StatusCode.CopyUpdatedRecord;

        /// <summary>
        /// Whether an existing record key was copied and appended to the log.
        /// </summary>
        public bool CopiedRecord => (statusCode & StatusCode.AdvancedMask) == StatusCode.CopiedRecord;

        /// <summary>
        /// Whether an existing record key was copied, updated, and added to the readcache.
        /// </summary>
        public bool CopiedRecordToReadCache => (statusCode & StatusCode.AdvancedMask) == StatusCode.CopiedRecordToReadCache;
        #endregion

        /// <summary>
        /// Get the underlying status code value
        /// </summary>
        public byte Value => (byte)statusCode;

        /// <inheritdoc />
        public override string ToString() => this.statusCode.ToString();
    }
}
