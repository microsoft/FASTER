// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

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
        readonly StatusCode statusCode;

        /// <summary>
        /// Get underlying status code
        /// </summary>
        public StatusCode StatusCode => statusCode;

        /// <summary>
        /// Create status from given status code
        /// </summary>
        /// <param name="statusCode"></param>
        internal Status(StatusCode statusCode) => this.statusCode = statusCode;

        internal Status(OperationStatus operationStatus) => statusCode = (StatusCode)operationStatus;

        /// <summary>
        /// For Read and RMW, item being read was found, and
        /// the operation completed successfully
        /// For Upsert, item was upserted successfully
        /// </summary>
        public static readonly Status OK = new(StatusCode.OK);

        /// <summary>
        /// For Read and RMW, item being read was not found
        /// </summary>
        public static readonly Status NOTFOUND = new(StatusCode.NOTFOUND);

        /// <summary>
        /// Operation went pending (async)
        /// </summary>
        public static readonly Status PENDING = new(StatusCode.PENDING);

        /// <summary>
        /// Operation resulted in some error
        /// </summary>
        public static readonly Status ERROR = new(StatusCode.ERROR);

        /// <summary>
        /// Whether operation has completed, i.e., it did not go pending
        /// </summary>
        public bool IsCompleted => statusCode != StatusCode.PENDING;

        /// <summary>
        /// Did operation complete successfully, i.e., it is not pending or errored out
        /// </summary>
        public bool IsCompletedSuccessfully => statusCode != StatusCode.PENDING && statusCode != StatusCode.ERROR;

        /// <summary>
        /// Whether operation is pending
        /// </summary>
        public bool IsPending => statusCode == StatusCode.PENDING;

        /// <summary>
        /// Whether operation is in error state
        /// </summary>
        public bool IsFaulted => statusCode == StatusCode.ERROR;

        #region Advanced status
        /// <summary>
        /// 
        /// </summary>
        public bool IsInitialUpdate => statusCode == StatusCode.OK_IU;

        /// <summary>
        /// 
        /// </summary>
        public bool IsInPlaceUpdate => statusCode == StatusCode.OK_IPU;

        /// <summary>
        /// 
        /// </summary>
        public bool IsAppend => statusCode == StatusCode.OK_IU || statusCode == StatusCode.OK_APPEND;

        /// <summary>
        /// 
        /// </summary>
        public bool IsNotFound => statusCode == StatusCode.NOTFOUND || statusCode == StatusCode.OK_IU;
        #endregion

        /// <summary>
        /// Check equals
        /// </summary>
        /// <param name="per1"></param>
        /// <param name="per2"></param>
        /// <returns></returns>
        public static bool operator ==(Status per1, Status per2)
            => per1.statusCode == per2.statusCode;

        /// <summary>
        /// Check not equals
        /// </summary>
        /// <param name="per1"></param>
        /// <param name="per2"></param>
        /// <returns></returns>
        public static bool operator !=(Status per1, Status per2)
            => per1.statusCode != per2.statusCode;

        /// <inheritdoc />
        public override bool Equals(object obj)
            => statusCode == ((Status)obj).statusCode;

        /// <inheritdoc />
        public override int GetHashCode()
            => statusCode.GetHashCode();
    }
}
