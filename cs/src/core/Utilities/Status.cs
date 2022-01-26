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
        readonly byte statusCode;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="statusCode"></param>
        public Status(byte statusCode) => this.statusCode = statusCode;

        internal Status(OperationStatus operationStatus) => this.statusCode = (byte)operationStatus;

        /// <summary>
        /// For Read and RMW, item being read was found, and
        /// the operation completed successfully
        /// For Upsert, item was upserted successfully
        /// </summary>
        public static Status OK => new((byte)0);

        /// <summary>
        /// For Read and RMW, item being read was not found
        /// </summary>
        public static Status NOTFOUND => new(1);

        /// <summary>
        /// Operation went pending (async)
        /// </summary>
        public static Status PENDING => new(2);

        /// <summary>
        /// Operation resulted in some error
        /// </summary>
        public static Status ERROR => new(3);

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
        {
            throw new System.Exception();
            // return statusCode == ((Status)obj).statusCode;
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            throw new System.Exception();
            // return statusCode.GetHashCode();
        }
    }
}
