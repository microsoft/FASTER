// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Runtime.InteropServices;

namespace FASTER.client
{
    /// <summary>
    /// Client-side status result of operation (compatible with FASTER (basic) Status)
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

        /// <summary>
        /// Create a <see cref="Pending"/> Status value. Use the Is* properties to query.
        /// </summary>
        internal static Status CreatePending() => new(StatusCode.Pending);

        /// <summary>
        /// Whether a Read or RMW found the key
        /// </summary>
        public bool Found => (statusCode & StatusCode.BasicMask) == StatusCode.Found;

        /// <summary>
        /// Whether a Read or RMW did not find the key
        /// </summary>
        public bool NotFound => (statusCode & StatusCode.BasicMask) == StatusCode.NotFound;

        /// <summary>
        /// Whether the operation went pending
        /// </summary>
        public bool Pending => statusCode == StatusCode.Pending;

        /// <summary>
        /// Whether the operation went pending
        /// </summary>
        public bool IsCompleted => !Pending;

        /// <summary>
        /// Whether the operation is in an error state
        /// </summary>
        public bool IsFaulted => statusCode == StatusCode.Error;

        /// <summary>
        /// Whether the operation completed successfully, i.e., it is not pending and did not error out
        /// </summary>
        public bool IsCompletedSuccessfully
        {
            get
            {
                var basicCode = statusCode & StatusCode.BasicMask;
                return basicCode != StatusCode.Pending && basicCode != StatusCode.Error;
            }
        }

        /// <inheritdoc />
        public override string ToString() => this.statusCode.ToString();
    }
}