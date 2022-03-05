// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

namespace FASTER.client
{
    internal enum StatusCode : byte
    {
        /// <summary>
        /// For Read and RMW, item being read was found, and
        /// the operation completed successfully
        /// For Upsert, item was upserted successfully
        /// </summary>
        Found,
        /// <summary>
        /// For Read and RMW, item being read was not found
        /// </summary>
        NotFound,
        /// <summary>
        /// The operation was canceled (e.g. by an IFunctions method setting info.CancelOperation).
        /// </summary>
        Canceled,
        /// <summary>
        /// Operation went pending (async)
        /// </summary>
        Pending,
        /// <summary>
        /// Operation resulted in some error
        /// </summary>
        Error,

        /// <summary>
        /// Masking to extract the basic values
        /// </summary>
        BasicMask = 0x0F,
    }
}