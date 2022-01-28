// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

namespace FASTER.core
{
    /// <summary>
    /// Return status code for FASTER operations
    /// </summary>
    enum StatusCode : byte
    {
        # region Basic status codes
        /// <summary>
        /// OK
        /// </summary>
        OK,

        /// <summary>
        /// NOTFOUND
        /// </summary>
        NOTFOUND,

        /// <summary>
        /// PENDING
        /// </summary>
        PENDING,

        /// <summary>
        /// ERROR
        /// </summary>
        ERROR,
        #endregion

        #region Advanced status codes
        /// <summary>
        /// OK_IPU
        /// </summary>
        OK_IU,

        /// <summary>
        /// OK_IPU
        /// </summary>
        OK_IPU,

        /// <summary>
        /// OK_APPEND
        /// </summary>
        OK_APPEND
        #endregion
    }
}
