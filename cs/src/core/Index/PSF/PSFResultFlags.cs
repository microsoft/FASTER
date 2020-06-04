// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;

namespace FASTER.core
{
    /// <summary>
    /// Internal flags indicating how the keys returned from a <see cref="PSF{TPSFKey, TRecordId}"/> are handled.
    /// </summary>
    [Flags]
    public enum PSFResultFlags
    {
        /// <summary>
        /// For initialization only
        /// </summary>
        None = 0,

        /// <summary>
        /// For Insert, this identifies a null PSF result (the record does not match the PSF and is not
        /// included in any TPSFKey chain for it). Also used in <see cref="PSFChangeTracker{TProviderData, TRecordId}"/> 
        /// to determine whether to set <see cref="UnlinkOld"/>.
        /// </summary>
        IsNull = 1,

        /// <summary>
        /// For Update, the TPSFKey has changed; remove this record from the previous TPSFKey chain.
        /// </summary>
        UnlinkOld = 2,

        /// <summary>
        /// For Update and insert, the TPSFKey has changed; add this record to the new TPSFKey chain.
        /// </summary>
        LinkNew = 4
    }
}
