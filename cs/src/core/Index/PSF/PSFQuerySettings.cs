// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Threading;

namespace FASTER.core
{
    /// <summary>
    /// Defines settings that control some behaviors of the QueryPSF execution.
    /// </summary>
    public class PSFQuerySettings
    {
        /// <summary>One or more streams has ended. Input PSF whose stream ended,
        /// and the index of that PSF in the parameters (left to right, or in the
        /// case of PSF arrays as parameters, left top to bottom, then right top
        /// to bottom.
        /// </summary>
        /// <returns>true to continue the enumeration, else false</returns>
        public Func<IPSF, int, bool> OnStreamEnded;

        /// <summary>Cancel the enumeration if set. Can be set by another thread,
        /// e.g. one presenting results to a UI, or by StreamEnded.
        /// </summary>
        public CancellationToken CancellationToken { get; set; }

        /// <summary> When cancellation is reqested, simply terminate the enumeration 
        ///     without throwing a CancellationException.</summary>
        public bool ThrowOnCancellation { get; set; }
    }
}
