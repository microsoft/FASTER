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
        /// <summary>One or more streams has ended. Inputs are the PSF whose stream ended and the index of that PSF in the parameters,
        ///     identified by the 0-based ordinal of the TPSFKey type (TPSFKey1, TPSFKey2, or TPSFkey3 in the QueryPSF overloads) and
        ///     the 0-based ordinal of the PSF within the TPSFKey type.</summary>
        /// <returns>true to continue the enumeration, else false</returns>
        public Func<IPSF, (int keyTypeOrdinal, int psfOrdinal), bool> OnStreamEnded;

        /// <summary>Cancel the enumeration if set. Can be set by another thread,
        ///     e.g. one presenting results to a UI, or by StreamEnded.
        /// </summary>
        public CancellationToken CancellationToken { get; set; }

        /// <summary> When cancellation is reqested, simply terminate the enumeration 
        ///     without throwing a CancellationException.</summary>
        public bool ThrowOnCancellation { get; set; }

        internal bool IsCanceled
        { 
            get
            {
                if (this.CancellationToken.IsCancellationRequested)
                {
                    if (this.ThrowOnCancellation)
                        CancellationToken.ThrowIfCancellationRequested();
                    return true;
                }
                return false;
            }
        }

        internal bool CancelOnEOS(IPSF psf, (int, int) location) => !(this.OnStreamEnded is null) && !this.OnStreamEnded(psf, location);

        // Default is to let all streams continue to completion.
        internal static readonly PSFQuerySettings Default = new PSFQuerySettings { OnStreamEnded = (unusedPsf, unusedIndex) => true };
    }
}
