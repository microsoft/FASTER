// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;

namespace FASTER.core
{
    /// <summary>
    /// This interface is implemented on a <see cref="PSFGroup{TProviderData, TPSFKey, TRecordId}"/> and decouples
    /// the <see cref="PSF{TPSFKey, TRecordId}"/> execution from the knowledge of the TKVKey and TKVValue of the
    /// primary FasterKV instance.
    /// </summary>
    /// <typeparam name="TProviderData"></typeparam>
    /// <typeparam name="TRecordId"></typeparam>
    public interface IExecutePSF<TProviderData, TRecordId>
    {
        /// <summary>
        /// For each <see cref="PSF{TPSFKey, TRecordId}"/> in the <see cref="PSFGroup{TProviderData, TPSFKey, TRecordId}"/>,
        /// and store the resultant TPSFKey in the secondary FasterKV instance.
        /// </summary>
        /// <param name="data">The provider's data, e.g. <see cref="FasterKVProviderData{TKVKey, TKVValue}"/></param>
        /// <param name="recordId">The provider's record ID, e.g. long (logicalAddress) for FasterKV</param>
        /// <param name="isInserted"></param>
        Status ExecuteAndStore(TProviderData data, TRecordId recordId, bool isInserted);

        /// <summary>
        /// For the given <see cref="PSF{TPSFKey, TRecordId}"/>, verify that the <paramref name="providerData"/> 
        /// matches (returns non-null).
        /// </summary>
        /// <param name="providerData">The provider data wrapped around the TRecordId</param>
        /// <param name="psfOrdinal">The ordinal of the <see cref="PSF{TPSFKey, TRecordId}"/> in the 
        ///     <see cref="PSFGroup{TRecordId, TPSFKey, TRecordId}"/>.</param>
        /// <returns>True if the providerData matches (returns non-null from) the <see cref="PSF{TPSFKey, TRecordId}"/>,
        ///     else false.</returns>
        bool Verify(TProviderData providerData, int psfOrdinal);
    }
}
