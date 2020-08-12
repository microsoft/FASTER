// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Collections.Generic;

namespace FASTER.core
{
    /// <summary>
    /// Provides an interface on the <see cref="PSFGroup{TProviderData, TPSFKey, TRecordId}"/> that decouples the
    /// PSFGroup from the primary FasterKV's TKVKey and TKVValue.
    /// </summary>
    /// <typeparam name="TPSFKey"></typeparam>
    /// <typeparam name="TRecordId"></typeparam>
    public interface IQueryPSF<TPSFKey, TRecordId>
    {
        /// <summary>
        /// Issues a query on the specified <see cref="PSF{TPSFKey, TRecordId}"/> to return <typeparamref name="TRecordId"/>s.
        /// </summary>
        /// <param name="psfOrdinal">The ordinal of the <see cref="PSF{TPSFKey, TRecordId}"/> in this group</param>
        /// <param name="key">The key to query on to rertrieve the <typeparamref name="TRecordId"/>s.</param>
        /// <param name="querySettings">Optional query settings for EOS, cancellation, etc.</param>
        /// <returns></returns>
        IEnumerable<TRecordId> Query(int psfOrdinal, TPSFKey key, PSFQuerySettings querySettings);

#if DOTNETCORE
        /// <summary>
        /// Issues a query on the specified <see cref="PSF{TPSFKey, TRecordId}"/> to return <typeparamref name="TRecordId"/>s.
        /// </summary>
        /// <param name="psfOrdinal">The ordinal of the <see cref="PSF{TPSFKey, TRecordId}"/> in this group</param>
        /// <param name="key">The key to query on to rertrieve the <typeparamref name="TRecordId"/>s.</param>
        /// <param name="querySettings">Optional query settings for EOS, cancellation, etc.</param>
        /// <returns></returns>
        IAsyncEnumerable<TRecordId> QueryAsync(int psfOrdinal, TPSFKey key, PSFQuerySettings querySettings);
#endif
    }
}
