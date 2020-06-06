// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

namespace FASTER.core
{
    /// <summary>
    /// A base interface for <see cref="PSF{TPSFKey, TRecordId}"/> to decouple the generic type parameters.
    /// </summary>
    public interface IPSF
    {
        /// <summary>
        /// The name of the <see cref="PSF{TPSFKey, TRecordId}"/>; must be unique among all
        ///     <see cref="PSFGroup{TProviderData, TPSFKey, TRecordId}"/>s.
        /// </summary>
        string Name { get; }
    }
}
