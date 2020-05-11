// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;

namespace FASTER.core
{
    /// <summary>
    /// The definition of a single PSF (Predicate Subset Function)
    /// </summary>
    /// <typeparam name="TProviderData">The data from the provider that the PSF should be run over</typeparam>
    /// <typeparam name="TPSFKey">The data from the provider that the PSF should be run over</typeparam>
    public interface IPSFDefinition<TProviderData, TPSFKey>
        where TPSFKey : struct
    {
        /// <summary>
        /// The callback used to obtain a new TPFSKey for the ProviderData record for this PSF definition.
        /// </summary>
        /// <param name="record">The representation of the data that was written to the primary store
        ///     (e.g. Upsert in FasterKV).</param>
        /// <returns>Null if the record does not match the PSF, else the indexing key for the record</returns>
        public TPSFKey? Execute(TProviderData record);

        /// <summary>
        /// The Name of the PSF, assigned by the caller. Must be unique among PSFs in the group. It is
        /// used by the caller to index PSFs in the group in a friendly way.
        /// </summary>
        public string Name { get; }

    }
}
