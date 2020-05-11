// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

namespace FASTER.core
{
    /// <summary>
    /// The data provider's instantiator that takes the RecordId and creates the data class the user sees.
    /// </summary>
    public interface IPSFCreateProviderData<TRecordId, TProviderData>
    {
        /// <summary>
        /// Creates the provider data from the record id.
        /// </summary>
        TProviderData Create(TRecordId recordId);
    }
}
