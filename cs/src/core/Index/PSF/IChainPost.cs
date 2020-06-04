// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

namespace FASTER.core
{
    /// <summary>
    /// This represents (a generic decoupling of) the concept that each PSFValue is essentially a "post"
    /// that supports the chains, like a post in a barbed-wire fence.
    /// </summary>
    /// <typeparam name="TPSFValue"></typeparam>
    internal unsafe interface IChainPost<TPSFValue>
    {
        int ChainHeight { get; }

        int RecordIdSize { get; }

        long* GetChainLinkPtrs(ref TPSFValue value);

        void SetRecordId<TRecordId>(ref TPSFValue value, TRecordId recordId);
    }
}