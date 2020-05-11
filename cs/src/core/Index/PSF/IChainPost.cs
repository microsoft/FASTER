// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

namespace FASTER.core
{
    internal unsafe interface IChainPost<TPSFValue>
    {
        int ChainHeight { get; }

        int RecordIdSize { get; }

        long* GetChainLinkPtrs(ref TPSFValue value);
    }
}