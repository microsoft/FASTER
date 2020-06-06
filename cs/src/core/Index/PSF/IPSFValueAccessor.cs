// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

namespace FASTER.core
{
    /// <summary>
    /// This interface is a generic indirection to access the pieces of a PSFValue{TPSFKey}.
    /// </summary>
    /// <typeparam name="TPSFValue"></typeparam>
    internal unsafe interface IPSFValueAccessor<TPSFValue>
    {
        int PSFCount { get; }

        int RecordIdSize { get; }

        long* GetChainLinkPtrs(ref TPSFValue value);

        void SetRecordId<TRecordId>(ref TPSFValue value, TRecordId recordId);
    }
}