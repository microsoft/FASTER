// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

namespace FASTER.core
{
    // TODO: Move out of PSFs and include the new chained-LA Read function.
    internal struct PSFReadArgs<TKey, TValue>
    {
        internal readonly long LivenessCheckLogicalAddress;

        internal PSFReadArgs(long livenessCheckAddress)
        {
            this.LivenessCheckLogicalAddress = livenessCheckAddress;
        }
    }
}
