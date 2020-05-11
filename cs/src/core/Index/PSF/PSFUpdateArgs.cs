// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

namespace FASTER.core
{
    internal struct PSFUpdateArgs
    {
        internal long logicalAddress;    // Set to the inserted or updated address
        internal bool isInserted;        // Set true if this was an insert rather than update or RMW
    }
}
