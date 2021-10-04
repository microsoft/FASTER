// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

namespace FASTER.core
{
    internal interface IClientSession
    {
        void AtomicSwitch(int version);
    }
}

