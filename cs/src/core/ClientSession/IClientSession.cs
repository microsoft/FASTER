// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma warning disable 0162


namespace FASTER.core
{
    internal interface IClientSession
    {
        void AtomicSwitch(int version);
    }
}
