// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

namespace FASTER.core
{
    class SessionInfo
    {
        public string sessionName;
        public bool isActive;
        public IClientSession session;
    }

    internal interface IClientSession
    {
        void AtomicSwitch(long version);
    }
}

