// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Net.Sockets;
using FASTER.common;

namespace FASTER.server
{
    internal class ConnectionArgs
    {
        public Socket socket;
        public ServerSessionBase session;
        public MaxSizeSettings maxSizeSettings;
    }
}
