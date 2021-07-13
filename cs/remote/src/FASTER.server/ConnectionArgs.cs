// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Net.Sockets;

namespace FASTER.server
{
    internal class ConnectionArgs
    {
        public Socket socket;
        public IServerSession session;
    }
}
