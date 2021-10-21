// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.common;
using System.Net.Sockets;

namespace FASTER.server
{
    internal class ConnectionArgs
    {
        public Socket socket;
        public WebsocketUtils websocketUtils;
        public IServerSession session;
        public int bytesRead;
        public NetworkProtocol networkProtocol;
    }
}
