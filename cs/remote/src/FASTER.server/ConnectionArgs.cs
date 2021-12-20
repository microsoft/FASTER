// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Net.Sockets;
using System.Runtime.InteropServices;

namespace FASTER.server
{
    internal unsafe class ConnectionArgs
    {
        public Socket socket;
        public IServerSession session;        
        public GCHandle recvHandle;
        public byte* recvBufferPtr = null;
    }
}
