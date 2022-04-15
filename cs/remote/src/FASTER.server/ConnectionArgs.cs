// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.common;
using System.Net.Sockets;
using System.Runtime.InteropServices;

namespace FASTER.server
{
    internal unsafe class ConnectionArgs
    {
        public Socket socket;
        public IMessageConsumer session;
#if !NET5_0_OR_GREATER
        public GCHandle recvHandle;
#endif
        public byte* recvBufferPtr = null;
        public int bytesRead;
    }
}
