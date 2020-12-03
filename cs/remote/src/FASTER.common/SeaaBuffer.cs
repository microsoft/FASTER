// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Net.Sockets;
using System.Runtime.InteropServices;

namespace FASTER.common
{
    public unsafe class SeaaBuffer
    {
        public readonly SocketAsyncEventArgs socketEventAsyncArgs;
        public readonly byte[] buffer;
        public readonly GCHandle handle;
        public readonly byte* bufferPtr;

        public SeaaBuffer(EventHandler<SocketAsyncEventArgs> eventHandler, int bufferSize)
        {
            socketEventAsyncArgs = new SocketAsyncEventArgs();
            buffer = new byte[bufferSize];
            handle = GCHandle.Alloc(buffer, GCHandleType.Pinned);
            bufferPtr = (byte*)handle.AddrOfPinnedObject();
            socketEventAsyncArgs.SetBuffer(buffer, 0, bufferSize);
            socketEventAsyncArgs.Completed += eventHandler;
        }

        public void Dispose()
        {
            handle.Free();
        }
    }
}
