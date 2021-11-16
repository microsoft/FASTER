// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Net.Sockets;
using System.Runtime.InteropServices;

namespace FASTER.common
{
    /// <summary>
    /// Buffer of SocketAsyncEventArgs and pinned byte array for transport
    /// </summary>
    public unsafe class SeaaBuffer : IDisposable
    {
        /// <summary>
        /// SocketAsyncEventArgs
        /// </summary>
        internal readonly SocketAsyncEventArgs socketEventAsyncArgs;

        /// <summary>
        /// Byte buffer used by instance
        /// </summary>
        public readonly byte[] buffer;

        /// <summary>
        /// Pointer to byte buffer
        /// </summary>
        public readonly byte* bufferPtr;
        
        private readonly GCHandle handle;

        /// <summary>
        /// Construct new instance
        /// </summary>
        /// <param name="eventHandler">Event handler</param>
        /// <param name="bufferSize">Buffer size</param>
        public SeaaBuffer(EventHandler<SocketAsyncEventArgs> eventHandler, int bufferSize)
        {
            socketEventAsyncArgs = new SocketAsyncEventArgs();
            buffer = new byte[bufferSize];
            handle = GCHandle.Alloc(buffer, GCHandleType.Pinned);
            bufferPtr = (byte*)handle.AddrOfPinnedObject();
            socketEventAsyncArgs.SetBuffer(buffer, 0, bufferSize);
            socketEventAsyncArgs.Completed += eventHandler;
        }

        /// <summary>
        /// Dispose instance
        /// </summary>
        public void Dispose()
        {
            handle.Free();
            socketEventAsyncArgs.Dispose();
        }
    }
}
