// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Net.Sockets;

namespace FASTER.common
{
    /// <summary>
    /// Helper for concurrent network sends using SocketEventAsyncArgs
    /// </summary>
    public sealed class NetworkSender : IDisposable
    {
        readonly SimpleObjectPool<SeaaBuffer> reusableSeaaBuffer;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="bufferSize">Buffer size</param>
        public NetworkSender(int bufferSize)
        {
             reusableSeaaBuffer = new SimpleObjectPool<SeaaBuffer>(() => new SeaaBuffer(SeaaBuffer_Completed, bufferSize), e => e.Dispose());
        }

        /// <summary>
        /// Dispose
        /// </summary>
        public void Dispose()
        {
            reusableSeaaBuffer.Dispose();
        }

        /// <summary>
        /// Get reusable SocketAsyncEventArgs buffer
        /// </summary>
        /// <returns></returns>
        public ReusableObject<SeaaBuffer> GetReusableSeaaBuffer() => reusableSeaaBuffer.Checkout();

        /// <summary>
        /// Send
        /// </summary>
        /// <param name="socket">Socket</param>
        /// <param name="sendObject">Reusable SocketAsyncEventArgs buffer</param>
        /// <param name="offset">Offset</param>
        /// <param name="size">Size in bytes</param>
        public unsafe void Send(Socket socket, ReusableObject<SeaaBuffer> sendObject, int offset, int size)
        {
            // Reset send buffer
            sendObject.obj.socketEventAsyncArgs.SetBuffer(offset, size);
            // Set user context to reusable object handle for disposal when send is done
            sendObject.obj.socketEventAsyncArgs.UserToken = sendObject;
            if (!socket.SendAsync(sendObject.obj.socketEventAsyncArgs))
                SeaaBuffer_Completed(null, sendObject.obj.socketEventAsyncArgs);
        }

        private void SeaaBuffer_Completed(object sender, SocketAsyncEventArgs e)
        {
            ((ReusableObject<SeaaBuffer>)e.UserToken).Dispose();
        }

        /// <summary>
        /// Receive
        /// </summary>
        /// <param name="clientSocket">Socket</param>
        /// <param name="buffer">Reusable SocketAsyncEventArgs buffer</param>
        /// <param name="numBytes">Size in bytes</param>
        /// <param name="flags">Socket flags</param>
        /// <returns></returns>
        public static bool ReceiveFully(Socket clientSocket, byte[] buffer, int numBytes, SocketFlags flags = SocketFlags.None)
        {
            Debug.Assert(numBytes <= buffer.Length);
            var totalReceived = 0;
            do
            {
                var received = clientSocket.Receive(buffer, totalReceived, numBytes - totalReceived, flags);
                if (received == 0) return false;
                totalReceived += received;
            } while (totalReceived < numBytes);

            return true;
        }

        /// <summary>
        /// Send
        /// </summary>
        /// <param name="clientSocket">Socket</param>
        /// <param name="buffer">Byte array to send</param>
        /// <param name="offset">Offset</param>
        /// <param name="size">Size to send</param>
        /// <param name="flags">Socket flags</param>
        public static void SendFully(Socket clientSocket, byte[] buffer, int offset, int size, SocketFlags flags = SocketFlags.None)
        {
            Debug.Assert(offset >= 0 && offset < buffer.Length && offset + size <= buffer.Length);
            var totalSent = 0;
            do
            {
                var sent = clientSocket.Send(buffer, offset + totalSent, size - totalSent, flags);
                totalSent += sent;
            } while (totalSent < size);
        }
    }
}
