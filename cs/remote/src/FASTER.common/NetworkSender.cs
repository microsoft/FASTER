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

        public NetworkSender(int bufferSize)
        {
             reusableSeaaBuffer = new SimpleObjectPool<SeaaBuffer>(() => new SeaaBuffer(SeaaBuffer_Completed, bufferSize), e => e.Dispose());
        }

        public void Dispose()
        {
            reusableSeaaBuffer.Dispose();
        }

         public ReusableObject<SeaaBuffer> GetReusableSeaaBuffer() => reusableSeaaBuffer.Checkout();

        public unsafe void Send(Socket socket, ReusableObject<SeaaBuffer> sendObject, int size)
        {
            // Set packet size
            *(int*)sendObject.obj.bufferPtr = size - sizeof(int);

            // Reset send buffer
            sendObject.obj.socketEventAsyncArgs.SetBuffer(0, size);
            // Set user context to reusable object handle for disposal when send is done
            sendObject.obj.socketEventAsyncArgs.UserToken = sendObject;
            if (!socket.SendAsync(sendObject.obj.socketEventAsyncArgs))
                SeaaBuffer_Completed(null, sendObject.obj.socketEventAsyncArgs);
        }

        private void SeaaBuffer_Completed(object sender, SocketAsyncEventArgs e)
        {
            ((ReusableObject<SeaaBuffer>)e.UserToken).Dispose();
        }

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
