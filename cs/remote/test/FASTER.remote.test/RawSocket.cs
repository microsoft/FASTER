// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Net.Sockets;
using NUnit.Framework;

namespace FASTER.remote.test
{
    /// <summary>
    /// Raw socket helpers shared by the fixtures that speak the wire protocol directly, bypassing the
    /// client library in order to send inputs that a well-behaved client would never produce.
    /// </summary>
    internal static class RawSocket
    {
        public static Socket Connect()
        {
            var socket = new Socket(SocketType.Stream, ProtocolType.Tcp) { NoDelay = true, ReceiveTimeout = 10000 };
            socket.Connect(TestUtils.Address, TestUtils.Port);
            return socket;
        }

        /// <summary>
        /// Drain any replies and require the server to end the connection, either cleanly or by reset.
        /// </summary>
        public static void AssertConnectionClosed(Socket socket)
        {
            var buffer = new byte[4096];
            var deadline = DateTime.UtcNow.AddSeconds(30);
            while (DateTime.UtcNow < deadline)
            {
                try
                {
                    if (socket.Receive(buffer) == 0) return;
                }
                catch (SocketException e) when (e.SocketErrorCode != SocketError.TimedOut)
                {
                    return;
                }
            }
            Assert.Fail("Server did not close the connection that sent a malformed request");
        }

        public static void ReceiveExactly(Socket socket, byte[] buffer, int count)
        {
            var received = 0;
            while (received < count)
            {
                var read = socket.Receive(buffer, received, count - received, SocketFlags.None);
                if (read == 0) Assert.Fail("Server closed the connection while a reply was outstanding");
                received += read;
            }
        }

        /// <summary>Read one binary reply, returning its payload (batch header followed by messages).</summary>
        public static byte[] ReceiveBinaryBatch(Socket socket)
        {
            var header = new byte[sizeof(int)];
            ReceiveExactly(socket, header, header.Length);
            var size = -BitConverter.ToInt32(header, 0);
            Assert.Greater(size, 0, "Reply had a malformed size field");

            var payload = new byte[size];
            ReceiveExactly(socket, payload, size);
            return payload;
        }
    }
}
