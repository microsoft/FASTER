// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using FASTER.common;

namespace FASTER.client
{
    internal class ClientNetworkSession<Key, Value, Input, Output, Context, Functions, ParameterSerializer>
        where Functions : ICallbackFunctions<Key, Value, Input, Output, Context>
        where ParameterSerializer : IClientSerializer<Key, Value, Input, Output>
    {
        public readonly Socket socket;
        private readonly ClientSession<Key, Value, Input, Output, Context, Functions, ParameterSerializer> session;

        private int bytesRead;
        private int readHead;

        public ClientNetworkSession(Socket socket, ClientSession<Key, Value, Input, Output, Context, Functions, ParameterSerializer> session)
        {
            this.socket = socket;
            this.session = session;
            bytesRead = 0;
            readHead = 0;
        }

        internal void AddBytesRead(int bytesRead) => this.bytesRead += bytesRead;

        internal int TryConsumeMessages(byte[] buf)
        {
            while (TryReadMessages(buf, out var offset))
                session.ProcessReplies(buf, offset);

            // The bytes left in the current buffer not consumed by previous operations
            var bytesLeft = bytesRead - readHead;
            if (bytesLeft != bytesRead)
            {
                // Shift them to the head of the array so we can reset the buffer to a consistent state
                Array.Copy(buf, readHead, buf, 0, bytesLeft);
                bytesRead = bytesLeft;
                readHead = 0;
            }

            return bytesRead;
        }

        private bool TryReadMessages(byte[] buf, out int offset)
        {
            offset = default;

            var bytesAvailable = bytesRead - readHead;
            // Need to at least have read off of size field on the message
            if (bytesAvailable < sizeof(int)) return false;

            var size = -BitConverter.ToInt32(buf, readHead);
            // Not all of the message has arrived
            if (bytesAvailable < size + sizeof(int)) return false;
            offset = readHead + sizeof(int);

            // Consume this message and the header
            readHead += size + sizeof(int);
            return true;
        }
    }
}