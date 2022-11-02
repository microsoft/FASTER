using System;
using System.Net.Sockets;

namespace FASTER.client
{
    internal interface INetworkMessageConsumer
    {
        void ProcessReplies(byte[] buf, int offset, int size);
    }
    
    internal class DarqClientNetworkSession<T> where T : INetworkMessageConsumer
    {
        internal readonly Socket socket;
        internal readonly T client;

        private int bytesRead;
        private int readHead;
        
        public DarqClientNetworkSession(Socket socket, T client)
        {
            this.socket = socket;
            this.client = client;
            bytesRead = 0;
            readHead = 0;
        }

        internal void AddBytesRead(int bytesRead) => this.bytesRead += bytesRead;

        internal int TryConsumeMessages(byte[] buf)
        {
            while (TryReadMessages(buf, out var offset, out var size))
                client.ProcessReplies(buf, offset, size);

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

        private bool TryReadMessages(byte[] buf, out int offset, out int size)
        {
            offset = default;
            size = default;
            var bytesAvailable = bytesRead - readHead;
            // Need to at least have read off of size field on the message
            if (bytesAvailable < sizeof(int)) return false;

            size = -BitConverter.ToInt32(buf, readHead);
            // Not all of the message has arrived
            if (bytesAvailable < size + sizeof(int)) return false;
            offset = readHead + sizeof(int);

            // Consume this message and the header
            readHead += size + sizeof(int);
            return true;
        }
    }
}