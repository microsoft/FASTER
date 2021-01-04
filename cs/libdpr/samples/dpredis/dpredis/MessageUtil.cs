using System;
using System.Net.Sockets;
using System.Text;
using FASTER.libdpr;

namespace dpredis
{
    public static class MessageUtil
    {
        public abstract class AbstractConnState
        {
            private Socket socket;
            private int bytesRead;
            private int readHead;

            public void Reset(Socket socket)
            {
                this.socket = socket;
            }

            protected abstract void HandleMessage(byte[] buf, int offset, int size);

            public void AddBytesRead(int bytesRead) => this.bytesRead += bytesRead;
            
            private int TryConsumeMessages(byte[] buf)
            {
                while (TryReadMessages(buf, out var offset, out var size))
                   HandleMessage(buf, offset, size);

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

                size = BitConverter.ToInt32(buf, readHead);
                // Not all of the message has arrived
                if (bytesAvailable < size + sizeof(int)) return false;
                offset = readHead + sizeof(int);

                // Consume this message and the header
                readHead += size + sizeof(int);
                return true;
            }

            private static bool HandleReceiveCompletion(SocketAsyncEventArgs e)
            {
                var connState = (AbstractConnState) e.UserToken;
                if (e.BytesTransferred == 0 || e.SocketError != SocketError.Success)
                {
                    connState.socket.Dispose();
                    e.Dispose();
                    return false;
                }

                connState.AddBytesRead(e.BytesTransferred);
                var newHead = connState.TryConsumeMessages(e.Buffer);
                e.SetBuffer(newHead, e.Buffer.Length - newHead);
                return true;
            }

            public static void RecvEventArg_Completed(object sender, SocketAsyncEventArgs e)
            {
                var connState = (AbstractConnState) e.UserToken;
                do
                {
                    // No more things to receive
                    if (!HandleReceiveCompletion(e)) break;
                } while (!connState.socket.ReceiveAsync(e));
            }
        }

        public static void SendDpredisMessage(this Socket socket, ReadOnlySpan<byte> dprHeader, string body)
        {
            var totalSize = dprHeader.Length + body.Length;
            unsafe
            {
                socket.Send(new Span<byte>(&totalSize, sizeof(int)));
            }
            socket.Send(dprHeader);
            socket.Send(Encoding.ASCII.GetBytes(body));
        }
    }
}