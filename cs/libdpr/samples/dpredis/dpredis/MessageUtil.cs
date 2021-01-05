using System;
using System.Diagnostics;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Text;
using FASTER.libdpr;

namespace dpredis
{
    public static class MessageUtil
    {
        public abstract class AbstractDprConnState
        {
            protected Socket socket;
            private int bytesRead;
            private int readHead;

            public void Reset(Socket socket)
            {
                this.socket = socket;
            }

            protected abstract void HandleMessage(byte[] buf, int offset, int size);

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

            private static void HandleReceiveCompletion(SocketAsyncEventArgs e)
            {
                var connState = (AbstractDprConnState) e.UserToken;
                if (e.BytesTransferred == 0 || e.SocketError != SocketError.Success)
                {
                    connState.socket.Dispose();
                    e.Dispose();
                    return;
                }

                connState.bytesRead += e.BytesTransferred;
                var newHead = connState.TryConsumeMessages(e.Buffer);
                e.SetBuffer(newHead, e.Buffer.Length - newHead);
            }

            public static void RecvEventArg_Completed(object sender, SocketAsyncEventArgs e)
            {
                var connState = (AbstractDprConnState) e.UserToken;
                do
                {
                    // No more things to receive
                    HandleReceiveCompletion(e);
                } while (!connState.socket.ReceiveAsync(e));
            }
        }

        public abstract class AbstractRedisConnState
        {
            private Socket socket;
            private int readHead, bytesRead, currentStringStart;

            public void Reset(Socket socket)
            {
                this.socket = socket;
            }

            // Return whether we should reset the buffer or keep message buffered
            protected abstract bool HandleSimpleString(byte[] buf, int start, int end);

            private static void HandleReceiveCompletion(SocketAsyncEventArgs e)
            {
                var connState = (AbstractRedisConnState) e.UserToken;
                if (e.BytesTransferred == 0 || e.SocketError != SocketError.Success)
                {
                    connState.socket.Dispose();
                    e.Dispose();
                    return;
                }

                connState.bytesRead += e.BytesTransferred;
                // TODO(Tianyu): Only supports simple interface for now
                for (; connState.readHead >= connState.bytesRead; connState.readHead++)
                {
                    // Beginning a simple string with +
                    if (e.Buffer[connState.readHead] == '+')
                    {
                        Debug.Assert(connState.currentStringStart == -1);
                        connState.currentStringStart = connState.readHead;
                    }
                    // Ending a simple string with \r\n
                    else if (e.Buffer[connState.readHead] == '\n'
                             // Never yields out-of-bound because no well-formed buffer starts with \n
                             && e.Buffer[connState.readHead - 1] == '\r')
                    {
                        Debug.Assert(connState.currentStringStart != -1);
                        var nextHead = connState.readHead + 1;
                        if (connState.HandleSimpleString(e.Buffer, connState.currentStringStart, nextHead))
                        {
                            var bytesLeft = connState.bytesRead - nextHead;
                            // Shift buffer to front
                            Array.Copy(e.Buffer, connState.readHead, e.Buffer, 0, bytesLeft);
                            connState.bytesRead = bytesLeft;
                            connState.readHead = 0;
                        }

                        connState.currentStringStart = -1;
                    }
                }

                e.SetBuffer(connState.readHead, e.Buffer.Length - connState.bytesRead);
            }

            public static void RecvEventArg_Completed(object sender, SocketAsyncEventArgs e)
            {
                var connState = (AbstractRedisConnState) e.UserToken;
                do
                {
                    // No more things to receive
                    HandleReceiveCompletion(e);
                } while (!connState.socket.ReceiveAsync(e));
            }
        }

        public static unsafe void SendDpredisRequest(this Socket socket, ReadOnlySpan<byte> dprHeader, string body)
        {
            fixed (byte* h = dprHeader)
            {
                ref var request = ref Unsafe.AsRef<DprBatchRequestHeader>(h);
                var totalSize = request.Size() + body.Length;
                socket.Send(new Span<byte>(&totalSize, sizeof(int)));
                socket.Send(dprHeader.Slice(0, request.Size()));
                socket.Send(Encoding.ASCII.GetBytes(body));
            }
        }

        public static unsafe void SendDpredisResponse(this Socket socket, ReadOnlySpan<byte> dprHeader, ReadOnlySpan<byte> body)
        {
            fixed (byte* h = dprHeader)
            {
                ref var request = ref Unsafe.AsRef<DprBatchResponseHeader>(h);
                var totalSize = request.Size() + body.Length;
                socket.Send(new Span<byte>(&totalSize, sizeof(int)));
                socket.Send(dprHeader.Slice(0, request.Size()));
                socket.Send(body);
            }
        }
    }
}