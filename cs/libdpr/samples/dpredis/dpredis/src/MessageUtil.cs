using System;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Text;
using FASTER.libdpr;

namespace dpredis
{
    public static class MessageUtil
    {
        private unsafe static int IntToDecimalString(int a, byte[] buf, int offset)
        {
            var digits = stackalloc byte[20];
            var i = 0;
            do
            {
                digits[i] = (byte) ((a % 10) + 48);
                i++;
                a /= 10;
            } while (a > 0);

            var head = offset;

            if (head + i + 1 >= buf.Length) return 0;
            for (; i >= 0; i--)
                buf[head++] = digits[i];
            return head - offset;
        }

        private static byte HexDigitToChar(ulong a)
        {
            Debug.Assert(a >= 0 && a < 16);
            if (a < 10) return (byte) (a + 48);
            return (byte) (a + 55);
        }

        // Writes long val as a hex string with full 8 bytes (e.g 42 -> 2A) 
        public static int WriteRedisBulkString(ulong val, byte[] buf, int offset)
        {
            var head = offset;
            if (head + 1 >= buf.Length) return 0;
            buf[head++] = (byte) '$';
            unsafe
            {
                var digits = stackalloc byte[16];
                var numDigits = 0;
                do
                {
                    digits[numDigits] = HexDigitToChar(val % 16);
                    numDigits++;
                    val /= 16;
                } while (val > 0);
                
                var size = IntToDecimalString(numDigits, buf, head);
                if (size == 0) return 0;
                head += size;
                
                if (head + 4 + numDigits >= buf.Length) return 0;
                buf[head++] = (byte) '\r';
                buf[head++] = (byte) '\n';
                for (var i = numDigits - 1; i >= 0; i--)
                    buf[head++] = digits[i];
                buf[head++] = (byte) '\r';
                buf[head++] = (byte) '\n';
            }
            return head - offset;
        }

        public static int WriteRedisBulkString(string val, byte[] buf, int offset)
        {
            var head = offset;
            if (head + 1 >= buf.Length) return 0;
            buf[head++] = (byte) '$';

            var size = IntToDecimalString(val.Length, buf, head);
            if (size == 0) return 0;
            head += size;

            if (head + 4 + val.Length >= buf.Length) return 0;
            buf[head++] = (byte) '\r';
            buf[head++] = (byte) '\n';

            foreach (var t in val)
                buf[head++] = (byte) t;

            buf[head++] = (byte) '\r';
            buf[head++] = (byte) '\n';
            return head - offset;
        }

        public static int WriteRedisArrayHeader(int numElems, byte[] buf, int offset)
        {
            var head = offset;
            if (head + 1 >= buf.Length) return 0;
            buf[head++] = (byte) '*';

            var size = IntToDecimalString(numElems, buf, head);
            if (size == 0) return 0;
            head += size;

            if (head + 2 >= buf.Length) return 0;
            buf[head++] = (byte) '\r';
            buf[head++] = (byte) '\n';
            return head - offset;
        }

        private static bool StringEqual(ReadOnlySpan<byte> bytes, int size, string comparand)
        {
            if (size != comparand.Length) return false;
            for (var i = 0; i < size; i++)
                if (bytes[i] != (byte) comparand[i])
                    return false;
            return true;
        }

        public static Socket GetNewRedisConnection(RedisShard shard)
        {
            var redisBackend = new IPEndPoint(Dns.GetHostAddresses(shard.name)[0], shard.port);
            var socket = new Socket(redisBackend.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
            socket.Connect(redisBackend);
            socket.Send(Encoding.ASCII.GetBytes($"*2\r\n$4\r\nAUTH\r\n${shard.auth.Length}\r\n{shard.auth}\r\n"));

            // TODO(Tianyu): Hacky
            unsafe
            {
                long buf;
                var span = new Span<byte>(&buf, sizeof(long));
                var len = socket.Receive(span);
                Debug.Assert(StringEqual(span, len, "+OK\r\n"));
            }

            return socket;
        }

        public abstract class AbstractDprConnState
        {
            protected Socket socket;
            private int bytesRead;
            private int readHead;


            protected AbstractDprConnState(Socket socket)
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

        internal enum RedisMessageType
        {
            // TODO(Tianyu): Add more
            SIMPLE_STRING,
            ERROR,
            BULK_STRING
        }

        internal struct RedisParserState
        {
            internal RedisMessageType type;
            internal int currentMessageStart;
            internal int subMessageCount;

            public bool ProcessChar(int readHead, byte[] buf)
            {
                switch ((char) buf[readHead])
                {
                    case '+':
                        if (currentMessageStart != -1) return false;
                        currentMessageStart = readHead;
                        type = RedisMessageType.SIMPLE_STRING;
                        subMessageCount = 1;
                        return false;
                    case '-':
                        // Special case for null bulk string
                        if (type == RedisMessageType.BULK_STRING && readHead == currentMessageStart + 1)
                            subMessageCount = 1;
                        if (currentMessageStart != -1) return false;
                        currentMessageStart = readHead;
                        type = RedisMessageType.ERROR;
                        subMessageCount = 1;
                        return false;
                    case '$':
                        if (currentMessageStart != -1) return false;
                        Debug.Assert(currentMessageStart == -1);
                        currentMessageStart = readHead;
                        type = RedisMessageType.BULK_STRING;
                        subMessageCount = 2;
                        return false;
                    case ':':
                    case '*':
                        if (currentMessageStart != -1) return false;
                        throw new NotImplementedException();
                    // TODO(Tianyu): Wouldn't technically work if \r\n in value, but I am not planning to put any so who cares
                    case '\n':
                        if (buf[readHead - 1] != '\r') return false;
                        Debug.Assert(currentMessageStart != -1);
                        // Special case for null string
                        return --subMessageCount == 0;
                    default:
                        // Nothing to do
                        return false;
                }
            }
        }

        public abstract class AbstractRedisConnState
        {
            private Socket socket;
            private int readHead, bytesRead;

            private RedisParserState parserState = new RedisParserState
            {
                type = default,
                currentMessageStart = -1,
                subMessageCount = 0
            };


            protected AbstractRedisConnState(Socket socket)
            {
                this.socket = socket;
            }

            // Return whether we should reset the buffer or keep message buffered
            protected abstract bool HandleRespMessage(byte[] buf, int start, int end);

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
                for (; connState.readHead < connState.bytesRead; connState.readHead++)
                {
                    if (connState.parserState.ProcessChar(connState.readHead, e.Buffer))
                    {
                        var nextHead = connState.readHead + 1;
                        if (connState.HandleRespMessage(e.Buffer, connState.parserState.currentMessageStart, nextHead))
                        {
                            var bytesLeft = connState.bytesRead - nextHead;
                            // Shift buffer to front
                            Array.Copy(e.Buffer, connState.readHead, e.Buffer, 0, bytesLeft);
                            connState.bytesRead = bytesLeft;
                            connState.readHead = 0;
                        }

                        connState.parserState.currentMessageStart = -1;
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

        public static unsafe void SendDpredisRequest(this Socket socket, ReadOnlySpan<byte> dprHeader, ReadOnlySpan<byte> body)
        {
            fixed (byte* h = dprHeader)
            {
                ref var request = ref Unsafe.AsRef<DprBatchRequestHeader>(h);
                var totalSize = request.Size() + body.Length;
                socket.Send(new Span<byte>(&totalSize, sizeof(int)));
                socket.Send(dprHeader.Slice(0, request.Size()));
                socket.Send(body);
            }
        }

        public static unsafe void SendDpredisResponse(this Socket socket, ReadOnlySpan<byte> dprHeader,
            ReadOnlySpan<byte> body)
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