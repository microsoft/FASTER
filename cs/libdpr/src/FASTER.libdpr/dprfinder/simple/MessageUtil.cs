using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Text;
using FASTER.core;

namespace FASTER.libdpr
{
 public static class MessageUtil
 {
     private static ThreadLocalObjectPool<byte[]> reusableMessageBuffers =
         new ThreadLocalObjectPool<byte[]>(() => new byte[BatchInfo.MaxHeaderSize], 1);
     
        private unsafe static int LongToDecimalString(long a, byte[] buf, int offset)
        {
            var digits = stackalloc byte[20];
            var numDigits = 0;
            do
            {
                digits[numDigits] = (byte) (a % 10 + '0');
                numDigits++;
                a /= 10;
            } while (a > 0);

            var head = offset;

            if (head + numDigits >= buf.Length) return 0;
            for (var i = numDigits - 1; i >= 0; i--)
                buf[head++] = digits[i];
            return head - offset;
        }

        public static long LongFromDecimalString(byte[] buf, int start, int end)
        {
            var negative = false;
            if (buf[start] == '-')
            {
                negative = true;
                start++;
            }

            long result = 0;

            for (var i = start; i < end; i++)
            {
                result *= 10;
                result += buf[i] - '0';
            }

            return negative ? -result : result;
        }
        
        public static int WriteRedisBulkString(string val, byte[] buf, int offset)
        {
            var head = offset;
            if (head + 1 >= buf.Length) return 0;
            buf[head++] = (byte) '$';

            var size = LongToDecimalString(val.Length, buf, head);
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

            var size = LongToDecimalString(numElems, buf, head);
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
        
        internal class DprFinderRedisProtocolConnState
        {
            private Socket socket;
            private int readHead, bytesRead, commandStart = 0;
            private SimpleRedisParser parser = new SimpleRedisParser();
            private Action<DprFinderCommand> commandHandler;

            internal DprFinderRedisProtocolConnState(Socket socket, Action<DprFinderCommand> commandHandler)
            {
                this.socket = socket;
                this.commandHandler = commandHandler;
            }
            
            private static bool HandleReceiveCompletion(SocketAsyncEventArgs e)
            {
                var connState = (DprFinderRedisProtocolConnState) e.UserToken;
                if (e.BytesTransferred == 0 || e.SocketError != SocketError.Success)
                {
                    connState.socket.Dispose();
                    e.Dispose();
                    return false;
                }

                connState.bytesRead += e.BytesTransferred;
                for (; connState.readHead < connState.bytesRead; connState.readHead++)
                {

                    if (connState.parser.ProcessChar(connState.readHead, e.Buffer))
                    {
                        connState.commandHandler(connState.parser.currentCommand);
                        connState.commandStart = connState.readHead + 1;
                    }
                }
                

                // TODO(Tianyu): Magic number
                // If less than some certain number of bytes left in the buffer, shift buffer content to head to free
                // up some space. Don't want to do this too often. Obviously ok to do if no bytes need to be copied (
                // the current end of buffer marks the end of a command, and we can discard the entire buffer).
                if (e.Buffer.Length - connState.readHead < 4096 || connState.readHead == connState.commandStart)
                {
                    var bytesLeft = connState.bytesRead - connState.commandStart;
                    // Shift buffer to front
                    Array.Copy(e.Buffer, connState.commandStart, e.Buffer, 0, bytesLeft);
                    connState.bytesRead = bytesLeft;
                    connState.readHead -= connState.commandStart;
                    connState.commandStart = 0;
                }
                e.SetBuffer(connState.readHead, e.Buffer.Length - connState.readHead);
                return true;
            }

            public static void RecvEventArg_Completed(object sender, SocketAsyncEventArgs e)
            {
                var connState = (DprFinderRedisProtocolConnState) e.UserToken;
                try
                {
                    do
                    {
                        // No more things to receive
                        if (!HandleReceiveCompletion(e)) return;
                    } while (!connState.socket.ReceiveAsync(e));
                }
                catch (ObjectDisposedException)
                {
                    // Probably caused by a normal cancellation from this side. Ok to ignore
                }
            }
        }

        public static unsafe void SendNewCheckpoint(this Socket socket, WorkerVersion checkpointed,
            IEnumerable<WorkerVersion> deps)
        {
            var buf = reusableMessageBuffers.Checkout();
            var head = WriteRedisArrayHeader(3, buf, 0);
            head += WriteRedisBulkString("NewCheckpoint", buf, head);
            // head += WriteRedisBulkString()
            // TODO(Tianyu): Write
            socket.Send(new Span<byte>(buf, 0, head));
            reusableMessageBuffers.Return(buf);
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