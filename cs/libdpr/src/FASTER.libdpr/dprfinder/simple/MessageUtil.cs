using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net.Sockets;

namespace FASTER.libdpr
{
 internal static class MessageUtil
 {
     private static ThreadLocalObjectPool<byte[]> reusableMessageBuffers =
         new ThreadLocalObjectPool<byte[]>(() => new byte[BatchInfo.MaxHeaderSize], 1);
     
        private static unsafe int LongToDecimalString(long a, byte[] buf, int offset)
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

        internal static long LongFromDecimalString(byte[] buf, int start, int end)
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
        
        internal static int WriteRedisBulkString(string val, byte[] buf, int offset)
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

        internal static int WriteRedisBulkString(long val, byte[] buf, int offset)
        {
            var head = offset;
            if (head + 1 >= buf.Length) return 0;
            buf[head++] = (byte) '$';

            var size = LongToDecimalString(sizeof(long), buf, head);
            if (size == 0) return 0;
            head += size;

            if (head + 4 + sizeof(long) >= buf.Length) return 0;
            buf[head++] = (byte) '\r';
            buf[head++] = (byte) '\n';

            BitConverter.TryWriteBytes(new Span<byte>(buf, head, sizeof(long)), val);
            head += sizeof(long);

            buf[head++] = (byte) '\r';
            buf[head++] = (byte) '\n';
            return head - offset;
        }
     
        // TODO(Tianyu): Eliminate ad-hoc serialization code and move this inside WorkerVersion class
        internal static unsafe int WriteRedisBulkString(WorkerVersion val, byte[] buf, int offset)
        {
            var head = offset;
            if (head + sizeof(byte) >= buf.Length) return 0;
            buf[head++] = (byte) '$';

            var size = LongToDecimalString(sizeof(WorkerVersion), buf, head);
            if (size == 0) return 0;
            head += size;

            if (head + 4 + sizeof(WorkerVersion) >= buf.Length) return 0;
            buf[head++] = (byte) '\r';
            buf[head++] = (byte) '\n';

            BitConverter.TryWriteBytes(new Span<byte>(buf, head, sizeof(long)), val.Worker.guid);
            head += sizeof(long);
            BitConverter.TryWriteBytes(new Span<byte>(buf, head, sizeof(long)), val.Version);
            head += sizeof(long);

            buf[head++] = (byte) '\r';
            buf[head++] = (byte) '\n';
            return head - offset;
        }
        
        internal static unsafe int WriteRedisBulkString(IEnumerable<WorkerVersion> val, byte[] buf, int offset)
        {
            var head = offset;
            if (head + sizeof(byte) >= buf.Length) return 0;
            buf[head++] = (byte) '$';
            
            // Find size of encoding up front
            var count = val.Count();
            var totalSize = sizeof(int) + count * sizeof(WorkerVersion);

            var size = LongToDecimalString(totalSize, buf, head);
            if (size == 0) return 0;
            head += size;

            if (head + 4 + totalSize >= buf.Length) return 0;
            buf[head++] = (byte) '\r';
            buf[head++] = (byte) '\n';

            BitConverter.TryWriteBytes(new Span<byte>(buf, head, sizeof(int)), count);
            head += sizeof(int);
            foreach (var wv in val)
            {
                BitConverter.TryWriteBytes(new Span<byte>(buf, head, sizeof(long)), wv.Worker.guid);
                head += sizeof(long);
                BitConverter.TryWriteBytes(new Span<byte>(buf, head, sizeof(long)), wv.Version);
                head += sizeof(long);
            }

            buf[head++] = (byte) '\r';
            buf[head++] = (byte) '\n';
            return head - offset;
        }
        
        internal static int WriteRedisArrayHeader(int numElems, byte[] buf, int offset)
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
        
        internal class DprFinderRedisProtocolConnState
        {
            private Socket socket;
            private int readHead, bytesRead, commandStart = 0;
            private DprFinderCommandParser parser = new DprFinderCommandParser();
            private Action<DprFinderCommand, Socket> commandHandler;

            internal DprFinderRedisProtocolConnState(Socket socket, Action<DprFinderCommand, Socket> commandHandler)
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
                        connState.commandHandler(connState.parser.currentCommand, connState.socket);
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

            internal static void RecvEventArg_Completed(object sender, SocketAsyncEventArgs e)
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

        internal static void SendNewCheckpointCommand(this Socket socket, WorkerVersion checkpointed,
            IEnumerable<WorkerVersion> deps)
        {
            var buf = reusableMessageBuffers.Checkout();
            var head = WriteRedisArrayHeader(3, buf, 0);
            head += WriteRedisBulkString("NewCheckpoint", buf, head);
            head += WriteRedisBulkString(checkpointed, buf, head);
            head += WriteRedisBulkString(deps, buf, head);
            socket.Send(new Span<byte>(buf, 0, head));
            reusableMessageBuffers.Return(buf);
        }
        
        internal static void SendReportRecoveryCommand(this Socket socket, WorkerVersion recovered,
            long worldLine)
        {
            var buf = reusableMessageBuffers.Checkout();
            var head = WriteRedisArrayHeader(3, buf, 0);
            head += WriteRedisBulkString("ReportRecovery", buf, head);
            head += WriteRedisBulkString(recovered, buf, head);
            head += WriteRedisBulkString(worldLine, buf, head);
            socket.Send(new Span<byte>(buf, 0, head));
            reusableMessageBuffers.Return(buf);
        }

        internal static void SendSyncCommand(this Socket socket)
        {
            var buf = reusableMessageBuffers.Checkout();
            var head = WriteRedisArrayHeader(1, buf, 0);
            head += WriteRedisBulkString("Sync", buf, head);
            socket.Send(new Span<byte>(buf, 0, head));
            reusableMessageBuffers.Return(buf);
        }
        
        internal static void SendSyncResponse(this Socket socket, long maxVersion, (byte[], int) serializedState)
        {
            var buf = reusableMessageBuffers.Checkout();
            var head = 0;
            Debug.Assert(head + 1 >= buf.Length);
            buf[head++] = (byte) '$';

            var size = LongToDecimalString(sizeof(long) + serializedState.Item2, buf, head);
            Debug.Assert(size != 0);
            head += size;

            Debug.Assert(head + 4 + sizeof(long) + serializedState.Item2 < buf.Length);
            buf[head++] = (byte) '\r';
            buf[head++] = (byte) '\n';

            BitConverter.TryWriteBytes(new Span<byte>(buf, head, sizeof(long)), maxVersion);
            head += sizeof(long);
            Array.Copy(serializedState.Item1, 0, buf, head, serializedState.Item2);
            head += serializedState.Item2;

            buf[head++] = (byte) '\r';
            buf[head++] = (byte) '\n';
            
            socket.Send(new Span<byte>(buf, 0, head));
            reusableMessageBuffers.Return(buf);
        } 
     
    }
}