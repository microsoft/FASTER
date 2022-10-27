using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net.Sockets;
using System.Text;
using FASTER.common;

namespace FASTER.libdpr
{
    internal class DprFinderSocketReaderWriter : IDisposable
    {
        private static readonly byte[] OkResponse = Encoding.GetEncoding("ASCII").GetBytes("+OK\r\n");

        // TODO(Tianyu): size information?
        private readonly ThreadLocalObjectPool<byte[]> reusableMessageBuffers =
            new ThreadLocalObjectPool<byte[]>(() => new byte[1 << 15]);

        private Socket socket;

        internal DprFinderSocketReaderWriter(Socket socket)
        {
            this.socket = socket;
        }

        public void Dispose()
        {
            socket.Dispose();
            reusableMessageBuffers.Dispose();
        }

        internal void WaitForAcks(int numAcks)
        {
            // Wait for all of the sent commands to be acked
            var received = 0;
            var buf = reusableMessageBuffers.Checkout();
            while (received < numAcks * 5)
                received += socket.Receive(buf);
            reusableMessageBuffers.Return(buf);
        }

        internal int ReceiveInto(byte[] buffer)
        {
            return socket.Receive(buffer);
        }

        internal void SendOk()
        {
            socket.Send(OkResponse);
        }

        internal int SendGraphReconstruction(Worker worker, IStateObject stateObject)
        {
            var buf = reusableMessageBuffers.Checkout();
            var head = 0;
            var checkpoints = stateObject.GetUnprunedVersions();
            var minVersion = long.MaxValue;
            var numRequests = 0;
            foreach (var (bytes, offset) in checkpoints)
            {
                SerializationUtil.DeserializeCheckpointMetadata(new Span<byte>(bytes, offset, bytes.Length - offset),
                    out var worldLine, out var wv, out var deps);
                head += RespUtil.WriteRedisArrayHeader(4, buf, head);
                head += RespUtil.WriteRedisBulkString("NewCheckpoint", buf, head);
                head += RespUtil.WriteRedisBulkString(worldLine, buf, head);
                head += RespUtil.WriteRedisBulkString(wv, buf, head);
                head += RespUtil.WriteRedisBulkString(deps, buf, head);
                if (minVersion > wv.Version) minVersion = wv.Version;
                numRequests++;
            }

            if (numRequests == 0) return 0;
            head += RespUtil.WriteRedisArrayHeader(2, buf, head);
            head += RespUtil.WriteRedisBulkString("GraphResent", buf, head);
            var committedVersion = new WorkerVersion(worker, minVersion == long.MaxValue ? 0 : minVersion);
            head += RespUtil.WriteRedisBulkString(committedVersion, buf, head);
            socket.Send(buf, 0, head, SocketFlags.None);
            return 1;
        }

        internal void SendAddWorkerCommand(Worker worker)
        {
            var buf = reusableMessageBuffers.Checkout();
            var head = RespUtil.WriteRedisArrayHeader(2, buf, 0);
            head += RespUtil.WriteRedisBulkString("AddWorker", buf, head);
            head += RespUtil.WriteRedisBulkString(worker.guid, buf, head);
            socket.Send(buf, 0, head, SocketFlags.None);
            reusableMessageBuffers.Return(buf);
        }

        internal void SendDeleteWorkerCommand(Worker worker)
        {
            var buf = reusableMessageBuffers.Checkout();
            var head = RespUtil.WriteRedisArrayHeader(2, buf, 0);
            head += RespUtil.WriteRedisBulkString("DeleteWorker", buf, head);
            head += RespUtil.WriteRedisBulkString(worker.guid, buf, head);
            socket.Send(buf, 0, head, SocketFlags.None);
            reusableMessageBuffers.Return(buf);
        }

        internal void SendNewCheckpointCommand(long worldLine, WorkerVersion checkpointed,
            IEnumerable<WorkerVersion> deps)
        {
            var buf = reusableMessageBuffers.Checkout();
            var head = RespUtil.WriteRedisArrayHeader(4, buf, 0);
            head += RespUtil.WriteRedisBulkString("NewCheckpoint", buf, head);
            head += RespUtil.WriteRedisBulkString(worldLine, buf, head);
            head += RespUtil.WriteRedisBulkString(checkpointed, buf, head);
            head += RespUtil.WriteRedisBulkString(deps, buf, head);
            socket.Send(buf, 0, head, SocketFlags.None);
            reusableMessageBuffers.Return(buf);
        }

        internal void SendReportRecoveryCommand(WorkerVersion recovered, long worldLine)
        {
            var buf = reusableMessageBuffers.Checkout();
            var head = RespUtil.WriteRedisArrayHeader(3, buf, 0);
            head += RespUtil.WriteRedisBulkString("ReportRecovery", buf, head);
            head += RespUtil.WriteRedisBulkString(recovered, buf, head);
            head += RespUtil.WriteRedisBulkString(worldLine, buf, head);
            socket.Send(buf, 0, head, SocketFlags.None);
            reusableMessageBuffers.Return(buf);
        }

        internal void SendSyncCommand()
        {
            var buf = reusableMessageBuffers.Checkout();
            var head = RespUtil.WriteRedisArrayHeader(1, buf, 0);
            head += RespUtil.WriteRedisBulkString("Sync", buf, head);
            socket.Send(buf, 0, head, SocketFlags.None);
            reusableMessageBuffers.Return(buf);
        }

        internal void SendSyncResponse(long maxVersion, (byte[], int) serializedState)
        {
            var buf = reusableMessageBuffers.Checkout();
            var head = 0;
            buf[head++] = (byte)'$';

            var size = RespUtil.LongToDecimalString(sizeof(long) + serializedState.Item2, buf, head);
            Debug.Assert(size != 0);
            head += size;

            Debug.Assert(head + 4 + sizeof(long) + serializedState.Item2 < buf.Length);
            buf[head++] = (byte)'\r';
            buf[head++] = (byte)'\n';

            Utility.TryWriteBytes(new Span<byte>(buf, head, sizeof(long)), maxVersion);
            head += sizeof(long);
            Array.Copy(serializedState.Item1, 0, buf, head, serializedState.Item2);
            head += serializedState.Item2;

            buf[head++] = (byte)'\r';
            buf[head++] = (byte)'\n';

            socket.Send(buf, 0, head, SocketFlags.None);
            reusableMessageBuffers.Return(buf);
        }

        internal void SendAddWorkerResponse((long, long) result)
        {
            var buf = reusableMessageBuffers.Checkout();
            var head = 0;
            buf[head++] = (byte)'$';

            var size = RespUtil.LongToDecimalString(2 * sizeof(long), buf, head);
            Debug.Assert(size != 0);
            head += size;

            Debug.Assert(head + 4 + 2 * sizeof(long) < buf.Length);
            buf[head++] = (byte)'\r';
            buf[head++] = (byte)'\n';

            Utility.TryWriteBytes(new Span<byte>(buf, head, sizeof(long)), result.Item1);
            head += sizeof(long);
            Utility.TryWriteBytes(new Span<byte>(buf, head, sizeof(long)), result.Item2);
            head += sizeof(long);

            buf[head++] = (byte)'\r';
            buf[head++] = (byte)'\n';

            socket.Send(buf, 0, head, SocketFlags.None);
            reusableMessageBuffers.Return(buf);
        }
    }

    internal class DprFinderRedisProtocolConnState
    {
        private readonly Action<DprFinderCommand, DprFinderSocketReaderWriter> commandHandler;
        private readonly DprFinderCommandParser parser = new DprFinderCommandParser();
        private int readHead, bytesRead, commandStart;
        private readonly DprFinderSocketReaderWriter readerWriter;
        private readonly Socket socket;

        internal DprFinderRedisProtocolConnState(Socket socket, Action<DprFinderCommand, DprFinderSocketReaderWriter> commandHandler)
        {
            this.socket = socket;
            readerWriter = new DprFinderSocketReaderWriter(socket);
            this.commandHandler = commandHandler;
        }

        private static bool HandleReceiveCompletion(SocketAsyncEventArgs e)
        {
            var connState = (DprFinderRedisProtocolConnState)e.UserToken;
            if (e.BytesTransferred == 0 || e.SocketError != SocketError.Success)
            {
                connState.socket.Dispose();
                e.Dispose();
                return false;
            }

            connState.bytesRead += e.BytesTransferred;
            for (; connState.readHead < connState.bytesRead; connState.readHead++)
                if (connState.parser.ProcessChar(connState.readHead, e.Buffer))
                {
                    connState.commandHandler(connState.parser.currentCommand, connState.readerWriter);
                    connState.commandStart = connState.readHead + 1;
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
            var connState = (DprFinderRedisProtocolConnState)e.UserToken;
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
}