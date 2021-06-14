using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using FASTER.libdpr;

namespace DprCounters
{
    public class CounterClientSession
    {
        private DprClientSession session;
        private Dictionary<Worker, IPEndPoint> cluster;
        private byte[] serializationBuffer = new byte[1 << 15];

        public CounterClientSession(DprClientSession session, Dictionary<Worker, IPEndPoint> cluster)
        {
            this.session = session;
            this.cluster = cluster;
        }

        public long Increment(Worker worker, long amount, out long result)
        {
            var seqNo = session.IssueBatch(1, worker, out var header);
            BitConverter.TryWriteBytes(new Span<byte>(serializationBuffer, 0, sizeof(int)),
                header.Length + sizeof(long));
            header.CopyTo(new Span<byte>(serializationBuffer, sizeof(int), header.Length));
            BitConverter.TryWriteBytes(new Span<byte>(serializationBuffer, header.Length + sizeof(int), sizeof(long)), amount);
            
            // For simplicity, start a new socket every operation
            var endPoint = cluster[worker];
            using var socket = new Socket(endPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
            socket.Connect(endPoint);
            socket.Send(serializationBuffer, 0, 2 * sizeof(int) + header.Length, SocketFlags.None);

            var receivedBytes = 0;
            while (receivedBytes < sizeof(int))
                receivedBytes += socket.Receive(serializationBuffer, receivedBytes, serializationBuffer.Length - receivedBytes, SocketFlags.None);

            var size = BitConverter.ToInt32(serializationBuffer);
            while (receivedBytes < size + sizeof(int))
                receivedBytes += socket.Receive(serializationBuffer, receivedBytes, serializationBuffer.Length - receivedBytes, SocketFlags.None);

            unsafe
            {
                bool success;
                fixed (byte* dprHeader = &serializationBuffer[sizeof(int)])
                    success = session.ResolveBatch(ref Unsafe.AsRef<DprBatchResponseHeader>(dprHeader));
                Debug.Assert(success);
            }

            // Response is 8 bytes, 
            result = BitConverter.ToInt64(serializationBuffer, sizeof(int) + size - sizeof(long));
            return seqNo;
        }

        public bool Committed(long seq)
        {
            var cp = session.GetCommitPoint();
            // Because the session is strictly sequential, operation will never be in exception list.
            Debug.Assert(cp.ExcludedSerialNos.Count == 0);
            return seq < session.GetCommitPoint().UntilSerialNo;
        }
    }
}