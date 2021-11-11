using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using FASTER.libdpr;

namespace DprCounters
{
    /// <summary>
    /// Client session to a cluster of CounterServers. DPR-capable. 
    /// </summary>
    public class CounterClientSession
    {
        private DprClientSession session;
        private Dictionary<Worker, IPEndPoint> cluster;
        private byte[] serializationBuffer = new byte[1 << 15];
        private long serialNum = 0;

        private ClientVersionTracker versionTracker = new ClientVersionTracker();
        
        /// <summary>
        /// Create a new client session
        /// </summary>
        /// <param name="session"> dpr session </param>
        /// <param name="cluster"> static cluster mapping </param>
        public CounterClientSession(DprClientSession session, Dictionary<Worker, IPEndPoint> cluster)
        {
            this.session = session;
            this.cluster = cluster;
        }


        /// <summary>
        /// Increments the counter at the given location by the given amount
        /// </summary>
        /// <param name="worker"> counter location</param>
        /// <param name="amount"> amount to increment counter by</param>
        /// <param name="result"> result </param>
        /// <returns>unique id for operation </returns>
        public long Increment(Worker worker, long amount, out long result)
        {
            var id = serialNum++;
            // Add unique id to tracking
            versionTracker.Add(id);
            // Before sending operations, consult with DPR client for a batch header. For this simple example, we 
            // are using one message per batch
            session.IssueBatch(1, worker, out var header);
            // Use a serialization scheme that writes a size field and then the DPR header and request in sequence.
            BitConverter.TryWriteBytes(new Span<byte>(serializationBuffer, 0, sizeof(int)),
                header.Length + sizeof(long));
            header.CopyTo(new Span<byte>(serializationBuffer, sizeof(int), header.Length));
            BitConverter.TryWriteBytes(new Span<byte>(serializationBuffer, header.Length + sizeof(int), sizeof(long)), amount);
            
            // For simplicity, start a new socket every operation
            var endPoint = cluster[worker];
            using var socket = new Socket(endPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
            socket.Connect(endPoint);
            socket.Send(serializationBuffer, 0, sizeof(int) + header.Length + sizeof(long), SocketFlags.None);

            // We expect the same format back from server. First read the size field
            var receivedBytes = 0;
            while (receivedBytes < sizeof(int))
                receivedBytes += socket.Receive(serializationBuffer, receivedBytes, serializationBuffer.Length - receivedBytes, SocketFlags.None);

            var size = BitConverter.ToInt32(serializationBuffer);
            // Now wait until the entire message arrives
            while (receivedBytes < size + sizeof(int))
                receivedBytes += socket.Receive(serializationBuffer, receivedBytes, serializationBuffer.Length - receivedBytes, SocketFlags.None);

            // Forward the DPR response header after we are done
            var success = session.ResolveBatch(new Span<byte>(serializationBuffer, sizeof(int), size - sizeof(long)), out var vector);
            // Because we use one-off sockets, resolve batch should never fail.
            Debug.Assert(success);

            versionTracker.Resolve(id, new WorkerVersion(worker, vector[0]));

            // (Non-DPR) Response is 8 bytes, 
            result = BitConverter.ToInt64(serializationBuffer, sizeof(int) + size - sizeof(long));
            return id;
        }

        /// <summary>
        /// Check whether the operation identified by seq is committed
        /// </summary>
        /// <param name="seq">operation to check</param>
        /// <returns>whether operation is committed</returns>
        public bool Committed(long seq)
        {
            if (session.TryGetCurrentCut(out var cut))
                versionTracker.HandleCommit(cut);
            
            var cp = versionTracker.GetCommitPoint();
            // Because the session is strictly sequential, operation will never be in exception list.
            Debug.Assert(cp.ExcludedSerialNos.Count == 0);
            return seq < cp.UntilSerialNo;
        }
    }
}