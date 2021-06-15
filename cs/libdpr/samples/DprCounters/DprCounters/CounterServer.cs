using System;
using System.Net;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Threading;
using FASTER.libdpr;

namespace DprCounters
{
    /// <summary>
    /// A single-threaded blocking server that accepts requests to atomically increment a counter. DPR-protected. 
    /// </summary>
    public class CounterServer
    {
        private IDprFinder dprFinder;
        private Socket socket;
        private DprServer<CounterStateObject> dprServer;

        /// <summary>
        /// Create a new CounterServer.
        /// </summary>
        /// <param name="ip"> ip address to listen </param>
        /// <param name="port"> port number to listen </param>
        /// <param name="me"> id of worker in DPR cluster </param>
        /// <param name="checkpointDir"> directory name to write checkpoint files to </param>
        /// <param name="dprFinder"> DprFinder for the cluster </param>
        public CounterServer(string ip, int port, Worker me, string checkpointDir, IDprFinder dprFinder)
        {
            var stateObject = new CounterStateObject(checkpointDir, dprFinder.SafeVersion(me));
            dprServer = new DprServer<CounterStateObject>(dprFinder, me, stateObject);
            var localEndpoint = new IPEndPoint(IPAddress.Parse(ip), port);
            socket = new Socket(localEndpoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
            socket.Bind(localEndpoint);
        }

        public void RunServer()
        {
            var backgroundThread = new Thread(() =>
            {
                Thread.Sleep(10);
                dprServer.TryRefreshAndCheckpoint(100, 10);
            });
            backgroundThread.Start();

            var inBuffer = new byte[1 << 15];
            var outBuffer = new byte[1 << 15];
            socket.Listen(512);
            while (true)
            {
                var conn = socket.Accept();
                var receivedBytes = 0;
                while (receivedBytes < sizeof(int))
                    receivedBytes += socket.Receive(inBuffer, receivedBytes, inBuffer.Length - receivedBytes,
                        SocketFlags.None);

                var size = BitConverter.ToInt32(inBuffer);
                while (receivedBytes < size + sizeof(int))
                    receivedBytes += socket.Receive(inBuffer, receivedBytes, inBuffer.Length - receivedBytes,
                        SocketFlags.None);

                ref var request = ref MemoryMarshal.GetReference(MemoryMarshal.Cast<byte, DprBatchRequestHeader>(
                    new ReadOnlySpan<byte>(inBuffer, sizeof(int), size - sizeof(int))));
                var responseBuffer = new Span<byte>(outBuffer, sizeof(int), outBuffer.Length - sizeof(int));
                ref var response =
                    ref MemoryMarshal.GetReference(MemoryMarshal.Cast<byte, DprBatchResponseHeader>(responseBuffer));
                var responseHeaderSize = response.Size();
                long result = 0;
                if (dprServer.RequestBatchBegin(ref request, ref response, out var tracker))
                {
                    var v = dprServer.StateObject().VersionScheme().Enter();
                    tracker.MarkOneOperationVersion(0, v);
                    result = dprServer.StateObject().value;
                    dprServer.StateObject().value +=
                        BitConverter.ToInt64(new Span<byte>(inBuffer, sizeof(int) + size - sizeof(long), sizeof(long)));
                    dprServer.StateObject().VersionScheme().Leave();
                    responseHeaderSize = dprServer.SignalBatchFinish(ref request, responseBuffer, tracker);
                }


                BitConverter.TryWriteBytes(new Span<byte>(outBuffer, 0, sizeof(int)),
                    sizeof(long) + responseHeaderSize);
                BitConverter.TryWriteBytes(
                    new Span<byte>(outBuffer, responseHeaderSize + sizeof(int),
                        outBuffer.Length - responseHeaderSize - sizeof(int)), result);
                conn.Send(outBuffer, 0, sizeof(int) + responseHeaderSize + sizeof(long), SocketFlags.None);
                conn.Close();
            }
        }
    }
}