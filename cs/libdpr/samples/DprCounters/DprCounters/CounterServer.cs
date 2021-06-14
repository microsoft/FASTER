using System;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using FASTER.libdpr;

namespace DprCounters
{
    public class CounterServer
    {
        private IDprFinder dprFinder;
        private Socket socket;
        private DprServer<CounterStateObject> dprServer;

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
                    receivedBytes += socket.Receive(inBuffer, receivedBytes, inBuffer.Length - receivedBytes, SocketFlags.None);

                var size = BitConverter.ToInt32(inBuffer);
                while (receivedBytes < size + sizeof(int))
                    receivedBytes += socket.Receive(inBuffer, receivedBytes, inBuffer.Length - receivedBytes, SocketFlags.None);

                var request = new ReadOnlySpan<byte>(inBuffer, sizeof(int), size - sizeof(int));
                var response = new Span<byte>(outBuffer, sizeof(int), outBuffer.Length - sizeof(int));
                if (!dprServer.RequestBatchBegin(request, response, out var tracker, out var headerSize))
                {
                    conn.Send(outBuffer, 0, headerSize, SocketFlags.None);
                    conn.Close();
                }
                else
                {
                    tracker.MarkOneOperationVersion(0, dprServer.StateObject().Version());
                    var result = dprServer.StateObject().value;
                    dprServer.StateObject().value +=
                        BitConverter.ToInt64(new Span<byte>(inBuffer, sizeof(int) + size - sizeof(long), sizeof(long)));
                    var responseHeaderSize = dprServer.SignalBatchFinish(request, response, tracker);
                    BitConverter.TryWriteBytes(new Span<byte>(outBuffer, 0, sizeof(int)), sizeof(long) + responseHeaderSize);
                    BitConverter.TryWriteBytes(new Span<byte>(outBuffer, responseHeaderSize + sizeof(int), outBuffer.Length - responseHeaderSize - sizeof(int)), result);
                    conn.Send(outBuffer, 0, sizeof(int) + responseHeaderSize + sizeof(long), SocketFlags.None);
                    conn.Close();
                }
            }
        }

    }
}