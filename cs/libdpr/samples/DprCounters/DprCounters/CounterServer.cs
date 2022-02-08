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
        private Socket socket;
        private DprServer<CounterStateObject> dprServer;
        private ManualResetEventSlim termination;

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
            // Each DPR worker should be backed by one state object. The state object exposes some methods 
            // for the DPR logic to invoke when necessary, but DPR does not otherwise mediate user interactions
            // with it. 
            var stateObject = new CounterStateObject(checkpointDir);
            // A DPR server provides DPR methods that the users should invoke at appropriate points of execution. There
            // should be one DPR server per worker in the cluster
            dprServer = new DprServer<CounterStateObject>(dprFinder, me, stateObject);
            
            var localEndpoint = new IPEndPoint(IPAddress.Parse(ip), port); 
            socket = new Socket(localEndpoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
            socket.Bind(localEndpoint);
        }

        public void RunServer()
        {
            dprServer.ConnectToCluster();
            
            termination = new ManualResetEventSlim();
            // DprServer must be continually refreshed and checkpointed for the system to make progress. It is easiest
            // to simply spawn a background thread to do that. 
            var backgroundThread = new Thread(() =>
            {
                while (!termination.IsSet)
                {
                    Thread.Sleep(10);
                    // A DprServer has built-in timers to rate-limit checkpoints and refreshes if needed
                    dprServer.TryRefreshAndCheckpoint(100, 10);
                }
            });
            backgroundThread.Start();

            // Allocate some memory buffers for a sequential, custom-built wire protocol for our CounterServer.
            // DPR is not a net work protocol, although it expects some help from the host system to pass information
            // around. 
            var inBuffer = new byte[1 << 15];
            var outBuffer = new byte[1 << 15];
            // A simple, sequential, blocking server implementation.
            socket.Listen(512);
            while (!termination.IsSet)
            {
                Socket conn;
                try
                {
                    conn = socket.Accept();
                }
                catch (SocketException e)
                {
                    return;
                }

                var receivedBytes = 0;
                // Our protocol first reads a size field of the combined DPR header + messages
                while (receivedBytes < sizeof(int))
                    receivedBytes += conn.Receive(inBuffer, receivedBytes, inBuffer.Length - receivedBytes,
                        SocketFlags.None);

                var size = BitConverter.ToInt32(inBuffer);
                // Receive the combined message.
                while (receivedBytes < size + sizeof(int))
                    receivedBytes += conn.Receive(inBuffer, receivedBytes, inBuffer.Length - receivedBytes,
                        SocketFlags.None);

                // We can obtain the DPR header by computing the size information
                var request = new ReadOnlySpan<byte>(inBuffer, sizeof(int), size - sizeof(int));
                
                var responseBuffer = new Span<byte>(outBuffer, sizeof(int), outBuffer.Length - sizeof(int));

                int responseHeaderSize;
                long result = 0;
                // Before executing server-side logic, check with DPR to start tracking for the batch and make sure 
                // we are allowed to execute it. If not, the response header will be populated and we should immediately
                // return that to the client side libDPR.
                if (dprServer.RequestRemoteBatchBegin(request, out var tracker))
                {
                    // If so, protect the execution and obtain the version this batch will execute in
                    var v = dprServer.StateObject().VersionScheme().Enter();
                    // Add operation to version tracking using the libDPR-supplied version tracker
                    tracker.MarkOneOperationVersion(0, v);
                    
                    // Execute the request batch. In this case, always a single increment operation.
                    result = dprServer.StateObject().value;
                    dprServer.StateObject().value +=
                        BitConverter.ToInt64(new Span<byte>(inBuffer, sizeof(int) + size - sizeof(long), sizeof(long)));
                    
                    // Once requests are done executing, stop protecting this batch so DPR can progress
                    dprServer.StateObject().VersionScheme().Leave();
                    // Signal the end of execution for DPR to finish up and populate a response header
                    responseHeaderSize = dprServer.SignalRemoteBatchFinish(request, responseBuffer, tracker);
                }
                else
                {
                    responseHeaderSize = dprServer.ComposeErrorResponse(request, responseBuffer);
                }

                // The server is then free to convey the result back to the client any way it wants, so long as it
                // forwards the DPR response header. In this case, we are using the same format as above by concatenating
                // the DPR response and our response
                BitConverter.TryWriteBytes(new Span<byte>(outBuffer, 0, sizeof(int)),
                    sizeof(long) + responseHeaderSize);
                BitConverter.TryWriteBytes(
                    new Span<byte>(outBuffer, responseHeaderSize + sizeof(int),
                        outBuffer.Length - responseHeaderSize - sizeof(int)), result);
                conn.Send(outBuffer, 0, sizeof(int) + responseHeaderSize + sizeof(long), SocketFlags.None);
                // One socket connection per client for simplicity
                conn.Close();
            }

            backgroundThread.Join();
        }

        public void StopServer()
        {
            socket.Dispose();
            termination.Set();
        }
    }
}