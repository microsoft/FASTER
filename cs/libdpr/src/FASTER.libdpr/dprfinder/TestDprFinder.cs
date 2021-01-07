using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Drawing;
using System.Net;
using System.Net.Sockets;
using System.Threading;

namespace FASTER.libdpr
{
    // Simple single-threaded server
    public class TestDprFinderServer
    {
        private Dictionary<long, (long, long)> entries = new Dictionary<long, (long, long)>();
        private Socket socket;
        private Thread serverThread;
        private long lastCut = 0;
        private long maxWorldline = 0;

        public void StartServer(string ip, int port)
        {
            var ipAddr = IPAddress.Parse(ip);
            var endPoint = new IPEndPoint(ipAddr, port);
            socket = new Socket(ipAddr.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
            socket.Bind(endPoint);
            socket.Listen(512);
            
            serverThread = new Thread(() =>
            {
                var buf = new byte[4096];

                while (true)
                {
                    try
                    {
                        var clientSocket = socket.Accept();
                        clientSocket.NoDelay = true;
                        var received = clientSocket.Receive(buf);
                        // 8 bytes worker id + 8 bytes world-line + 8 bytes version
                        Debug.Assert(received == 24);
                        var workerId = BitConverter.ToInt64(buf, 0);
                        var worldLine = BitConverter.ToInt64(buf, 8);
                        var version = BitConverter.ToInt64(buf, 16);
                        entries.TryAdd(workerId,
                            ValueTuple.Create(worldLine, version));

                        var inRecovery = false;
                        long minVersion = long.MaxValue, maxVersion = long.MinValue;
                        foreach (var entry in entries.Values)
                        {
                            maxWorldline = Math.Max(maxWorldline, entry.Item1);
                            if (maxWorldline != entry.Item1) inRecovery = true;
                            minVersion = Math.Min(entry.Item2, minVersion);
                            maxVersion = Math.Max(entry.Item2, maxVersion);
                        }

                        if (!inRecovery) lastCut = minVersion;
                        BitConverter.TryWriteBytes(new Span<byte>(buf, 0, 8), maxWorldline);
                        BitConverter.TryWriteBytes(new Span<byte>(buf, 8, 8), lastCut);
                        BitConverter.TryWriteBytes(new Span<byte>(buf, 16, 8), maxVersion);

                        clientSocket.Send(new Span<byte>(buf, 0, 24));
                        clientSocket.Dispose();
                    }
                    catch (ObjectDisposedException)
                    {
                        // Fine to exit normally
                    }
                }
            });

            serverThread.Start();
        }

        public void EndServer()
        {
            socket.Dispose();
            serverThread.Join();
        }
    }
    
    public class TestDprFinder : IDprFinder
    {
        // cached local value
        private readonly Worker me;
        private long globalSafeVersionNum = 0, globalMaxVersionNum = 1, systemWorldLine = 0;
        private string ip;
        private int port;
        private byte[] buf = new byte[4096];
        private long workerWorldLine = 0, workerVersion = 0;

        public TestDprFinder(string ip, int port)
        {
            this.ip = ip;
            this.port = port;
        }
        
        public void Clear()
        {
        }

        private Socket GetNewConnection()
        {
            var dprFinderBackend = new IPEndPoint(IPAddress.Parse(ip), port);
            var socket = new Socket(dprFinderBackend.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
            socket.Connect(dprFinderBackend);
            socket.NoDelay = true;
            return socket;
        }

        private void Sync()
        {
            var conn = GetNewConnection();
            BitConverter.TryWriteBytes(new Span<byte>(buf, 0, 8), me.guid);
            BitConverter.TryWriteBytes(new Span<byte>(buf, 8, 8), workerWorldLine);
            BitConverter.TryWriteBytes(new Span<byte>(buf, 16, 8), workerVersion);

            conn.Send(new Span<byte>(buf, 0, 24));
            conn.Receive(buf);

            systemWorldLine = BitConverter.ToInt64(buf, 0);
            globalSafeVersionNum = BitConverter.ToInt64(buf, 8);
            globalMaxVersionNum = BitConverter.ToInt64(buf, 16);
            conn.Dispose();
        }

        public void ReportRecovery(long worldLine, WorkerVersion latestRecoveredVersion)
        {
            workerVersion = worldLine;
            workerVersion = latestRecoveredVersion.Version;
            Sync();
        }
        
        
        public long SafeVersion(Worker worker)
        {
            return globalSafeVersionNum;
        }

        public long GlobalMaxVersion()
        {
            return globalMaxVersionNum;
        }

        public long SystemWorldLine()
        {
            return systemWorldLine;
        }

        public IDprTableSnapshot ReadSnapshot()
        {
            return new V1DprTableSnapshot(globalSafeVersionNum);
        }

        public void ReportNewPersistentVersion(WorkerVersion persisted, IEnumerable<WorkerVersion> deps)
        {
            workerVersion = persisted.Version;
            Sync();
        }

        public void Refresh()
        {
            Sync();
        }
    }
}