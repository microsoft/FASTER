using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Threading;

namespace FASTER.libdpr
{
    public class GlobalMinDprTableSnapshot: IDprTableSnapshot
    {
        private readonly long globalSafeVersion;

        public GlobalMinDprTableSnapshot(long globalSafeVersion)
        {
            this.globalSafeVersion = globalSafeVersion;
        }
        
        public long SafeVersion(Worker worker) => globalSafeVersion;
    }
    
    // Simple single-threaded server
    public class SimpleTestDprFinderServer
    {
        private readonly Dictionary<long, (long, long)> entries = new Dictionary<long, (long, long)>();
        private Socket socket;
        private Thread serverThread;
        private long lastCut;
        private long maxWorldline;

        public void StartServer(string ip, int port)
        {
            var ipAddr = IPAddress.Parse(ip);
            var endPoint = new IPEndPoint(ipAddr, port);
            socket = new Socket(ipAddr.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
            socket.Bind(endPoint);
            socket.Listen(512);

            serverThread = new Thread(() =>
            {
                // TODO(Tianyu): Magic number
                var buf = new byte[4096];

                try
                {
                    while (true)
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
                        foreach (var (entryWorldline, entryVersion) in entries.Values)
                        {
                            maxWorldline = Math.Max(maxWorldline, entryWorldline);
                            if (maxWorldline != entryWorldline) inRecovery = true;
                            minVersion = Math.Min(entryVersion, minVersion);
                            maxVersion = Math.Max(entryVersion, maxVersion);
                        }

                        if (!inRecovery) lastCut = minVersion;
                        BitConverter.TryWriteBytes(new Span<byte>(buf, 0, 8), maxWorldline);
                        BitConverter.TryWriteBytes(new Span<byte>(buf, 8, 8), lastCut);
                        BitConverter.TryWriteBytes(new Span<byte>(buf, 16, 8), maxVersion);

                        clientSocket.Send(new Span<byte>(buf, 0, 24));
                        clientSocket.Dispose();
                    }
                }
                catch (SocketException)
                {
                    // Fine to exit normally
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

    public class SimpleTestDprFinder : IDprFinder
    {
        // cached local value
        private readonly Worker me;
        private long globalSafeVersionNum = 0, globalMaxVersionNum = 1, systemWorldLine = 0;
        private readonly string ip;
        private readonly int port;
        // TODO(Tianyu): Magic number
        private readonly byte[] buf = new byte[4096];
        private long workerWorldLine = 0, workerVersion = 0;

        public SimpleTestDprFinder(string ip, int port, Worker me)
        {
            this.ip = ip;
            this.port = port;
            this.me = me;
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
            return new GlobalMinDprTableSnapshot(globalSafeVersionNum);
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