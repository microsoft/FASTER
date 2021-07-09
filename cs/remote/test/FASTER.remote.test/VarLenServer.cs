using System;
using System.IO;
using FASTER.core;
using FASTER.server;
using FASTER.common;

namespace FASTER.remote.test
{
    class VarLenServer : IDisposable
    {
        readonly string folderName;
        readonly FasterServer server;
        readonly FasterKV<SpanByte, SpanByte> store;

        public VarLenServer(string folderName, string address = "127.0.0.1", int port = 33278)
        {
            this.folderName = folderName;
            GetSettings(folderName, out var logSettings, out var checkpointSettings, out var indexSize);

            // We use blittable structs Key and Value to construct a costomized server for fixed-length types
            store = new FasterKV<SpanByte, SpanByte>(indexSize, logSettings, checkpointSettings);

            // Create session provider for VarLen
            var provider = new SpanByteFasterKVProvider(store);

            server = new FasterServer(address, port);
            server.Register(WireFormat.DefaultVarLenKV, provider);
            server.Start();
        }

        public void Dispose()
        {
            server.Dispose();
            store.Dispose();
            new DirectoryInfo(folderName).Delete(true);
        }

        private static void GetSettings(string LogDir, out LogSettings logSettings, out CheckpointSettings checkpointSettings, out int indexSize)
        {
            logSettings = new LogSettings { PreallocateLog = false };

            logSettings.PageSizeBits = 20;
            logSettings.MemorySizeBits = 25;
            logSettings.SegmentSizeBits = 30;
            indexSize = 1 << 20;

            var device = LogDir == "" ? new NullDevice() : Devices.CreateLogDevice(LogDir + "/hlog", preallocateFile: false);
            logSettings.LogDevice = device;

            string CheckpointDir = null;
            if (CheckpointDir == null && LogDir == null)
                checkpointSettings = null;
            else
                checkpointSettings = new CheckpointSettings
                {
                    CheckPointType = CheckpointType.FoldOver,
                    CheckpointDir = CheckpointDir ?? (LogDir + "/checkpoints")
                };
        }
    }
}
