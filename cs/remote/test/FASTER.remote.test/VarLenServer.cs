using FASTER.core;
using FASTER.server;
using System;
using System.IO;

namespace FASTER.remote.test
{
    class VarLenServer : IDisposable
    {
        readonly string folderName;
        readonly FasterKVServer<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, SpanByteFunctionsForServer<long>, SpanByteSerializer> server;
        readonly FasterKV<SpanByte, SpanByte> store;

        public VarLenServer(string folderName, string address = "127.0.0.1", int port = 33278)
        {
            this.folderName = folderName;
            GetSettings(folderName, out var logSettings, out var checkpointSettings, out var indexSize);

            // We use blittable structs Key and Value to construct a costomized server for fixed-length types
            store = new FasterKV<SpanByte, SpanByte>(indexSize, logSettings, checkpointSettings);

            var provider =
                new FasterKVBackendProvider<SpanByte, SpanByte, SpanByteFunctionsForServer<long>, SpanByteSerializer>(
                    store, wp => new SpanByteFunctionsForServer<long>(wp), new SpanByteSerializer());
            // We specify FixedLenSerializer as our in-built serializer for blittable (fixed length) types
            // This server can be used with compatible clients such as FixedLenClient and FASTER.benchmark
            server = server = new FasterKVServer<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, SpanByteFunctionsForServer<long>, SpanByteSerializer>
                (provider, address, port);
            server.Start();
        }

        public void Dispose()
        {
            server.Dispose();
            store.Dispose();
            new DirectoryInfo(folderName).Delete(true);
        }

        private void GetSettings(string LogDir, out LogSettings logSettings, out CheckpointSettings checkpointSettings, out int indexSize)
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
