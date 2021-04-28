using FASTER.core;
using FASTER.server;
using System;
using System.IO;

namespace FASTER.remote.test
{
    class FixedLenServer<Key, Value> : IDisposable
        where Key : unmanaged
        where Value : unmanaged
    {
        readonly string folderName;
        readonly FasterKVServer<Key, Value, Value, Value, FixedLenServerFunctions<Key, Value>, FixedLenSerializer<Key, Value, Value, Value>> server;

        public FixedLenServer(string folderName, Func<Value, Value, Value> merger, string address = "127.0.0.1", int port = 33278)
        {
            this.folderName = folderName;
            GetSettings(folderName, out var logSettings, out var checkpointSettings, out var indexSize);

            // We use blittable structs Key and Value to construct a costomized server for fixed-length types
            var store = new FasterKV<Key, Value>(indexSize, logSettings, checkpointSettings);

            // We specify FixedLenSerializer as our in-built serializer for blittable (fixed length) types
            // This server can be used with compatible clients such as FixedLenClient and FASTER.benchmark
            server = new FasterKVServer<Key, Value, Value, Value, FixedLenServerFunctions<Key, Value>, FixedLenSerializer<Key, Value, Value, Value>>
                (store, e => new FixedLenServerFunctions<Key, Value>(merger), address, port);
            server.Start();
        }

        public void Dispose()
        {
            server.Dispose();
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

    sealed class FixedLenServerFunctions<Key, Value> : SimpleFunctions<Key, Value, long>
    {
        public FixedLenServerFunctions(Func<Value, Value, Value> merger) : base(merger)
        {
        }
    }
}
