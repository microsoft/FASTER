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
        readonly SpanByteFasterKVProvider provider;
        readonly SubscribeKVBroker<SpanByte, SpanByte, SpanByte, IKeyInputSerializer<SpanByte, SpanByte>> kvBroker;
        readonly SubscribeBroker<SpanByte, SpanByte, IKeySerializer<SpanByte>> broker;

        public VarLenServer(string folderName, string address = "127.0.0.1", int port = 33278, bool enablePubSub = false)
        {
            this.folderName = folderName;
            GetSettings(folderName, out var logSettings, out var checkpointSettings, out var indexSize);

            store = new FasterKV<SpanByte, SpanByte>(indexSize, logSettings, checkpointSettings);

            if (enablePubSub)
            {
                kvBroker = new SubscribeKVBroker<SpanByte, SpanByte, SpanByte, IKeyInputSerializer<SpanByte, SpanByte>>(new SpanByteKeyInputSerializer(), null, true);
                broker = new SubscribeBroker<SpanByte, SpanByte, IKeySerializer<SpanByte>>(new SpanByteKeySerializer(), null, true);
            }

            // Create session provider for VarLen
            provider = new SpanByteFasterKVProvider(store, kvBroker, broker);

            server = new FasterServer(address, port);
            server.Register(WireFormat.DefaultVarLenKV, provider);
            server.Start();
        }

        public void Dispose()
        {
            server.Dispose();
            provider.Dispose();
            broker?.Dispose();
            kvBroker?.Dispose();
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
