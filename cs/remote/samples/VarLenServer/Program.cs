// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Threading;
using FASTER.core;
using FASTER.server;
using FASTER.common;

namespace YcsbServer
{
    class Program
    {
        const int kMaxKey = 1 << 28;

        static void Main(string[] args)
        {
            string ip = "127.0.0.1";
            int port = 3278;
            string drive = "D";
            var logSettings = new LogSettings { PreallocateLog = false };

            if (args.Length > 0 && args[0] != "-")
                ip = args[0];
            if (args.Length > 1 && args[1] != "-")
                port = int.Parse(args[1]);
            if (args.Length > 2 && args[2] != "-")
                drive = args[2];
            if (args.Length > 3 && args[3] == "lowmemory")
            {
                logSettings.PageSizeBits = 12; // 4KB pages
                logSettings.MemorySizeBits = 14; // 16KB total log memory
            }

            var path = drive + ":\\data\\FasterYcsbBenchmark\\";
            var device = Devices.CreateLogDevice(path + "hlog", preallocateFile: false);
            logSettings.LogDevice = device;

            var store = new FasterKV<SpanByte, SpanByte>(kMaxKey / 2, logSettings, new CheckpointSettings { CheckPointType = CheckpointType.FoldOver, CheckpointDir = path });

            var server = new FasterKVServer<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, SpanByteFunctionsForServer<long>, SpanByteParameterSerializer>
                (store, new SpanByteFunctionsForServer<long>(), ip, port, new SpanByteParameterSerializer());
            server.Start();
            Console.WriteLine("Started server");
            Thread.Sleep(Timeout.Infinite);
        }
    }
}
