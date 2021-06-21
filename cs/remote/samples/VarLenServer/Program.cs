// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Threading;
using CommandLine;
using ServerOptions;
using FASTER.core;
using FASTER.server;
using FASTER.common;

namespace VarLenServer
{
    /// <summary>
    /// Server for variable-length keys and values.
    /// </summary>
    class Program
    {
        static void Main(string[] args)
        {
            VarLenServer(args);
        }


        static void VarLenServer(string[] args)
        {
            Console.WriteLine("FASTER variable-length KV server");

            ParserResult<Options> result = Parser.Default.ParseArguments<Options>(args);
            if (result.Tag == ParserResultType.NotParsed) return;
            var opts = result.MapResult(o => o, xs => new Options());

            opts.GetSettings(out var logSettings, out var checkpointSettings, out var indexSize);

            // Create a new instance of the FasterKV, customized for variable-length blittable data (represented by SpanByte)
            // With SpanByte, keys and values are stored inline in the FASTER log as [ 4 byte length | payload ]
            var store = new FasterKV<SpanByte, SpanByte>(indexSize, logSettings, checkpointSettings);
            if (opts.Recover) store.Recover();

            // This variable-length session provider can be used with compatible clients such as VarLenClient
            var provider = new FasterKVProvider<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, SpanByteFunctionsForServer<long>, SpanByteSerializer>(store, wp => new SpanByteFunctionsForServer<long>(wp), new SpanByteSerializer());

            // Create server
            var server = new FasterServer(opts.Address, opts.Port);

            // Register provider as backend provider for WireFormat.DefaultFixedLenKV
            // You can register multiple providers with the same server, with different wire protocol specifications
            server.Register(WireFormat.DefaultVarLenKV, provider);

            // Start server
            server.Start();
            Console.WriteLine("Started server");

            Thread.Sleep(Timeout.Infinite);
        }
    }
}
