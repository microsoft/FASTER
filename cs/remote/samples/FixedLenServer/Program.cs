// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Threading;
using ServerOptions;
using CommandLine;
using FASTER.core;
using FASTER.server;
using FASTER.common;

namespace FixedLenServer
{
    /// <summary>
    /// This sample creates a FASTER server for fixed-length (struct) keys and values
    /// Types are defined in Types.cs; they are 8-byte keys and values in the sample.
    /// A binary wire protocol is used.
    /// </summary>
    class Program
    {
        static void Main(string[] args)
        {
            FixedLenServer(args);
        }

        static void FixedLenServer(string[] args)
        {
            Console.WriteLine("FASTER fixed-length (binary) KV server");

            ParserResult<Options> result = Parser.Default.ParseArguments<Options>(args);
            if (result.Tag == ParserResultType.NotParsed) return;
            var opts = result.MapResult(o => o, xs => new Options());

            opts.GetSettings(out var logSettings, out var checkpointSettings, out var indexSize);

            // We use blittable structs Key and Value to construct a customized server for fixed-length types
            var store = new FasterKV<Key, Value>(indexSize, logSettings, checkpointSettings);
            if (opts.Recover) store.Recover();

            // We specify FixedLenSerializer as our in-built serializer for blittable (fixed length) types
            // This provider can be used with compatible clients such as FixedLenClient and FASTER.benchmark
            var provider = new FasterKVProvider<Key, Value, Input, Output, Functions, FixedLenSerializer<Key, Value, Input, Output>>(store, e => new Functions());

            // Create server
            var server = new FasterServer(opts.Address, opts.Port);

            // Register provider as backend provider for WireFormat.DefaultFixedLenKV
            server.Register(WireFormat.DefaultFixedLenKV, provider);

            // Start server
            server.Start();
            Console.WriteLine("Started server");

            Thread.Sleep(Timeout.Infinite);
        }
    }
}
