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

            SubscribeKVBroker<Key, Value, Input, IKeyInputSerializer<Key, Input>> kvBroker = null;
            SubscribeBroker<Key, Value, IKeySerializer<Key>> broker = null;

            if (opts.EnablePubSub)
            {
                // Create a broker for pub-sub of key-value pairs in remote FASTER instance
                kvBroker = new SubscribeKVBroker<Key, Value, Input, IKeyInputSerializer<Key, Input>>(new FixedLenKeyInputSerializer<Key, Input>(), opts.LogDir, true);
                // Create a broker for pub-sub of key-value pairs
                broker = new SubscribeBroker<Key, Value, IKeySerializer<Key>>(new FixedLenKeySerializer<Key>(), null, true);
            }

            // This fixed-length session provider can be used with compatible clients such as FixedLenClient and FASTER.benchmark
            // Uses FixedLenSerializer as our in-built serializer for blittable (fixed length) types
            var provider = new FasterKVProvider<Key, Value, Input, Output, Functions, FixedLenSerializer<Key, Value, Input, Output>>(store, e => new Functions(), kvBroker, broker);

            // Create server
            var server = new FasterServer(opts.Address, opts.Port);

            // Register session provider for WireFormat.DefaultFixedLenKV
            // You can register multiple session providers with the same server, with different wire protocol specifications
            server.Register(WireFormat.DefaultFixedLenKV, provider);

            // Start server
            server.Start();
            Console.WriteLine("Started server");

            Thread.Sleep(Timeout.Infinite);
        }
    }
}
