// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Threading;
using ServerOptions;
using CommandLine;
using FASTER.core;
using FASTER.server;

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

            var store = new FasterKV<Key, Value>(indexSize, logSettings, checkpointSettings);
            if (opts.Recover) store.Recover();

            var server = new FasterKVServer<Key, Value, Input, Output, Functions, FixedLenSerializer<Key, Value, Input, Output>>
                (store, e => new Functions(), opts.Address, opts.Port);
            server.Start();
            Thread.Sleep(Timeout.Infinite);
        }
    }
}
