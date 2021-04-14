// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Threading;
using CommandLine;
using ServerOptions;
using FASTER.core;
using FASTER.server;

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

            var store = new FasterKV<SpanByte, SpanByte>(indexSize, logSettings, checkpointSettings);
            if (opts.Recover) store.Recover();

            var server = new FasterKVServer<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, SpanByteFunctionsForServer<long>, SpanByteSerializer>
                (store, wp => new SpanByteFunctionsForServer<long>(wp), opts.Address, opts.Port, new SpanByteSerializer(), default);
            server.Start();
            Console.WriteLine("Started server");
            Thread.Sleep(Timeout.Infinite);
        }
    }
}
