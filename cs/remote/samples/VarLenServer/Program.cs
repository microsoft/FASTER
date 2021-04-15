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

            // Create a new instance of the FasterKV, customized for variable-length blittable data (represented by SpanByte)
            // With SpanByte, keys and values are stored inline in the FASTER log as [ 4 byte length | payload ]
            var store = new FasterKV<SpanByte, SpanByte>(indexSize, logSettings, checkpointSettings);
            if (opts.Recover) store.Recover();

            // Create a new server based on above store. You specify additional details such as the serializer (to read and write
            // from and to the wire) and functions (to communicate with FASTER via IFunctions)
            var server = new FasterKVServer<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, SpanByteFunctionsForServer<long>, SpanByteSerializer>
                (store, wp => new SpanByteFunctionsForServer<long>(wp), opts.Address, opts.Port, new SpanByteSerializer(), default);
            server.Start();
            Console.WriteLine("Started server");
            Thread.Sleep(Timeout.Infinite);
        }
    }
}
