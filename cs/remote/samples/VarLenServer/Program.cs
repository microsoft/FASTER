// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Threading;
using CommandLine;
using FasterServerOptions;
using FASTER.server;
using System.Diagnostics;
using Microsoft.Extensions.Logging;

namespace FasterVarLenServer
{
    /// <summary>
    /// Sample server for variable-length keys and values.
    /// </summary>
    class Program
    {
        static void Main(string[] args)
        {
            Environment.SetEnvironmentVariable("DOTNET_SYSTEM_NET_SOCKETS_INLINE_COMPLETIONS", "1");
            Trace.Listeners.Add(new ConsoleTraceListener());

            Console.WriteLine("FASTER variable-length KV server");

            ParserResult<Options> result = Parser.Default.ParseArguments<Options>(args);
            if (result.Tag == ParserResultType.NotParsed) return;
            var opts = result.MapResult(o => o, xs => new Options());

            ILoggerFactory loggerFactory = LoggerFactory.Create(builder =>
            {
                builder.AddSimpleConsole(options =>
                {
                    options.SingleLine = true;
                    options.TimestampFormat = "hh::mm::ss ";
                });
                builder.SetMinimumLevel(LogLevel.Error);
            });

            using var server = new VarLenServer(opts.GetServerOptions(loggerFactory.CreateLogger("")));
            server.Start();
            Console.WriteLine("Started server");

            Thread.Sleep(Timeout.Infinite);
        }
    }
}
