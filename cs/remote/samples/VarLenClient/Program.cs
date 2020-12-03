// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;

namespace VarLenClient
{
    class Program
    {
        static void Main(string[] args)
        {
            string ip = "127.0.0.1";
            int port = 3278;

            if (args.Length > 0 && args[0] != "-")
                ip = args[0];
            if (args.Length > 1 && args[1] != "-")
                port = int.Parse(args[1]);

            new MemorySamples().Run(ip, port);
            new CustomTypeSamples().Run(ip, port);
            Console.WriteLine("Success!");
        }
    }
}
