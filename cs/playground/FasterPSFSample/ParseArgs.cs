// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;

namespace FasterPSFSample
{
    public partial class FasterPSFSample
    {
        private static bool useObjectValue;

        const string ObjValuesArg = "--objValues";

        static bool ParseArgs(string[] argv)
        {
            static bool Usage(string message = null)
            {
                Console.WriteLine();
                Console.WriteLine($"Usage: Run one or more Predicate Subset Functions (PSFs), specifying whether to use object or blittable (primitive) values.");
                Console.WriteLine();
                Console.WriteLine($"    {ObjValuesArg}: Use objects instead of blittable Value; default is {useObjectValue}");
                Console.WriteLine();
                if (!string.IsNullOrEmpty(message))
                {
                    Console.WriteLine("====== Invalid Argument(s) ======");
                    Console.WriteLine(message);
                    Console.WriteLine();
                }
                Console.WriteLine();
                return false;
            }

            for (var ii = 0; ii < argv.Length; ++ii)
            {
                var arg = argv[ii];
                if (string.Compare(arg, ObjValuesArg, ignoreCase: true) == 0)
                {
                    useObjectValue = true;
                    continue;
                }
                if (string.Compare(arg, "--help", ignoreCase: true) == 0 || arg == "/?" || arg == "-?")
                {
                    return Usage();
                }
                return Usage($"Unknown argument: {arg}");
            }
            return true;
        }
    }
}
