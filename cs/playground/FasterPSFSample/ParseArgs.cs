// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;

namespace FasterPSFSample
{
    public partial class FasterPSFSample
    {
        private static bool useObjectValue;
        private static bool useMultiGroups;
        private static bool verbose;

        const string ObjValuesArg = "--objValues";
        const string MultiGroupArg = "--multiGroup";

        static bool ParseArgs(string[] argv)
        {
            static bool Usage(string message = null)
            {
                Console.WriteLine();
                Console.WriteLine($"Usage: Run one or more Predicate Subset Functions (PSFs), specifying whether to use object or blittable (primitive) values.");
                Console.WriteLine();
                Console.WriteLine($"    {ObjValuesArg}: Use objects instead of blittable Value; default is {useObjectValue}");
                Console.WriteLine($"    {MultiGroupArg}: Put each PSF in a separate group; default is {useMultiGroups}");
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
                if (string.Compare(arg, MultiGroupArg, ignoreCase: true) == 0)
                {
                    useMultiGroups = true;
                    continue;
                }
                if (string.Compare(arg, "--help", ignoreCase: true) == 0 || arg == "/?" || arg == "-?")
                    return Usage();
                if (string.Compare(arg, "-v", ignoreCase: true) == 0)
                {
                    verbose = true;
                    continue;
                }
                return Usage($"Unknown argument: {arg}");
            }
            return true;
        }
    }
}
