// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;

namespace SumStore
{
    public class Program
    {
        static void Main(string[] args)
        {
            if (args.Length == 0)
            {
                Console.WriteLine("Usage:");
                Console.WriteLine("Concurrency Test:\n  SumStore.exe concurrency_test #threads");
                Console.WriteLine("Recovery Test:\n  SumStore.exe recovery_test #threads populate");
                Console.WriteLine("  SumStore.exe recovery_test #threads continue");
                Console.WriteLine("  SumStore.exe recovery_test #threads recover");
                Console.WriteLine("  SumStore.exe recovery_test #threads recover single_guid");
                Console.WriteLine("  SumStore.exe recovery_test #threads recover index_guid hlog_guid");
                return;
            }
            
            int nextArg = 0;
            var type = args[nextArg++];
            int threadCount = int.Parse(args[nextArg++]);

            if (type == "concurrency_test")
            {
                var ctest = new ConcurrencyTest(threadCount);
                ctest.Populate();
                return;
            }

            if (type != "recovery_test" || args.Length < 3)
            {
                throw new Exception("Invalid test");
            }

            RecoveryTest test = new RecoveryTest(threadCount);

            var task = args[nextArg++];
            if (task == "populate")
            {
                test.Populate();
            }
            else if (task == "recover")
            {
                switch (args.Length - nextArg)
                {
                    case 0:
                        test.RecoverLatest();
                        break;
                    case 1:
                        var version = Guid.Parse(args[nextArg++]);
                        test.Recover(version, version);
                        break;
                    case 2:
                        test.Recover(Guid.Parse(args[nextArg++]), Guid.Parse(args[nextArg++]));
                        break;
                    default:
                        throw new Exception("Invalid input");
                }
            }
            else if (task == "continue")
            {
                test.Continue();
            }
            else
            {
                throw new Exception("Invalid test");
            }
        }
    }
}
