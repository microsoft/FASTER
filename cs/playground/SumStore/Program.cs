// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SumStore
{
    public class Program
    {
        static void Main(string[] args)
        {
            if (args.Length == 0)
            {
                Console.WriteLine("Usage: SumStore.exe [recovery #threads|test #threads] [populate|recover|continue] [() | (single_guid) | (index_guid hlog_guid)]");
                return;
            }
            
            int nextArg = 0;
            var test = default(IFasterRecoveryTest);
            var type = args[nextArg++];
            if (type == "recovery")
            {
                int threadCount = int.Parse(args[nextArg++]);
                test = new RecoveryTest(threadCount);
            }
            else if (type == "test")
            {
                int threadCount = int.Parse(args[nextArg++]);
                test = new ConcurrentTest(threadCount);
                test.Populate();
                return;
            }
            else
            {
                Debug.Assert(false);
            }

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
                throw new InvalidOperationException();
            }
        }
    }
}
