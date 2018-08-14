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
    public interface IFASTERRecoveryTest
    {
        void Populate();
        void Continue();
        void RecoverAndTest(Guid indexToken, Guid hybridLogToken);
    }
    public class Program
    {
        static unsafe void Main(string[] args)
        {
            int nextArg = 0;
            var test = default(IFASTERRecoveryTest);
            var type = args[nextArg++];
            if(type == "single")
            {
                test = new SingleThreadedRecoveryTest();
            }
            else if(type == "concurrent")
            {
                int threadCount = int.Parse(args[nextArg++]);
                test = new ConcurrentRecoveryTest(threadCount);
            }
            else if(type == "test")
            {
                int threadCount = int.Parse(args[nextArg++]);
                test = new ConcurrentTest(threadCount);
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
            else if(task == "recover")
            {
                Guid version = Guid.Parse(args[nextArg++]);
                test.RecoverAndTest(version, version);
            }
            else if(task == "continue")
            {
                test.Continue();
            }
            else
            {
                Debug.Assert(false);
            }
        }
    }
}
