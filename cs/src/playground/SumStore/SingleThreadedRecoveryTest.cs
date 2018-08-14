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
    public class SingleThreadedRecoveryTest : IFASTERRecoveryTest
    {
        const long numUniqueKeys = (1 << 23);
        const long keySpace = (1L << 15);
        const long numOps = (1L << 25);
        const long refreshInterval = (1 << 8);
        const long completePendingInterval = (1 << 12);
        const long checkpointInterval = (1 << 20);
        ICustomFaster fht;

        public SingleThreadedRecoveryTest()
        {
            // Create FASTER index
            var log = FASTERFactory.CreateLogDevice(DirectoryConfiguration.GetHybridLogFileName());
            fht = FASTERFactory.Create
                <AdId, NumClicks, Input, Output, Empty, Functions, ICustomFaster>
                (keySpace, log);
        }

        public void Continue()
        {
            throw new NotImplementedException();
        }

        public unsafe void Populate()
        {
            List<Guid> tokens = new List<Guid>();

            Empty context;

            // Prepare the dataset
            var inputArray = new Input[numOps];
            for (int i = 0; i < numOps; i++)
            {
                inputArray[i].adId.adId = i % numUniqueKeys;
                inputArray[i].numClicks.numClicks = 1;
            }

            // Register thread with FASTER
            fht.StartSession();

            // Prpcess the batch of input data
            fixed (Input* input = inputArray)
            {
                for (int i = 0; i < numOps; i++)
                {
                    fht.RMW(&((input + i)->adId), input + i, &context, i);

                    if (i % checkpointInterval == 0)
                    {
                        if(fht.TakeFullCheckpoint(out Guid token))
                        {
                            tokens.Add(token);
                        }
                    }

                    if (i % completePendingInterval == 0)
                    {
                        fht.CompletePending(false);
                    }
                    else if (i % refreshInterval == 0)
                    {
                        fht.Refresh();
                    }
                }
            }

            // Make sure operations are completed
            fht.CompletePending(true);

            // Deregister thread from FASTER
            fht.StopSession();

            Console.WriteLine("Populate successful");
            foreach(var token in tokens)
            {
                Console.WriteLine(token);
            }
            Console.ReadLine();
        }

        public unsafe void RecoverAndTest(Guid indexToken, Guid hybridLogToken)
        {
            // Recover
            fht.Recover(indexToken, hybridLogToken);

            // Create array for reading
            Empty context;
            var inputArray = new Input[numUniqueKeys];
            for (int i = 0; i < numUniqueKeys; i++)
            {
                inputArray[i].adId.adId = i;
                inputArray[i].numClicks.numClicks = 0;
            }

            // Register with thread
            fht.StartSession();

            // Issue read requests
            fixed (Input* input = inputArray)
            {
                for (var i = 0; i < numUniqueKeys; i++)
                {
                    fht.Read(&((input + i)->adId), null, (Output*)&((input + i)->numClicks), &context, i);
                }
            }

            // Complete all pending requests
            fht.CompletePending(true);

            // Release
            fht.StopSession();

            // Test outputs
            var recoveryInfo = default(HybridLogRecoveryInfo);
            recoveryInfo.Recover(hybridLogToken);

            int num_threads = recoveryInfo.numThreads;
            DirectoryInfo info = new DirectoryInfo(DirectoryConfiguration.GetHybridLogCheckpointFolder(hybridLogToken));
            List<ExecutionContext> cpr_points = new List<ExecutionContext>();
            foreach (var file in info.GetFiles())
            {
                if (file.Name != "info.dat" && file.Name != "snapshot.dat")
                {
                    using (var reader = new StreamReader(file.FullName))
                    {
                        var ctx = new ExecutionContext();
                        ctx.Load(reader);
                        cpr_points.Add(ctx);
                    }
                }
            }

            // Compute expected array
            long[] expected = new long[numUniqueKeys];
            long[] found = new long[numUniqueKeys];
            long sno = cpr_points.First().serialNum;
            for (long i = 0; i <= sno; i++)
            {
                var id = i % numUniqueKeys;
                expected[id]++;
            }

            // Assert if expected is same as found
            for (long i = 0; i < numUniqueKeys; i++)
            {
                if (expected[i] != inputArray[i].numClicks.numClicks)
                {
                    Console.WriteLine("Debug error for AdId {0}: Expected ({1}), Found({2})", inputArray[i].adId.adId, expected[i], inputArray[i].numClicks.numClicks);
                }
            }
            Console.WriteLine("Test successful");

            Console.ReadLine();
        }
    }
}
