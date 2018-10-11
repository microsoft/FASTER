// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace SumStore
{
    public class ConcurrentRecoveryTest : IFasterRecoveryTest
    {
        const long numUniqueKeys = (1 << 22);
        const long keySpace = (1L << 14);
        const long numOps = (1L << 25);
        const long refreshInterval = (1 << 8);
        const long completePendingInterval = (1 << 12);
        const long checkpointInterval = (1 << 22);
        int threadCount;
        int numActiveThreads;
        ICustomFasterKv fht;
        BlockingCollection<Input[]> inputArrays;
        List<Guid> tokens;
        public ConcurrentRecoveryTest(int threadCount)
        {
            this.threadCount = threadCount;
            tokens = new List<Guid>();

            var log = FasterFactory.CreateLogDevice("logs\\hlog");

            // Create FASTER index
            fht = FasterFactory.Create
                <AdId, NumClicks, Input, Output, Empty, Functions, ICustomFasterKv>
                (keySpace, log, checkpointDir: "logs");
            numActiveThreads = 0;

            inputArrays = new BlockingCollection<Input[]>();

            Prepare();
        }

        public unsafe void Prepare()
        {
            Console.WriteLine("Creating Input Arrays");

            Thread[] workers = new Thread[threadCount];
            for (int idx = 0; idx < threadCount; ++idx)
            {
                int x = idx;
                workers[idx] = new Thread(() => CreateInputArrays(x));
            }


            // Start threads.
            foreach (Thread worker in workers)
            {
                worker.Start();
            }

            // Wait until all are completed
            foreach (Thread worker in workers)
            {
                worker.Join();
            }
        }

        private unsafe void CreateInputArrays(int threadId)
        {
            var inputArray = new Input[numOps];
            for (int i = 0; i < numOps; i++)
            {
                inputArray[i].adId.adId = i % numUniqueKeys;
                inputArray[i].numClicks.numClicks = 1;
            }

            inputArrays.Add(inputArray);
        }


        public unsafe void Populate()
        {
            Thread[] workers = new Thread[threadCount];
            for (int idx = 0; idx < threadCount; ++idx)
            {
                int x = idx;
                workers[idx] = new Thread(() => PopulateWorker(x));
            }

            Console.WriteLine("Ready to Populate, Press [Enter]");
            Console.ReadLine(); 

            // Start threads.
            foreach (Thread worker in workers)
            {
                worker.Start();
            }

            // Wait until all are completed
            foreach (Thread worker in workers)
            {
                worker.Join();
            }


            foreach (var token in tokens)
            {
                Console.WriteLine(token);
            }
        }

        private unsafe void PopulateWorker(int threadId)
        {
            Native32.AffinitizeThreadRoundRobin((uint)threadId);

            Empty context;

            var success = inputArrays.TryTake(out Input[] inputArray);
            if(!success)
            {
                Console.WriteLine("No input array for {0}", threadId);
                return;
            }

            // Register thread with the store
            fht.StartSession();

            Interlocked.Increment(ref numActiveThreads);

            // Process the batch of input data
            fixed (Input* input = inputArray)
            {
                for (long i = 0; i < numOps; i++)
                {
                    fht.RMW(&((input + i)->adId), input + i, &context, i);


                    if ((i+1) % checkpointInterval == 0 && numActiveThreads == threadCount)
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

            //Interlocked.Decrement(ref numActiveThreads);

            Console.WriteLine("Populate successful on thread {0}", threadId);
        }


        public unsafe void Continue()
        {
            Console.WriteLine("Ready to Run. version to recover? [Enter]");
            var line = Console.ReadLine();
            Guid token = Guid.Parse(line);

            Console.WriteLine("Recovering version {0}", token);
            fht.Recover(token, token);
            Console.WriteLine("Recovery Done!");

            var checkpointInfo = default(HybridLogRecoveryInfo);
            checkpointInfo.Recover(token);

            threadCount = checkpointInfo.numThreads;

            Console.WriteLine("Continuing");
            Thread[] workers = new Thread[threadCount];
            int idx = 0;
            for (int i = 0; i < threadCount; i++)
            {
                int x = idx++;
                Guid guid = checkpointInfo.guids[i];
                workers[x] = new Thread(() => ContinueWorker(x, guid));
            }

            // Start threads.
            foreach (Thread worker in workers)
            {
                worker.Start();
            }

            // Wait until all are completed
            foreach (Thread worker in workers)
            {
                worker.Join();
            }
        }

        private unsafe void ContinueWorker(int threadId, Guid guid)
        {
            Native32.AffinitizeThreadRoundRobin((uint)threadId);

            Empty context;

            var success = inputArrays.TryTake(out Input[] inputArray);
            if (!success)
            {
                Console.WriteLine("No input array for {0}", threadId);
                return;
            }

            // Register thread with the store
            var startNum = fht.ContinueSession(guid);

            Interlocked.Increment(ref numActiveThreads);

            Console.WriteLine("Thread {0} starting from {1}", threadId, startNum + 1);

            // Prpcess the batch of input data
            fixed (Input* input = inputArray)
            {
                for (long i = startNum + 1; i < numOps; i++)
                {
                    fht.RMW(&((input + i)->adId), input + i, &context, i);

                    if ((i+1) % checkpointInterval == 0 && numActiveThreads == threadCount)
                    {
                        if (fht.TakeFullCheckpoint(out Guid token))
                        {
                            Console.WriteLine("Calling TakeCheckpoint");
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

            //Interlocked.Decrement(ref numActiveThreads);

            Console.WriteLine("Populate successful on thread {0}", threadId);
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
            var checkpointInfo = default(HybridLogRecoveryInfo);
            checkpointInfo.Recover(hybridLogToken);

            // Compute expected array
            long[] expected = new long[numUniqueKeys];
            foreach(var guid in checkpointInfo.continueTokens.Keys)
            {
                var sno = checkpointInfo.continueTokens[guid];
                for (long i = 0; i <= sno; i++)
                {
                    var id = i % numUniqueKeys;
                    expected[id]++;
                }
            }

            int numCompleted = threadCount - checkpointInfo.continueTokens.Count;
            for(int t = 0; t < numCompleted; t++)
            {
                var sno = numOps;
                for (long i = 0; i < sno; i++)
                {
                    var id = i % numUniqueKeys;
                    expected[id]++;
                }
            }


            // Assert if expected is same as found
            var counts = new Dictionary<long, long>();
            var sum = 0L;
            bool error = false;
            for (long i = 0; i < numUniqueKeys; i++)
            {
                if(expected[i] != inputArray[i].numClicks.numClicks)
                {
                    long diff = inputArray[i].numClicks.numClicks - expected[i];
                    if (!counts.ContainsKey(diff))
                    {
                        counts.Add(diff, 0);
                    }
                    counts[diff] = counts[diff] + 1;
                    sum += diff;
                    Console.WriteLine("Debug error for AdId {0}: Expected ({1}), Found({2})", inputArray[i].adId.adId, expected[i], inputArray[i].numClicks.numClicks);
                    error = true;
                }
            }

            if(sum > 0)
            {
                foreach (var key in counts.Keys)
                {
                    Console.WriteLine("{0}: {1}", key, counts[key]);
                }
                Console.WriteLine("Sum : {0:X}, (1 << {1})", sum, Math.Log(sum, 2));
            }
            if (error)
                Console.WriteLine("Test failed");
            else 
                Console.WriteLine("Test successful");
        }
    }
}
