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
    public class ConcurrentTest: IFasterRecoveryTest
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
        long[] threadNumOps;

        public ConcurrentTest(int threadCount)
        {
            this.threadCount = threadCount;

            // Create FASTER index
            var log = FasterFactory.CreateLogDevice(DirectoryConfiguration.GetHybridLogFileName());
            fht = FasterFactory.Create
                <AdId, NumClicks, Input, Output, Empty, Functions, ICustomFasterKv>
                (keySpace, log);
            numActiveThreads = 0;

            inputArrays = new BlockingCollection<Input[]>();
            threadNumOps = new long[threadCount];
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

            Test();
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
            var random = new Random(threadId + 1);
            threadNumOps[threadId] = (numOps / 2) + random.Next() % (numOps / 4);
            
            fixed (Input* input = inputArray)
            {
                for (long i = 0; i < threadNumOps[threadId]; i++)
                {
                    fht.RMW(&((input + i)->adId), input + i, &context, i);

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

        public unsafe void Test()
        {

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

            // Compute expected array
            long[] expected = new long[numUniqueKeys];
            for(long j = 0; j < threadCount; j++)
            {
                var sno = threadNumOps[j];
                for (long i = 0; i < sno; i++)
                {
                    var id = i % numUniqueKeys;
                    expected[id]++;
                }
            }


            // Assert if expected is same as found
            var counts = new Dictionary<long, long>();
            var sum = 0L;
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
            Console.WriteLine("Test successful");
        }

        public void Continue()
        {
            throw new NotImplementedException();
        }

        public void RecoverAndTest(Guid indexToken, Guid hybridLogToken)
        {
            throw new NotImplementedException();
        }
    }
}
