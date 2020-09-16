// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;

namespace SumStore
{
    public class ConcurrencyTest
    {
        const long numUniqueKeys = (1 << 22);
        const long keySpace = (1L << 14);
        const long numOps = (1L << 20);
        const long completePendingInterval = (1 << 12);
        readonly int threadCount;
        int numActiveThreads;
        readonly FasterKV<AdId, NumClicks> fht;
        readonly BlockingCollection<Input[]> inputArrays;
        readonly long[] threadNumOps;

        public ConcurrencyTest(int threadCount)
        {
            this.threadCount = threadCount;

            // Create FASTER index
            var log = Devices.CreateLogDevice("logs\\hlog");
            fht = new FasterKV<AdId, NumClicks>
                (keySpace, new LogSettings { LogDevice = log }, 
                new CheckpointSettings { CheckpointDir = "logs" });
            numActiveThreads = 0;

            inputArrays = new BlockingCollection<Input[]>();
            threadNumOps = new long[threadCount];
            Prepare();
        }

        public void Prepare()
        {
            Thread[] workers = new Thread[threadCount];
            for (int idx = 0; idx < threadCount; ++idx)
            {
                int x = idx;
                workers[idx] = new Thread(() => CreateInputArrays());
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

        private void CreateInputArrays()
        {
            var inputArray = new Input[numOps];
            for (int i = 0; i < numOps; i++)
            {
                inputArray[i].adId.adId = i % numUniqueKeys;
                inputArray[i].numClicks.numClicks = 1;
            }

            inputArrays.Add(inputArray);
        }


        public void Populate()
        {
            Thread[] workers = new Thread[threadCount];
            for (int idx = 0; idx < threadCount; ++idx)
            {
                int x = idx;
                workers[idx] = new Thread(() => PopulateWorker(x));
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

            Test();
        }

        private void PopulateWorker(int threadId)
        {
            Native32.AffinitizeThreadRoundRobin((uint)threadId);

            var success = inputArrays.TryTake(out Input[] inputArray);
            if(!success)
            {
                Console.WriteLine("No input array for {0}", threadId);
                return;
            }

            // Register thread with the store
            var session = fht.NewSession(new Functions());

            Interlocked.Increment(ref numActiveThreads);

            // Process the batch of input data
            threadNumOps[threadId] = numOps / 2;
            
            for (long i = 0; i < threadNumOps[threadId]; i++)
            {
                var status = session.RMW(ref inputArray[i].adId, ref inputArray[i], Empty.Default, i);

                if (status != Status.OK && status != Status.NOTFOUND)
                    throw new Exception();

                if (i % completePendingInterval == 0)
                {
                    session.CompletePending(false);
                }
            }

            // Make sure operations are completed
            session.CompletePending(true);

            // Deregister thread from FASTER
            session.Dispose();

            Console.WriteLine("Populate successful on thread {0}", threadId);
        }

        public void Test()
        {
            // Create array for reading
            var inputArray = new Input[numUniqueKeys];
            for (int i = 0; i < numUniqueKeys; i++)
            {
                inputArray[i].adId.adId = i;
                inputArray[i].numClicks.numClicks = 0;
            }

            // Register with thread
            var session = fht.NewSession(new Functions());

            // Issue read requests
            for (var i = 0; i < numUniqueKeys; i++)
            {
                Input input = default;
                Output output = default;
                var status = session.Read(ref inputArray[i].adId, ref input, ref output, Empty.Default, i);
                if (status == Status.PENDING)
                    throw new NotImplementedException();

                inputArray[i].numClicks = output.value;
            }

            // Complete all pending requests
            session.CompletePending(true);

            // Release
            session.Dispose();

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
    }
}
