// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Security.Cryptography;
using System.Threading;
using FASTER.core;

namespace EpvsSample
{
    internal class EpvsBench
    {
        internal SemaphoreSlim testedLatch = null!;
        internal EpochProtectedVersionScheme tested = null!;
        internal byte[] hashBytes = null!;

        internal class Worker
        {
            readonly byte[] scratchPad;

            readonly HashAlgorithm hasher;

            // private long scratchPad;
            readonly EpvsBench parent;
            readonly List<int> versionChangeIndexes;
            readonly int numOps, versionChangeDelay, numaStyle, threadId;

            readonly byte syncMode;

            internal Worker(EpvsBench parent, Options options, Random random, int threadId)
            {
                hasher = SHA256.Create();
                scratchPad = new byte[hasher.HashSize / 8];
                this.parent = parent;
                versionChangeIndexes = new List<int>();
                numOps = options.NumOps;
                versionChangeDelay = options.VersionChangeDelay;
                numaStyle = options.NumaStyle;
                this.threadId = threadId;

                for (var i = 0; i < numOps; i++)
                {
                    if (random.NextDouble() < options.VersionChangeProbability)
                        versionChangeIndexes.Add(i);
                }

                switch (options.SynchronizationMode)
                {
                    case "epvs":
                        syncMode = 1;
                        break;
                    case "epvs-refresh":
                        syncMode = 2;
                        break;
                    case "latch":
                        syncMode = 3;
                        break;
                }
                
            }

            private void DoWork(int numUnits)
            {
                for (var i = 0; i < numUnits; i++)
                    hasher.TryComputeHash(parent.hashBytes, scratchPad, out _);
                // scratchPad++;
            }

            internal void RunOneThread()
            {                            
                if (numaStyle == 0)
                    Native32.AffinitizeThreadRoundRobin((uint) threadId);
                else if (numaStyle == 1)
                    Native32.AffinitizeThreadShardedNuma((uint) threadId, 2); // assuming two NUMA sockets

                if (syncMode == 2)
                    parent.tested.Enter();
                
                var nextChangeIndex = 0;
                for (var i = 0; i < numOps; i++)
                {
                    switch (syncMode)
                    {
                        case 1:
                            if (nextChangeIndex < versionChangeIndexes.Count &&
                                i == versionChangeIndexes[nextChangeIndex])
                            {
                                parent.tested.TryAdvanceVersionWithCriticalSection((_, _) => DoWork(versionChangeDelay));
                                nextChangeIndex++;
                            }
                            else
                            {
                                parent.tested.Enter();
                                DoWork(1);
                                parent.tested.Leave();
                            }
                            break;
                        case 2:
                            if (nextChangeIndex < versionChangeIndexes.Count &&
                                i == versionChangeIndexes[nextChangeIndex])
                            {
                                while (parent.tested.TryAdvanceVersionWithCriticalSection((_, _) =>
                                {
                                    DoWork(versionChangeDelay);
                                }) == StateMachineExecutionStatus.RETRY)
                                    parent.tested.Refresh();
                                nextChangeIndex++;
                            }
                            else
                            {
                                parent.tested.Refresh();
                                DoWork(1);
                            }

                            break;
                        case 3:
                            parent.testedLatch.Wait();
                            if (nextChangeIndex < versionChangeIndexes.Count &&
                                i == versionChangeIndexes[nextChangeIndex])
                            {
                                DoWork(versionChangeDelay);
                                nextChangeIndex++;
                            }
                            else
                            {
                                DoWork(1);
                            }
                            parent.testedLatch.Release();
                            break;
                        default:
                            throw new NotImplementedException();
                    }
                }
                
                if (syncMode == 2)
                    parent.tested.Leave();
            }
        }


        internal void RunExperiment(Options options)
        {
            hashBytes = new byte[8];
            new Random().NextBytes(hashBytes);
            tested = new EpochProtectedVersionScheme(new LightEpoch());
            testedLatch = new SemaphoreSlim(1, 1);

            var threads = new List<Thread>();
            var random = new Random();
            for (var i = 0; i < options.NumThreads; i++)
            {
                var worker = new Worker(this, options, random, i);
                var t = new Thread(() => worker.RunOneThread());
                threads.Add(t);
            }

            var sw = Stopwatch.StartNew();
            foreach (var t in threads)
                t.Start();
            foreach (var t in threads)
                t.Join();
            var timeMilli = sw.ElapsedMilliseconds;
            var throughput = options.NumOps * options.NumThreads * 1000.0 / timeMilli;
            Console.WriteLine(throughput);
            if (!options.OutputFile.Equals(""))
            {
                using var outputFile = new StreamWriter(options.OutputFile, true);
                outputFile.WriteLine(throughput);
            }
        }
    }
}