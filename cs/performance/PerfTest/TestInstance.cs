// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using Performance.Common;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace FASTER.PerfTest
{
    internal class TestInstance<TKey>
        where TKey : IKey, new()
    {
        private readonly TestRun testRun;

        private bool Verbose => keyManager.Verbose;
        readonly KeyManager<TKey> keyManager;
        private readonly IFasterEqualityComparer<TKey> keyComparer;

        private readonly long verboseInterval;

        long NextChunkStart = 0;

        // The TKey information is specified on the constructor; the TValue and other information on the Run() call.
        internal TestInstance(TestRun testRun, KeyManager<TKey> keyManager, IFasterEqualityComparer<TKey> keyComparer)
        {
            this.testRun = testRun;
            this.keyManager = keyManager;
            this.keyComparer = keyComparer;
            verboseInterval = 1L << (testRun.TestResult.Inputs.HashSizeShift - 1);
        }

        internal bool Run<TValue, TOutput, TFunctions>(SerializerSettings<TKey, TValue> serializerSettings, VariableLengthStructSettings<TKey, TValue> varLenSettings,
                                                       IThreadValueRef<TValue, TOutput> threadValueRef)
            where TValue: new()
            where TOutput: new()
            where TFunctions: IFunctions<TKey, TValue, Input, TOutput, Empty>, new()
        {
            this.keyManager.CreateKeys(testRun.TestResult);

            for (testRun.currentIter = 0; testRun.currentIter < testRun.TestResult.Inputs.IterationCount; ++testRun.currentIter)
            {
                const int pauseMs = 1000;
                if (Verbose)
                {
                    var workingSetMB = (ulong)Process.GetCurrentProcess().WorkingSet64 / 1048576;
                    Console.Write($"GC.Collect and pausing for {pauseMs}ms before starting iteration {testRun.currentIter}." +
                                  $" Working set: before {workingSetMB:N0}MB, ");
                }
                GC.Collect();
                if (Verbose)
                {
                    var workingSetMB = (ulong)Process.GetCurrentProcess().WorkingSet64 / 1048576;
                    Console.WriteLine($"after {workingSetMB:N0}MB");
                }
                Thread.Sleep(pauseMs);

                var fht = new FHT<TKey, TValue, TOutput, TFunctions>(
                    false, testRun.TestResult.Inputs.HashSizeShift, testRun.TestResult.Inputs.UseObjectKey || testRun.TestResult.Inputs.UseObjectValue, 
                    testRun.TestResult.Inputs.UseReadCache, serializerSettings, varLenSettings, this.keyComparer);
                RunIteration(fht, testRun, threadValueRef);
            }
            this.keyManager.Dispose();
            return true;
        }
        
        private bool RunIteration<TValue, TOutput, TFunctions>(
                    FHT<TKey, TValue, TOutput, TFunctions> fht, TestRun testRun,
                    IThreadValueRef<TValue, TOutput> threadValueRef)
            where TValue : new()
            where TOutput : new()
            where TFunctions : IFunctions<TKey, TValue, Input, TOutput, Empty>, new()
        {
            Initialize(fht, testRun, threadValueRef);
            FlushLog(fht, testRun);
            RunOperations(fht, testRun, threadValueRef);
            fht.Close();
            return true;
        }

        private void Initialize<TValue, TOutput, TFunctions>(
                FHT<TKey, TValue, TOutput, TFunctions> fht, TestRun testRun,
                IThreadValueRef<TValue, TOutput> threadValueRef)
            where TValue : new()
            where TOutput : new()
            where TFunctions : IFunctions<TKey, TValue, Input, TOutput, Empty>, new()
        {
            if (Verbose)
                Console.WriteLine($"Writing initial key values from 0 to {testRun.TestResult.Inputs.InitKeyCount} to FASTER");

            var sw = new Stopwatch();
            sw.Start();

            // Reset the global chunk tracker.
            NextChunkStart = 0;

            Globals.IsInitialInsertPhase = true;
            var tasks = Enumerable.Range(0, testRun.TestResult.Inputs.ThreadCount)
                                  .Select(threadIdx => Task.Run(() => Initialize(fht, threadIdx, testRun, threadValueRef)));
            Task.WaitAll(tasks.ToArray());
            Globals.IsInitialInsertPhase = false;

            sw.Stop();

            testRun.InitializeMs = (ulong)sw.ElapsedMilliseconds;
            var numSec = sw.ElapsedMilliseconds / 1000.0;
            var workingSetMB = (ulong)Process.GetCurrentProcess().WorkingSet64 / 1048576;
            Console.WriteLine($"Initialization: Time to insert {testRun.TestResult.Inputs.InitKeyCount:N0} initial key values:" +
                              $" {numSec:N3} sec ({testRun.TestResult.Inputs.InitKeyCount / numSec:N2} inserts/sec;" +
                              $" {testRun.TestResult.Inputs.InitKeyCount / (numSec * testRun.TestResult.Inputs.ThreadCount):N2} thread/sec);" +
                              $" working set {workingSetMB:N0}MB");
        }

        private void Initialize<TValue, TOutput, TFunctions>(
                FHT<TKey, TValue, TOutput, TFunctions> fht, int threadIndex, TestRun testRun,
                IThreadValueRef<TValue, TOutput> threadValueRef)
            where TValue : new()
            where TOutput : new()
            where TFunctions : IFunctions<TKey, TValue, Input, TOutput, Empty>, new()
        {
            // Each thread needs to set NUMA and create a FASTER session
            Numa.AffinitizeThread(testRun.TestResult.Inputs.NumaMode, threadIndex);
            using var session = fht.Faster.NewSession(null, true);
            ref TValue value = ref threadValueRef.GetRef(threadIndex);

            // We just do one iteration through the KeyCount to load the initial keys. If there are
            // multiple threads, each thread does (KeyCount / #threads) Inserts (on average).
            for (long chunkStart = Interlocked.Add(ref NextChunkStart, Globals.ChunkSize) - Globals.ChunkSize;
                chunkStart < testRun.TestResult.Inputs.InitKeyCount;
                chunkStart = Interlocked.Add(ref NextChunkStart, Globals.ChunkSize) - Globals.ChunkSize)
            {
                var chunkEnd = chunkStart + Globals.ChunkSize;
                for (var ii = chunkStart; ii < chunkEnd; ii++)
                {
                    if (ii % 256 == 0 && ii > 0)
                    {
                        session.Refresh();
                        if (ii % 65536 == 0)
                        {
                            session.CompletePending(false);
                            if (Verbose && ii % verboseInterval == 0)
                            {
                                long workingSetMB = Process.GetCurrentProcess().WorkingSet64 / 1048576;
                                Console.WriteLine($"Insert: {ii}, {workingSetMB:N0}MB");
                            }
                        }
                    }
                    var key = keyManager.GetInitKey((int)ii);
                    threadValueRef.SetInitialValue(ref value, key.Value);
                    session.Upsert(ref key, ref value, Empty.Default, 1);
                }
            }
            session.CompletePending(true);
        }

        private void FlushLog<TValue, TOutput, TFunctions>(
                FHT<TKey, TValue, TOutput, TFunctions> fht, TestRun testRun)
            where TValue : new()
            where TOutput : new()
            where TFunctions : IFunctions<TKey, TValue, Input, TOutput, Empty>, new()
        {
            if (Verbose)
                Console.WriteLine("Flushing log");
            switch (testRun.TestResult.Inputs.LogMode)
            {
                case LogMode.None:
                    break;
                case LogMode.Flush:
                    fht.Faster.Log.Flush(true);
                    break;
                case LogMode.FlushAndEvict:
                    fht.Faster.Log.FlushAndEvict(true);
                    break;
                case LogMode.DisposeFromMemory:
                    fht.Faster.Log.DisposeFromMemory();
                    break;
                default:
                    Console.WriteLine($"Missing LogMode case: {testRun.TestResult.Inputs.LogMode}");
                    return;
            }
        }

        private void RunOperations<TValue, TOutput, TFunctions>(
                FHT<TKey, TValue, TOutput, TFunctions> fht, TestRun testRun,
                IThreadValueRef<TValue, TOutput> threadValueRef)
            where TValue : new()
            where TOutput : new()
            where TFunctions : IFunctions<TKey, TValue, Input, TOutput, Empty>, new()
        {
            IEnumerable<(Operations, string, int)> prepareOps()
            {
                if (testRun.TestResult.Inputs.MixOperations)
                {
                    IEnumerable<string> getMixedOpNames()
                    {
                        if (testRun.TestResult.Inputs.UpsertCount > 0)
                            yield return "Upsert";
                        if (testRun.TestResult.Inputs.ReadCount > 0)
                            yield return "Read";
                        if (testRun.TestResult.Inputs.RMWCount > 0)
                            yield return "RMW";
                    }
                    yield return (Operations.Mixed, "mixed " + string.Join(", ", getMixedOpNames()), testRun.TestResult.Inputs.TotalOperationCount);
                    yield break;
                }
                if (testRun.TestResult.Inputs.UpsertCount > 0)
                    yield return (Operations.Upsert, "Upsert", testRun.TestResult.Inputs.UpsertCount);
                if (testRun.TestResult.Inputs.ReadCount > 0)
                    yield return (Operations.Read, "Read", testRun.TestResult.Inputs.ReadCount);
                if (testRun.TestResult.Inputs.RMWCount > 0)
                    yield return (Operations.RMW, "RMW", testRun.TestResult.Inputs.RMWCount);
            }

            var ops = prepareOps();

            var sw = new Stopwatch();

            // Reset the global chunk tracker here (outside the loop).
            NextChunkStart = 0;

            foreach (var (op, opName, opCount) in ops)
            {
                long startTailAddress = fht.LogTailAddress;

                sw.Restart();

                // Each thread does the full count
                var tasks = Enumerable.Range(0, testRun.TestResult.Inputs.ThreadCount)
                                      .Select(threadIdx => Task.Run(() => RunOperations(fht, op, opCount, threadIdx, testRun, threadValueRef)));
                Task.WaitAll(tasks.ToArray());

                sw.Stop();

                var numSec = sw.ElapsedMilliseconds / 1000.0;

                // Total Ops/Second is always reported 
                testRun.TotalOpsMs += (ulong)sw.ElapsedMilliseconds;

                switch (op)
                {
                    case Operations.Mixed: break;
                    case Operations.Upsert: testRun.UpsertMs = (ulong)sw.ElapsedMilliseconds; break;
                    case Operations.Read: testRun.ReadMs = (ulong)sw.ElapsedMilliseconds; break;
                    case Operations.RMW: testRun.RMWMs = (ulong)sw.ElapsedMilliseconds; break;
                    default:
                        throw new InvalidOperationException($"Unexpected Operations value: {op}");
                }

                var suffix = op == Operations.Mixed ? "" : "s";
                Console.WriteLine($"Iteration {testRun.currentIter}: Time for {opCount:N0} {opName} operations per thread ({opCount * testRun.TestResult.Inputs.ThreadCount:N0} total):" +
                                  $" {numSec:N3} sec ({opCount / numSec:N2} {op}{suffix}/sec)");
                var endTailAddress = fht.LogTailAddress;
                if (endTailAddress != startTailAddress)
                {
                    var isExpected = testRun.TestResult.Inputs.LogMode != LogMode.None
                        ? $"expected due to"
                        : $"*** UNEXPECTED *** with";
                    Console.WriteLine($"Log growth: {endTailAddress - startTailAddress}; {isExpected} {nameof(LogMode)}.{testRun.TestResult.Inputs.LogMode}");
                }
            }
        }

        private void RunOperations<TValue, TOutput, TFunctions>(
                FHT<TKey, TValue, TOutput, TFunctions> fht, Operations op, long opCount,
                int threadIndex, TestRun testRun, IThreadValueRef<TValue, TOutput> threadValueRef)
            where TValue : new()
            where TOutput : new()
            where TFunctions : IFunctions<TKey, TValue, Input, TOutput, Empty>, new()
        {
            // Each thread needs to set NUMA and create a FASTER session
            Numa.AffinitizeThread(testRun.TestResult.Inputs.NumaMode, threadIndex);
            using var session = fht.Faster.NewSession(null, true);

            if (Verbose)
                Console.WriteLine($"Running Operation {op} count {opCount:N0} for threadId {threadIndex}");

            var rng = new RandomGenerator((uint)threadIndex);
            var totalOpCount = testRun.TestResult.Inputs.TotalOperationCount;
            var upsertThreshold = testRun.TestResult.Inputs.UpsertCount;
            var readThreshold = upsertThreshold + testRun.TestResult.Inputs.ReadCount;
            var rmwThreshold = readThreshold + testRun.TestResult.Inputs.RMWCount;

            ref TValue value = ref threadValueRef.GetRef(threadIndex);
            var input = default(Input);
            var output = threadValueRef.GetOutput(threadIndex);

            long currentCount = 0;

            // We multiply the number of operations by the number of threads, so we will wrap around
            // the end of the operations keys if we get there
            for (long chunkStart = Interlocked.Add(ref NextChunkStart, Globals.ChunkSize) - Globals.ChunkSize;
                currentCount < opCount;
                chunkStart = Interlocked.Add(ref NextChunkStart, Globals.ChunkSize) - Globals.ChunkSize)
            {
                chunkStart %= testRun.TestResult.Inputs.OperationKeyCount;
                currentCount += Globals.ChunkSize;
                var chunkEnd = chunkStart + Globals.ChunkSize;
                for (var ii = chunkStart; ii < chunkEnd; ii++)
                {
                    var thisOp = op;
                    if (thisOp == Operations.Mixed)
                    {
                        var rand = rng.Generate((uint)totalOpCount);
                        if (rand < upsertThreshold)
                            thisOp = Operations.Upsert;
                        else if (rand <= readThreshold)
                            thisOp = Operations.Read;
                        else if (rand <= rmwThreshold)
                            thisOp = Operations.RMW;
                        else
                            throw new InvalidOperationException($"rand {rand} out of threshold ranges: u {upsertThreshold} r {readThreshold} m {rmwThreshold}");
                    }

                    if (ii % 256 == 0 && ii > 0)
                    {
                        session.Refresh();
                        if (ii % 65536 == 0)
                        {
                            session.CompletePending(false);
                            if (Verbose && ii % verboseInterval == 0)
                            {
                                Console.WriteLine($"{thisOp}: {ii}");
                            }
                        }
                    }

                    Status status = Status.OK;
                    var key = keyManager.GetOpKey((int)ii);
                    switch (thisOp)
                    {
                        case Operations.Upsert:
                            threadValueRef.SetUpsertValue(ref value, key.Value, ii & 7);
                            status = session.Upsert(ref key, ref value, Empty.Default, 1);
                            break;
                        case Operations.Read:
                            status = session.Read(ref key, ref input, ref output, Empty.Default, 0);
                            break;
                        case Operations.RMW:
                            input.value = (int)(ii & 7);
                            status = session.RMW(ref key, ref input, Empty.Default, 0);
                            break;
                    }

                    if (status != Status.OK && status != Status.PENDING)
                        throw new ApplicationException($"Error: Unexpected status in {nameof(RunOperations)} {thisOp}; key[{ii}] = {key.Value}: {status}");
                }
            }
            session.CompletePending(true);
        }
    }
}