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
    partial class PerfTest
    {
        static readonly TestResult defaultTestResult = new TestResult();
        static TestParameters testParams;
        static string testFilename;
        static string resultsFilename;
        static string compareFirstFilename, compareSecondFilename;
        static ResultComparisonMode comparisonMode = ResultComparisonMode.None;
        static readonly List<string> mergeResultsFilespecs = new List<string>();

        static bool verbose = false;
        static bool prompt = false;
        static long verboseInterval;

        static Key[] initKeys;
        static Key[] opKeys;
        static TestResult prevTestResult;

        static long NextChunkStart = 0;

        static void Main(string[] argv)
        {
            if (!ParseArgs(argv))
                return;

            if (comparisonMode != ResultComparisonMode.None)
            {
                TestResultComparisons.Compare(compareFirstFilename, compareSecondFilename, comparisonMode, resultsFilename);
                return;
            }
            if (mergeResultsFilespecs.Count > 0)
            {
                TestResults.Merge(mergeResultsFilespecs.ToArray(), resultsFilename);
                return;
            }
            ExecuteTestRuns();
        }

        static void ExecuteTestRuns()
        {
            var results = new TestResults();
            if (!(testParams is null))
                testParams.Override(parseResult);
            var testRuns = (testParams is null ? new[] { new TestRun(parseResult) } : testParams.GetParamSweeps().Select(sweep => new TestRun(sweep))).ToArray();

            // This overall time includes overhead for allocating and distributing the keys, 
            // which has to be done per-test-run.
            var sw = new Stopwatch();
            sw.Start();

            int testNum = 0;
            foreach (var testRun in testRuns)
            {
                Console.WriteLine($"Test {++testNum} of {testRuns.Length}");

                // If running from a testfile, print command line for investigating testfile failures
                if (!(testParams is null))
                    Console.WriteLine(testRun.TestResult);  

                Globals.DataSize = testRun.TestResult.Inputs.DataSize;
                verboseInterval = 1L << (testRun.TestResult.Inputs.HashSizeShift - 1);

                CreateKeys(testRun.TestResult);

                for (testRun.currentIter = 0; testRun.currentIter < testRun.TestResult.Inputs.IterationCount; ++testRun.currentIter)
                {
                    const int pauseMs = 1000;
                    if (verbose)
                    {
                        var workingSetMB = (ulong)Process.GetCurrentProcess().WorkingSet64 / 1048576;
                        Console.Write($"GC.Collect and pausing for {pauseMs}ms before starting iteration {testRun.currentIter}." +
                                      $" Working set: before {workingSetMB}MB, ");
                    }
                    GC.Collect();
                    if (verbose)
                    {
                        var workingSetMB = (ulong)Process.GetCurrentProcess().WorkingSet64 / 1048576;
                        Console.WriteLine($"after {workingSetMB}MB");
                    }
                    Thread.Sleep(pauseMs);

                    if (testRun.TestResult.Inputs.UseVarLenValue)
                    {
                        var fht = new FHT<VarLenValue, VarLenOutput, VarLenFunctions, NoSerializer<VarLenValue>>(
                            false, testRun.TestResult.Inputs.HashSizeShift, testRun.TestResult.Inputs.UseVarLenValue, 
                            testRun.TestResult.Inputs.UseObjectValue, useReadCache: testRun.TestResult.Inputs.UseReadCache);
                        RunIteration(fht, testRun, new GetVarLenValueRef(testRun.TestResult.Inputs.ThreadCount));
                    }
                    else if (testRun.TestResult.Inputs.UseObjectValue)
                    {
                        var fht = new FHT<ObjectValue, ObjectValueOutput, ObjectValueFunctions, ObjectValueSerializer>(
                            false, testRun.TestResult.Inputs.HashSizeShift, testRun.TestResult.Inputs.UseVarLenValue, 
                            testRun.TestResult.Inputs.UseObjectValue, useReadCache: testRun.TestResult.Inputs.UseReadCache);
                        RunIteration(fht, testRun, new GetObjectValueRef(testRun.TestResult.Inputs.ThreadCount));

                    } else
                    {
                        switch (Globals.DataSize) {
                            case 8:
                                RunBlittableIteration<BlittableValue8>(testRun);
                                break;
                            case 16:
                                RunBlittableIteration<BlittableValue16>(testRun);
                                break;
                            case 32:
                                RunBlittableIteration<BlittableValue32>(testRun);
                                break;
                            case 64:
                                RunBlittableIteration<BlittableValue64>(testRun);
                                break;
                            case 128:
                                RunBlittableIteration<BlittableValue128>(testRun);
                                break;
                            case 256:
                                RunBlittableIteration<BlittableValue256>(testRun);
                                break;
                            default:
                                throw new InvalidOperationException($"Unexpected Blittable data size: {Globals.DataSize}");
                        }
                    }
                }
                testRun.Finish();
                results.Add(testRun.TestResult);
            }

            sw.Stop();

            if (results.Results.Length == 0)
            {
                Console.WriteLine("No tests were run");
                return;
            }

            Console.WriteLine($"Completed {results.Results.Length} test run(s) in {TimeSpan.FromMilliseconds(sw.ElapsedMilliseconds)}");
            if (!string.IsNullOrEmpty(resultsFilename))
            {
                results.Write(resultsFilename);
                Console.WriteLine($"Results written to {resultsFilename}");
            }
        }

        static void CreateKeys(TestResult testResult)
        {
            // Just to make the test complete a little faster, don't rebuild if we don't have to.
            // This is not part of the timed test.
            if (!(prevTestResult is null)
                    && prevTestResult.Inputs.InitKeyCount == testResult.Inputs.InitKeyCount
                    && prevTestResult.Inputs.OperationKeyCount == testResult.Inputs.OperationKeyCount
                    && prevTestResult.Inputs.DistributionInfo == testResult.Inputs.DistributionInfo)
            {
                Console.WriteLine("Reusing keys from prior run");
                return;
            }

            var sw = new Stopwatch();
            sw.Start();

            prevTestResult = null;

            initKeys = new Key[testResult.Inputs.InitKeyCount];
            for (var ii = 0; ii < testResult.Inputs.InitKeyCount; ++ii)
                initKeys[ii] = new Key(ii);

            var rng = new RandomGenerator((uint)testResult.Inputs.DistributionSeed);
            if (testResult.Inputs.Distribution == Distribution.Uniform)
            {
                opKeys = new Key[testResult.Inputs.OperationKeyCount];
                for (var ii = 0; ii < opKeys.Length; ++ii)
                    opKeys[ii] = new Key ((long)rng.Generate64((ulong)testResult.Inputs.InitKeyCount));
            } else
            {
                opKeys = new Zipf<Key>().GenerateOpKeys(initKeys, testResult.Inputs.OperationKeyCount,
                                                        testResult.Inputs.DistributionParameter, rng,
                                                        testResult.Inputs.Distribution == Distribution.ZipfShuffled, verbose);
            }
            prevTestResult = testResult;

            sw.Stop();
            var workingSetMB = (ulong)Process.GetCurrentProcess().WorkingSet64 / 1048576;
            Console.WriteLine($"Initialization: Time to generate {testResult.Inputs.InitKeyCount} keys" + 
                              $" and {testResult.Inputs.OperationKeyCount} operation keys in {testResult.Inputs.Distribution} distribution:" +
                              $" {sw.ElapsedMilliseconds / 1000.0:0.000} sec; working set {workingSetMB}MB");
        }

        static void RunBlittableIteration<TBV>(TestRun testRun) where TBV : IBlittableValue, new()
        {
            var fht = new FHT<TBV, BlittableOutput<TBV>, BlittableFunctions<TBV>, NoSerializer<TBV>>(
                            usePsf: false, sizeShift: testRun.TestResult.Inputs.HashSizeShift, useVarLenValues: false, 
                            useObjectValues: false, useReadCache: testRun.TestResult.Inputs.UseReadCache);
            RunIteration(fht, testRun, new GetBlittableValueRef<TBV>(testRun.TestResult.Inputs.ThreadCount));
        }

        static void RunIteration<TValue, TOutput, TFunctions, TSerializer>(
                    FHT<TValue, TOutput, TFunctions, TSerializer> fht, TestRun testRun,
                    IGetValueRef<TValue, TOutput> getValueRef)
            where TValue : new()
            where TOutput : new()
            where TFunctions : IFunctions<Key, TValue, Input, TOutput, Empty>, new()
            where TSerializer : BinaryObjectSerializer<TValue>, new()
        {
            Initialize(fht, testRun, getValueRef);
            FlushLog(fht, testRun);
            RunOperations(fht, testRun, getValueRef);

            fht.Close();

            if (prompt)
            {
                Console.WriteLine("Press <ENTER> to end");
                Console.ReadLine();
            }
        }

        private static void Initialize<TValue, TOutput, TFunctions, TSerializer>(
                FHT<TValue, TOutput, TFunctions, TSerializer> fht, TestRun testRun,
                IGetValueRef<TValue, TOutput> getValueRef)
            where TValue : new()
            where TOutput : new()
            where TFunctions : IFunctions<Key, TValue, Input, TOutput, Empty>, new()
            where TSerializer : BinaryObjectSerializer<TValue>, new()
        {
            if (verbose)
                Console.WriteLine($"Writing initial key values from 0 to {testRun.TestResult.Inputs.InitKeyCount} to FASTER");

            var sw = new Stopwatch();
            sw.Start();

            // Reset the global chunk tracker.
            NextChunkStart = 0;

            var tasks = Enumerable.Range(0, testRun.TestResult.Inputs.ThreadCount)
                                  .Select(threadIdx => Task.Run(() => Initialize(fht, threadIdx, testRun,
                                                                                 ref getValueRef.GetRef(threadIdx))));
            Task.WaitAll(tasks.ToArray());

            sw.Stop();

            testRun.InitializeMs = (ulong)sw.ElapsedMilliseconds;
            var numSec = sw.ElapsedMilliseconds / 1000.0;
            var workingSetMB = (ulong)Process.GetCurrentProcess().WorkingSet64 / 1048576;
            Console.WriteLine($"Initialization: Time to insert {testRun.TestResult.Inputs.InitKeyCount} initial key values:" +
                              $" {numSec:0.000} sec ({testRun.TestResult.Inputs.InitKeyCount / numSec:0.00} inserts/sec;" +
                              $" {testRun.TestResult.Inputs.InitKeyCount / (numSec * testRun.TestResult.Inputs.ThreadCount):0.00} thread/sec);" +
                              $" working set {workingSetMB}MB");
        }

        private static void Initialize<TValue, TOutput, TFunctions, TSerializer>(
                FHT<TValue, TOutput, TFunctions, TSerializer> fht, int threadIndex, TestRun testRun, ref TValue value)
            where TValue : new()
            where TOutput : new()
            where TFunctions : IFunctions<Key, TValue, Input, TOutput, Empty>, new()
            where TSerializer : BinaryObjectSerializer<TValue>, new()
        {
            // Each thread needs to set NUMA and create a FASTER session
            Numa.AffinitizeThread(testRun.TestResult.Inputs.NumaMode, threadIndex);
            using var session = fht.Faster.NewSession(null, true);

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
                            if (verbose && ii % verboseInterval == 0)
                            {
                                long workingSetMB = Process.GetCurrentProcess().WorkingSet64 / 1048576;
                                Console.WriteLine($"Insert: {ii}, {workingSetMB}MB");
                            }
                        }
                    }
                    session.Upsert(ref initKeys[ii], ref value, Empty.Default, 1);
                }
            }
            session.CompletePending(true);
        }

        private static void FlushLog<TValue, TOutput, TFunctions, TSerializer>(
                FHT<TValue, TOutput, TFunctions, TSerializer> fht, TestRun testRun)
            where TValue : new()
            where TOutput : new()
            where TFunctions : IFunctions<Key, TValue, Input, TOutput, Empty>, new()
            where TSerializer : BinaryObjectSerializer<TValue>, new()
        {
            if (verbose)
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

        private static void RunOperations<TValue, TOutput, TFunctions, TSerializer>(
                FHT<TValue, TOutput, TFunctions, TSerializer> fht, TestRun testRun,
                IGetValueRef<TValue, TOutput> getValueRef)
            where TValue : new()
            where TOutput : new()
            where TFunctions : IFunctions<Key, TValue, Input, TOutput, Empty>, new()
            where TSerializer : BinaryObjectSerializer<TValue>, new()
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
                    yield return (Operations.Mixed, "mixed " + string.Join(", ", getMixedOpNames()), testRun.TestResult.Inputs.TotalOpCount);
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

                // Split the counts to be per-thread (that is, if we have --reads 100m and --threads 4,
                // each thread will get 25m reads).
                long threadOpCount = (long)opCount / testRun.TestResult.Inputs.ThreadCount;
                var tasks = Enumerable.Range(0, testRun.TestResult.Inputs.ThreadCount)
                                      .Select(threadIdx => Task.Run(() => RunOperations(fht, op, threadOpCount,
                                                                                        threadIdx, testRun, getValueRef)));
                Task.WaitAll(tasks.ToArray());

                sw.Stop();

                var numSec = sw.ElapsedMilliseconds / 1000.0;

                // Total Ops/Second is always reported 
                testRun.TotalOpsMs = (ulong)sw.ElapsedMilliseconds;

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
                Console.WriteLine($"Iteration {testRun.currentIter}: Time for {opCount} {opName} operations:" +
                                  $" {numSec:0.000} sec ({opCount / numSec:0.00} {op}{suffix}/sec)");
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

        private static void RunOperations<TValue, TOutput, TFunctions, TSerializer>(
                FHT<TValue, TOutput, TFunctions, TSerializer> fht, Operations op, long opCount,
                int threadIndex, TestRun testRun, IGetValueRef<TValue, TOutput> getValueRef)
            where TValue : new()
            where TOutput : new()
            where TFunctions : IFunctions<Key, TValue, Input, TOutput, Empty>, new()
            where TSerializer : BinaryObjectSerializer<TValue>, new()
        {
            // Each thread needs to set NUMA and create a FASTER session
            Numa.AffinitizeThread(testRun.TestResult.Inputs.NumaMode, threadIndex);
            using var session = fht.Faster.NewSession(null, true);

            if (verbose)
                Console.WriteLine($"Running Operation {op} count {opCount} for threadId {threadIndex}");

            var rng = new RandomGenerator((uint)threadIndex);
            var totalOpCount = testRun.TestResult.Inputs.TotalOpCount;
            var upsertThreshold = testRun.TestResult.Inputs.UpsertCount;
            var readThreshold = upsertThreshold + testRun.TestResult.Inputs.ReadCount;
            var rmwThreshold = readThreshold + testRun.TestResult.Inputs.RMWCount;

            ref TValue value = ref getValueRef.GetRef(threadIndex);
            var input = default(Input);
            var output = getValueRef.GetOutput(threadIndex);

            long currentCount = 0;

            // We multiply the number of operations by the number of threads, so we will wrap around
            // the end of the operations keys if we get there
            for (long chunkStart = Interlocked.Add(ref NextChunkStart, Globals.ChunkSize) - Globals.ChunkSize;
                currentCount < opCount;
                chunkStart = Interlocked.Add(ref NextChunkStart, Globals.ChunkSize) - Globals.ChunkSize)
            {
                chunkStart %= opKeys.Length;
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
                            if (verbose && ii % verboseInterval == 0)
                            {
                                Console.WriteLine($"{thisOp}: {ii}");
                            }
                        }
                    }

                    Status status = Status.OK;
                    switch (thisOp)
                    {
                        case Operations.Upsert:
                            status = session.Upsert(ref opKeys[ii], ref value, Empty.Default, 1);
                            break;
                        case Operations.Read:
                            status = session.Read(ref opKeys[ii], ref input, ref output, Empty.Default, 0);
                            break;
                        case Operations.RMW:
                            input.value = (int)(ii & 7);
                            status = session.RMW(ref opKeys[ii], ref input, Empty.Default, 0);
                            break;
                    }

                    if (status != Status.OK && status != Status.PENDING)
                        throw new ApplicationException($"Error: Unexpected status in {nameof(RunOperations)} {thisOp}; key[{ii}] = {opKeys[ii].key}: {status}");
                }
            }
            session.CompletePending(true);
        }
    }
}
