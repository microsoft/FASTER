// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using Newtonsoft.Json;
using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace PerfTest
{
    partial class PerfTest
    {
        static readonly TestResult defaultTestResult = new TestResult();
        static TestParameters testParams;
        static string testFilename;
        static string resultsFilename;
        static string compareFirstFilename, compareSecondFilename;
        static ResultComparisonMode comparisonMode = ResultComparisonMode.None;

        static bool verbose = false;
        static bool prompt = false;

        internal static JsonSerializerSettings jsonSerializerSettings = new JsonSerializerSettings { Formatting = Formatting.Indented };

        static void Main(string[] argv)
        {
            if (!ParseArgs(argv))
                return;

            if (comparisonMode != ResultComparisonMode.None)
            {
                TestResultComparisons.Compare(compareFirstFilename, compareSecondFilename, comparisonMode, resultsFilename);
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

            var sw = new Stopwatch();
            sw.Start();

            int testNum = 0;
            foreach (var testRun in testRuns)
            {
                Console.WriteLine($"Test {++testNum} of {testRuns.Length}");

                CacheGlobals.DataSize = testRun.TestResult.DataSize;
                for (testRun.currentIter = 0; testRun.currentIter < testRun.TestResult.IterationCount; ++testRun.currentIter)
                {
                    if (testRun.currentIter > 0)
                    {
                        const int pauseMs = 1000;
                        if (verbose)
                            Console.WriteLine($"GC and pausing for {pauseMs} before starting iteration {testRun.currentIter}");
                        GC.Collect();
                        Thread.Sleep(pauseMs);
                    }

                    if (testRun.TestResult.UseVarLenValue)
                    {
                        var fht = new FHT<VarLenValue, VarLenOutput, VarLenFunctions, UnusedSerializer<VarLenValue>>(
                            false, testRun.TestResult.UseVarLenValue, testRun.TestResult.UseObjectValue, useReadCache: testRun.TestResult.UseReadCache);
                        RunIteration(fht, testRun);

                    }
                    else if (testRun.TestResult.UseObjectValue)
                    {
                        var fht = new FHT<CacheObjectValue, CacheObjectOutput, CacheObjectFunctions, CacheObjectSerializer>(
                            false, testRun.TestResult.UseVarLenValue, testRun.TestResult.UseObjectValue, useReadCache: testRun.TestResult.UseReadCache);
                        RunIteration(fht, testRun);

                    } else
                    {
                        switch (CacheGlobals.DataSize) {
                            case 8:
                                RunBlittableIteration<CacheBlittableValue8>(testRun);
                                break;
                            case 16:
                                RunBlittableIteration<CacheBlittableValue16>(testRun);
                                break;
                            case 32:
                                RunBlittableIteration<CacheBlittableValue32>(testRun);
                                break;
                            case 64:
                                RunBlittableIteration<CacheBlittableValue64>(testRun);
                                break;
                            case 128:
                                RunBlittableIteration<CacheBlittableValue128>(testRun);
                                break;
                            case 256:
                                RunBlittableIteration<CacheBlittableValue256>(testRun);
                                break;
                            default:
                                throw new InvalidOperationException($"Unexpected Blittable data size: {CacheGlobals.DataSize}");
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

        static void RunBlittableIteration<BVT>(TestRun testRun) where BVT : ICacheValue<BVT>, new()
        {
            var fht = new FHT<BVT, CacheBlittableOutput<BVT>, CacheBlittableFunctions<BVT>, UnusedSerializer<BVT>>(
                            usePsf: false, useVarLenValues: false, useObjectValues: false, useReadCache: testRun.TestResult.UseReadCache);
            RunIteration(fht, testRun);
        }

        static void RunIteration<TValue, TOutput, TFunctions, TSerializer>(
                FHT<TValue, TOutput, TFunctions, TSerializer> fht, TestRun testRun)
            where TValue : ICacheValue<TValue>, new()
            where TOutput : ICacheOutput<TValue>, new()
            where TFunctions : IFunctions<CacheKey, TValue, CacheInput, TOutput, CacheContext>, new()
            where TSerializer : BinaryObjectSerializer<TValue>, new()
        {
            RunUpserts(fht, testRun);
            FlushLog(fht, testRun);
            RunRandomReads(fht, testRun);

            fht.Close();

            if (prompt)
            {
                Console.WriteLine("Press <ENTER> to end");
                Console.ReadLine();
            }
        }

        private static void RunUpserts<TValue, TOutput, TFunctions, TSerializer>(
                FHT<TValue, TOutput, TFunctions, TSerializer> fht, TestRun testRun)
            where TValue : ICacheValue<TValue>, new()
            where TOutput : ICacheOutput<TValue>, new()
            where TFunctions : IFunctions<CacheKey, TValue, CacheInput, TOutput, CacheContext>, new()
            where TSerializer : BinaryObjectSerializer<TValue>, new()
        {
            // Thread starts session with FASTER
            var session = fht.Faster.NewSession();

            // We use context to store and report latency of async operations
            if (verbose)
                Console.WriteLine($"Writing keys from 0 to {testRun.TestResult.UpsertCount} to FASTER");

            var creator = new TValue();

            var sw = new Stopwatch();
            sw.Start();
            for (int i = 0; i < testRun.TestResult.UpsertCount; i++)
            {
                if (verbose && i > 0 && i % (1 << 19) == 0)
                {
                    long workingSet = Process.GetCurrentProcess().WorkingSet64;
                    Console.WriteLine($"{i}: {workingSet / 1048576}M");
                }
                var key = new CacheKey(i);
                var value = creator.Create(i);
                session.Upsert(ref key, ref value, CacheContext.None, 0);
            }
            sw.Stop();

            session.Dispose();

            testRun.totalUpsertMs += (ulong)sw.ElapsedMilliseconds;
            var numSec = sw.ElapsedMilliseconds / 1000.0;
            var workingSetMB = (ulong)Process.GetCurrentProcess().WorkingSet64 / 1048576;
            testRun.totalWorkingSetMB += workingSetMB;
            Console.WriteLine("Iteration {4} time to upsert {0} elements: {1:0.000} secs ({2:0.00} inserts/sec), working set {3}MB",
                              testRun.TestResult.UpsertCount, numSec, testRun.TestResult.UpsertCount / numSec, workingSetMB, testRun.currentIter);
        }

        private static void FlushLog<TValue, TOutput, TFunctions, TSerializer>(
                FHT<TValue, TOutput, TFunctions, TSerializer> fht, TestRun testRun)
            where TValue : ICacheValue<TValue>, new()
            where TOutput : ICacheOutput<TValue>, new()
            where TFunctions : IFunctions<CacheKey, TValue, CacheInput, TOutput, CacheContext>, new()
            where TSerializer : BinaryObjectSerializer<TValue>, new()
        {
            if (verbose)
                Console.WriteLine("Flushing log");
            switch (testRun.TestResult.LogMode)
            {
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
                    Console.WriteLine($"Missing LogMode case: {testRun.TestResult.LogMode}");
                    return;
            }
        }

        private static void RunRandomReads<TValue, TOutput, TFunctions, TSerializer>(
                FHT<TValue, TOutput, TFunctions, TSerializer> fht, TestRun testRun)
            where TValue : ICacheValue<TValue>, new()
            where TOutput : ICacheOutput<TValue>, new()
            where TFunctions : IFunctions<CacheKey, TValue, CacheInput, TOutput, CacheContext>, new()
            where TSerializer : BinaryObjectSerializer<TValue>, new()
        {
            var sw = new Stopwatch();
            sw.Start();

            var tasks = Enumerable.Range(0, testRun.TestResult.ReadThreadCount)
                                  .Select(ii => Task.Run(() => RunRandomReads<TValue, TOutput, TFunctions, TSerializer>(fht, ii, testRun))).ToArray();
            Task.WaitAll(tasks);
            var statusPending = tasks.Select(task => task.Result).Sum();

            sw.Stop();

            var numSec = sw.ElapsedMilliseconds / 1000.0;
            testRun.totalReadMs += (ulong)sw.ElapsedMilliseconds;
            testRun.totalReadsPending += (ulong)statusPending;

            Console.WriteLine("Iteration {5} time to read {0} elements: {1:0.000} secs ({2:0.00} reads/sec), PENDING {3} ({4:0.00}%)",
                              testRun.numReadsPerIteration, numSec, testRun.numReadsPerIteration / numSec, statusPending,
                              ((double)statusPending / testRun.numReadsPerIteration) * 100, testRun.currentIter);
        }

        private static int RunRandomReads<TValue, TOutput, TFunctions, TSerializer>(
                FHT<TValue, TOutput, TFunctions, TSerializer> fht, int threadId, TestRun testRun)
            where TValue : ICacheValue<TValue>, new()
            where TOutput : ICacheOutput<TValue>, new()
            where TFunctions : IFunctions<CacheKey, TValue, CacheInput, TOutput, CacheContext>, new()
            where TSerializer : BinaryObjectSerializer<TValue>, new()
        {
            var numReads = testRun.TestResult.UpsertCount * testRun.TestResult.ReadMultiple;
            if (verbose)
                Console.WriteLine($"Issuing uniform random read workload of {numReads} reads for threadId {threadId}");

            int statusPending = 0;
            var rng = new Random(threadId);
            using (var session = fht.Faster.NewSession())
            {
                var output = new TOutput();
                var input = default(CacheInput);

                for (int i = 0; i < numReads; i++)
                {
                    if (verbose && i > 0 && i % (1 << 19) == 0)
                        Console.WriteLine($"{i}");

                    var key = new CacheKey(rng.Next(testRun.TestResult.UpsertCount));
                    var status = session.Read(ref key, ref input, ref output, CacheContext.None, 0);

                    switch (status)
                    {
                        case Status.PENDING:
                            if (++statusPending % 1000 == 0)
                                session.CompletePending(false);
                            break;
                        case Status.OK:
                            if (output.Value.Value != key.key)
                                throw new Exception($"Error: Value does not match key in {nameof(RunRandomReads)}");
                            break;
                        default:
                            throw new Exception($"Error: Unexpected status in {nameof(RunRandomReads)}");
                    }
                }
                session.CompletePending(true);
            }
            return statusPending;
        }
    }
}
