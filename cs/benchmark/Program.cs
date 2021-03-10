// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Threading;

namespace FASTER.benchmark
{
    public class Program
    {
        const int kTrimResultCount = 3; // Use some high value like int.MaxValue to disable

        public static void Main(string[] args)
        {
            var testLoader = new TestLoader();
            if (!testLoader.Parse(args))
                return;

            var testStats = new TestStats(testLoader.Options);
            testLoader.LoadData();
            var options = testLoader.Options;   // shortcut

            for (var iter = 0; iter < options.IterationCount; ++iter)
            {
                Console.WriteLine();
                if (options.IterationCount > 1)
                    Console.WriteLine($"Iteration {iter + 1} of {options.IterationCount}");

                switch (testLoader.BenchmarkType)
                {
                    case BenchmarkType.Ycsb:
                        var yTest = new FASTER_YcsbBenchmark(testLoader.init_keys, testLoader.txn_keys, testLoader);
                        testStats.AddResult(yTest.Run(testLoader));
                        yTest.Dispose();
                        break;
                    case BenchmarkType.SpanByte:
                        var sTest = new FasterSpanByteYcsbBenchmark(testLoader.init_span_keys, testLoader.txn_span_keys, testLoader);
                        testStats.AddResult(sTest.Run(testLoader));
                        sTest.Dispose();
                        break;
                    case BenchmarkType.ConcurrentDictionaryYcsb:
                        var cTest = new ConcurrentDictionary_YcsbBenchmark(testLoader.init_keys, testLoader.txn_keys, testLoader);
                        testStats.AddResult(cTest.Run(testLoader));
                        cTest.Dispose();
                        break;
                    default:
                        throw new ApplicationException("Unknown benchmark type");
                }

                if (options.IterationCount > 1)
                {
                    testStats.ShowAllStats(AggregateType.Running);
                    if (iter < options.IterationCount - 1)
                    {
                        GC.Collect();
                        GC.WaitForFullGCComplete();
                        Thread.Sleep(1000);
                    }
                }
            }

            Console.WriteLine();
            testStats.ShowAllStats(AggregateType.FinalFull);
            if (options.IterationCount >= kTrimResultCount)
                testStats.ShowTrimmedStats();
        }
    }
}
