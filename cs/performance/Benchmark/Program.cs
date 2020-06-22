// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using CommandLine;
using Performance.Common;
using System;
using System.Linq;
using System.Threading;

namespace FASTER.Benchmark
{
    public class Program
    {
        public static void Main(string[] args)
        {
            ParserResult<Options> result = Parser.Default.ParseArguments<Options>(args);
            if (result.Tag == ParserResultType.NotParsed)
                return;
            var options = result.MapResult(o => o, xs => new Options());
            if (!options.Verify())
                return;

            if (options.GetMergeFileSpecs(out var filespecs))
            {
                TestResult.Merge(filespecs, options.ResultsFile);
                return;
            }
            if (options.GetCompareFileSpecs(out filespecs))
            {
                if (filespecs.Length < 2)
                    Console.WriteLine("Compare requires exactly two file names");
                else
                    TestResultComparison.Compare(filespecs[0], filespecs[1], options.ResultsFile);
                return;
            }

            BenchmarkType b = (BenchmarkType)options.Benchmark;
            var testResult = new TestResult
            {
                Inputs = options.GetTestInputs(),
                Outputs = new TestOutputs
                {
                    Counts = new TestCounts { TransactionCountsFull = new long[options.IterationCount] }
                }
            };

            var inputs = testResult.Inputs;
            var outputs = testResult.Outputs;
            var threadCount = testResult.Inputs.ThreadCount;

            var initializeMs = new ulong[options.IterationCount];
            var transactionMs = new ulong[options.IterationCount];

            for (var iter = 0; iter < options.IterationCount; ++iter)
            {
                Console.WriteLine();
                Console.WriteLine($"Iteration {iter + 1} of {options.IterationCount}");
                if (b == BenchmarkType.Ycsb)
                {
                    var test = new FASTER_YcsbBenchmark(options.ThreadCount, options.GetNumaMode(), 
                                                        options.GetDistribution(), options.ReadPercent, options.RunSeconds);
                    (ulong initMs, ulong tranMs, long count) = test.Run();
                    initializeMs[iter] = initMs;
                    transactionMs[iter] = tranMs;
                    testResult.Outputs.Counts.TransactionCountsFull[iter] = count;
                }
                else if (b == BenchmarkType.ConcurrentDictionaryYcsb)
                {
                    var test = new ConcurrentDictionary_YcsbBenchmark(options.ThreadCount, options.GetNumaMode(), 
                                                                      options.GetDistribution(), options.ReadPercent, options.RunSeconds);
                    test.Run();
                }

                if (iter < options.IterationCount - 1)
                {
                    GC.Collect();
                    Thread.Sleep(1000);
                }
            }

            ResultStats.Calc(outputs.InitialInserts, threadCount, inputs.InitKeyCount, initializeMs, inputs.InitKeyCount, isInit: true);
            var (trimmedMs, trimmedOps) = ResultStats.Calc(outputs.Transactions, threadCount, transactionMs, outputs.Counts.TransactionCountsFull);
            outputs.Counts.TransactionSecondsFull = transactionMs.Select(ms => ms / 1000.0).ToArray();
            outputs.Counts.TransactionSecondsTrimmed = trimmedMs.Select(ms => ms / 1000.0).ToArray();
            outputs.Counts.TransactionCountsTrimmed = trimmedOps;

            var initSec = outputs.Counts.TransactionSecondsFull.Average();
            var iCount = outputs.Counts.TransactionCountsFull.Average() / inputs.IterationCount;
            var tCount = (int)(outputs.Counts.TransactionCountsFull.Average() / inputs.ThreadCount);
            var totalOpSec = outputs.Counts.TransactionSecondsFull.Average();
            Console.WriteLine($"----- Averages:");
            Console.WriteLine($"{FASTER_YcsbBenchmark.kInitCount:N0} Initial Keys inserted in {initSec:N3} sec" +
                              $" ({outputs.InitialInserts.AllThreadsFull.Mean:N2} Inserts/sec;" +
                              $" {outputs.InitialInserts.PerThreadFull.Mean:N2} thread/sec)");
            Console.WriteLine($"{tCount:N0} Mixed Operations per thread ({iCount * inputs.ThreadCount:N0} total) in {totalOpSec:N3} sec" +
                              $" ({outputs.Transactions.AllThreadsFull.Mean:N2} Operations/sec;" +
                              $" {outputs.Transactions.PerThreadFull.Mean:N2} thread/sec)");

            if (!string.IsNullOrEmpty(options.ResultsFile))
                testResult.Write(options.ResultsFile);
        }
    }
}
