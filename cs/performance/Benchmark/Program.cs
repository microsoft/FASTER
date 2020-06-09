// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using CommandLine;
using Performance.Common;
using System;
using System.Linq;
using System.Threading;

namespace FASTER.Benchmark
{

    // TODO: 
    //  No --testfile; but create a TestInputs in Main() from the properties here, and use that to run the program.
    //  + Build up ResultsStats and average like PerfTest (move some to Common)
    //  + Calculate and write the outputs
    //  + Check whether Merge is worth it
    //  + Move OperationComparison.cs and ResultStats to Common
    //     + But only do InitialInserts and TotalOperations
    //  + Change TestInputs to use NumaMode
    //  + Change the Distribution in the test classes to be Distribution -- move Distribution.cs to Common
    //  + remove extra space after “read,” “ops done”
    //  + PerfTest switch its summary ops/sec to base it on totalOps / totalMs, rather than sum(opsPerSec) / #runs
    //  + Move Distribution to Common

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
                Outputs = new TestOutputs { TransactionCountsFull = new long[options.IterationCount] }
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
                                                        options.GetDistribution(), options.ReadPercent);
                    (ulong initMs, ulong tranMs, long count) = test.Run();
                    initializeMs[iter] = initMs;
                    transactionMs[iter] = tranMs;
                    testResult.Outputs.TransactionCountsFull[iter] = count;
                }
                else if (b == BenchmarkType.ConcurrentDictionaryYcsb)
                {
                    var test = new ConcurrentDictionary_YcsbBenchmark(options.ThreadCount, options.GetNumaMode(), 
                                                                      options.GetDistribution(), options.ReadPercent);
                    test.Run();
                }

                if (iter < options.IterationCount - 1)
                {
                    GC.Collect();
                    Thread.Sleep(1000);
                }
            }

            ResultStats.Calc(outputs.InitialInserts, threadCount, inputs.InitKeyCount, initializeMs, inputs.InitKeyCount, isInit: true);
            var (trimmedMs, trimmedOps) = ResultStats.Calc(outputs.Transactions, threadCount, transactionMs, outputs.TransactionCountsFull);
            outputs.TransactionSecondsFull = transactionMs.Select(ms => ms / 1000.0).ToArray();
            outputs.TransactionSecondsTrimmed = trimmedMs.Select(ms => ms / 1000.0).ToArray();
            outputs.TransactionCountsTrimmed = trimmedOps;

            if (!string.IsNullOrEmpty(options.ResultsFile))
                testResult.Write(options.ResultsFile);
        }
    }
}
