// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Linq;

namespace FASTER.PerfTest
{
    using OperationResults = TestOutputs.OperationResults;
    using ResultStats = TestOutputs.ResultStats;

    internal class TestRun
    {
        internal TestResult TestResult = new TestResult();

        // Time for the initial upserts (which will be inserts)
        internal ulong[] initializeMs;
        internal ulong InitializeMs { set => this.initializeMs[this.currentIter] = value; }

        // We'll always record opsMs, and record Upsert/Read/RMWMs depending on whether MixOoperations was specified
        private readonly ulong[] totalOpsMs;
        internal ulong TotalOpsMs { set => this.totalOpsMs[this.currentIter] = value; }
        private readonly ulong[] upsertMs;
        internal ulong UpsertMs { set => this.upsertMs[this.currentIter] = value; }
        private readonly ulong[] readMs;
        internal ulong ReadMs { set => this.readMs[this.currentIter] = value; }
        private readonly ulong[] rmwMs;
        internal ulong RMWMs { set => this.rmwMs[this.currentIter] = value; }

        // Current iteration of the test.
        internal int currentIter;

        internal TestRun(TestResult testResult)
        {
            this.TestResult = testResult;
            this.initializeMs = new ulong[testResult.Inputs.IterationCount];
            this.totalOpsMs = new ulong[testResult.Inputs.IterationCount];
            this.upsertMs = new ulong[testResult.Inputs.IterationCount];
            this.readMs = new ulong[testResult.Inputs.IterationCount];
            this.rmwMs = new ulong[testResult.Inputs.IterationCount];
        }

        internal void Finish()
        {
            ResultStats calcPerSecondStatsFull(ulong[] runMs, int opCount) 
                => ResultStats.Create(runMs.Select(ms => ms == 0 ? 0.0 : opCount / (ms / 1000.0)).ToArray());

            ResultStats calcPerSecondStatsTrimmed(ulong[] runMs, int opCount)
            {
                if (runMs.Length < 3)
                    return calcPerSecondStatsFull(runMs, opCount);

                // Trim the highest and lowest values. There may be ties so can't just delete at min/max value.
                ulong max = 0, min = ulong.MaxValue;
                int maxIndex = 0, minIndex = 0;
                for (int ii = 0; ii < runMs.Length; ++ii)
                {
                    if (runMs[ii] > max)
                    {
                        max = runMs[ii];
                        maxIndex = ii;
                    }
                    else if (runMs[ii] < min)
                    {
                        min = runMs[ii];
                        minIndex = ii;
                    }
                }
                var trimmedMs = runMs.Where((ms, ii) => ii != maxIndex && ii != minIndex).ToArray();
                 return calcPerSecondStatsFull(trimmedMs, opCount);
            }

            ResultStats calcPerThreadStats(ResultStats allThreads)
                => ResultStats.Create(allThreads.OperationsPerSecond.Select(n => n / this.TestResult.Inputs.ThreadCount).ToArray());

            var inputs = this.TestResult.Inputs;
            var outputs = this.TestResult.Outputs;

            void calcStats(Func<OperationResults> opResultsSelector, ulong[] runMs, int opCount)
            {
                OperationResults opResults = opResultsSelector();
                opResults.AllThreadsFull = calcPerSecondStatsFull(runMs, opCount);
                opResults.PerThreadFull = calcPerThreadStats(opResults.AllThreadsFull);
                opResults.AllThreadsTrimmed = calcPerSecondStatsTrimmed(runMs, opCount);
                opResults.PerThreadTrimmed = calcPerThreadStats(opResults.AllThreadsTrimmed);
            }

            calcStats(() => outputs.InitialInserts, this.initializeMs, inputs.InitKeyCount);
            calcStats(() => outputs.TotalOperations, this.totalOpsMs, inputs.TotalOpCount);
            calcStats(() => outputs.Upserts, this.upsertMs, inputs.UpsertCount);
            calcStats(() => outputs.Reads, this.readMs, inputs.ReadCount);
            calcStats(() => outputs.RMWs, this.rmwMs, inputs.RMWCount);

            var valueType = (TestResult.Inputs.UseVarLenValue, TestResult.Inputs.UseObjectValue) switch { 
                (true, _) => "varLenVals",
                (_, true) => "objVals",
                _ => "fixBlitVals" 
            };

            // Keep this in one readable line...
            var tCount = inputs.TotalOpCount > 1_000_000 ? $"{(inputs.TotalOpCount / 1_000_000)}m" : $"{inputs.TotalOpCount}";
            var uCount = inputs.UpsertCount > 1_000_000 ? $"{(inputs.UpsertCount / 1_000_000)}m" : $"{inputs.UpsertCount}";
            var rCount = inputs.ReadCount > 1_000_000 ? $"{(inputs.ReadCount / 1_000_000)}m" : $"{inputs.ReadCount}";
            var mCount = inputs.RMWCount > 1_000_000 ? $"{(inputs.RMWCount / 1_000_000)}m" : $"{inputs.RMWCount}";
            Console.WriteLine($"----- Summary: totOps {tCount} (u {uCount}, r {rCount}, m {mCount})," +
                                $" inkeys {inputs.InitKeyCount}, opKeys {inputs.OperationKeyCount}," +
                                $" {valueType}, data {Globals.DataSize}, threads {inputs.ThreadCount}," +
                                $" rCache {inputs.UseReadCache}, log.{inputs.LogMode}, iters {currentIter}");

            // LINQ does not have ulong Average() or Sum().
            static double meanSec(ulong[] ms) 
                => ms.Aggregate<ulong, ulong>(0, (total, current) => total + current) / (ms.Length * 1000.0);

            var initSec = meanSec(this.initializeMs);
            var totalOpSec = meanSec(this.totalOpsMs);
            var upsertSec = meanSec(this.upsertMs);
            var readSec = meanSec(this.readMs);
            var rmwSec = meanSec(this.rmwMs);

            Console.WriteLine($"{inputs.InitKeyCount} Initial Keys inserted in {initSec:0.000} sec" +
                              $" ({outputs.InitialInserts.AllThreadsFull.Mean:0.00} Inserts/sec;" +
                              $" {outputs.InitialInserts.PerThreadFull.Mean:0.00} thread/sec)");
            if (inputs.MixOperations)
            {
                Console.WriteLine($"{inputs.TotalOpCount} Mixed Operations in {totalOpSec:0.000} sec" +
                                  $" ({outputs.TotalOperations.AllThreadsFull.Mean:0.00} Operations/sec;" +
                                  $" {outputs.TotalOperations.PerThreadFull.Mean:0.00} thread/sec)");
                Console.WriteLine($"    {inputs.UpsertCount} Upserts ({((double)inputs.UpsertCount / inputs.TotalOpCount) * 100:0.00}%)");
                Console.WriteLine($"    {inputs.ReadCount} Reads ({((double)inputs.ReadCount / inputs.TotalOpCount) * 100:0.00}%)");
                Console.WriteLine($"    {inputs.RMWCount} RMWs ({((double)inputs.RMWCount / inputs.TotalOpCount) * 100:0.00}%)");
            }
            else
            {
                Console.WriteLine($"{inputs.TotalOpCount} Separate Operations in {totalOpSec:0.000} sec" +
                                  $" ({outputs.TotalOperations.AllThreadsFull.Mean:0.00} Operations/sec;" +
                                  $" {outputs.TotalOperations.PerThreadFull.Mean:0.00} thread/sec)");
                Console.WriteLine($"{inputs.UpsertCount} Upserts in {upsertSec:0.000} sec" +
                                  $" ({outputs.Upserts.AllThreadsFull.Mean:0.00} Upserts/sec; {outputs.Upserts.PerThreadFull.Mean:0.00} thread/sec)");
                Console.WriteLine($"{inputs.ReadCount} Reads in {readSec:0.000} sec ({outputs.Reads.AllThreadsFull.Mean:0.00} Reads/sec;" +
                                  $" {outputs.Reads.PerThreadFull.Mean:0.00} thread/sec)");
                Console.WriteLine($"{inputs.RMWCount} RMWs in {rmwSec:0.000} sec ({outputs.RMWs.AllThreadsFull.Mean:0.00} RMWs/sec;" +
                                  $" {outputs.RMWs.PerThreadFull.Mean:0.00} thread/sec)");
            }
            Console.WriteLine();
        }
    }
}
