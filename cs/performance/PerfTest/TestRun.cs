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
        internal ulong TotalOpsMs { get => this.totalOpsMs[this.currentIter]; set => this.totalOpsMs[this.currentIter] = value; }
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
            // Initial inserts split the keyCount among the threads; all other operations execute the full count on each thread.
            ResultStats calcPerSecondStatsFull(ulong[] runMs, int opCount, bool isInit) 
                => ResultStats.Create(runMs.Select(ms => ms == 0 ? 0.0 : (opCount * (isInit ? 1 : this.TestResult.Inputs.ThreadCount)) / (ms / 1000.0)).ToArray());

            ResultStats calcPerSecondStatsTrimmed(ulong[] runMs, int opCount, bool isInit)
            {
                if (runMs.Length < 3)
                    return calcPerSecondStatsFull(runMs, opCount, isInit);

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
                 return calcPerSecondStatsFull(trimmedMs, opCount, isInit);
            }

            ResultStats calcPerThreadStats(ResultStats allThreads)
                => ResultStats.Create(allThreads.OperationsPerSecond.Select(n => n / this.TestResult.Inputs.ThreadCount).ToArray());

            var inputs = this.TestResult.Inputs;
            var outputs = this.TestResult.Outputs;

            void calcStats(Func<OperationResults> opResultsSelector, ulong[] runMs, int opCount, bool isInit = false)
            {
                OperationResults opResults = opResultsSelector();
                opResults.AllThreadsFull = calcPerSecondStatsFull(runMs, opCount, isInit);
                opResults.PerThreadFull = calcPerThreadStats(opResults.AllThreadsFull);
                opResults.AllThreadsTrimmed = calcPerSecondStatsTrimmed(runMs, opCount, isInit);
                opResults.PerThreadTrimmed = calcPerThreadStats(opResults.AllThreadsTrimmed);
            }

            calcStats(() => outputs.InitialInserts, this.initializeMs, inputs.InitKeyCount, isInit:true);
            calcStats(() => outputs.TotalOperations, this.totalOpsMs, inputs.TotalOperationCount);
            calcStats(() => outputs.Upserts, this.upsertMs, inputs.UpsertCount);
            calcStats(() => outputs.Reads, this.readMs, inputs.ReadCount);
            calcStats(() => outputs.RMWs, this.rmwMs, inputs.RMWCount);

            static string kvType(bool useVarLen, bool useObj)
                => (useVarLen, useObj) switch {  (true, _) => "varlen", (_, true) => "obj", _ => "fixBlit" };
            var keyType = kvType(TestResult.Inputs.UseVarLenKey, TestResult.Inputs.UseObjectKey);
            var valueType = kvType(TestResult.Inputs.UseVarLenValue, TestResult.Inputs.UseObjectValue);

            // Keep this in one readable line...
            var inKeyCount = inputs.InitKeyCount > 1_000_000 ? $"{(inputs.InitKeyCount / 1_000_000)}m" : $"{inputs.InitKeyCount:N0}";
            var opKeyCount = inputs.OperationKeyCount > 1_000_000 ? $"{(inputs.OperationKeyCount / 1_000_000)}m" : $"{inputs.OperationKeyCount:N0}";
            var tCount = inputs.TotalOperationCount > 1_000_000 ? $"{(inputs.TotalOperationCount / 1_000_000)}m" : $"{inputs.TotalOperationCount:N0}";
            var uCount = inputs.UpsertCount > 1_000_000 ? $"{(inputs.UpsertCount / 1_000_000)}m" : $"{inputs.UpsertCount:N0}";
            var rCount = inputs.ReadCount > 1_000_000 ? $"{(inputs.ReadCount / 1_000_000)}m" : $"{inputs.ReadCount:N0}";
            var mCount = inputs.RMWCount > 1_000_000 ? $"{(inputs.RMWCount / 1_000_000)}m" : $"{inputs.RMWCount:N0}";
            Console.WriteLine($"----- Summary: totOps {tCount} (u {uCount}, r {rCount}, m {mCount})," +
                                $" inkeys {inKeyCount}, opKeys {opKeyCount}, ksize {Globals.KeySize} {keyType}, vsize {Globals.ValueSize} {valueType}," +
                                $" threads {inputs.ThreadCount}, rCache {inputs.UseReadCache}, log.{inputs.LogMode}, iters {currentIter}");

            // LINQ does not have ulong Average() or Sum().
            static double meanSec(ulong[] ms) 
                => ms.Aggregate<ulong, ulong>(0, (total, current) => total + current) / (ms.Length * 1000.0);

            var initSec = meanSec(this.initializeMs);
            var totalOpSec = meanSec(this.totalOpsMs);
            var upsertSec = meanSec(this.upsertMs);
            var readSec = meanSec(this.readMs);
            var rmwSec = meanSec(this.rmwMs);

            Console.WriteLine($"{inKeyCount} Initial Keys inserted in {initSec:N3} sec" +
                              $" ({outputs.InitialInserts.AllThreadsFull.Mean:N2} Inserts/sec;" +
                              $" {outputs.InitialInserts.PerThreadFull.Mean:N2} thread/sec)");
            if (inputs.MixOperations)
            {
                Console.WriteLine($"{tCount} Mixed Operations per thread ({inputs.TotalOperationCount * inputs.ThreadCount:N0} total) in {totalOpSec:N3} sec" +
                                  $" ({outputs.TotalOperations.AllThreadsFull.Mean:N2} Operations/sec;" +
                                  $" {outputs.TotalOperations.PerThreadFull.Mean:N2} thread/sec)");
                Console.WriteLine($"    {uCount} Upserts per thread ({inputs.UpsertCount * inputs.ThreadCount:N0} total) ({((double)inputs.UpsertCount / inputs.TotalOperationCount) * 100:N2}%)");
                Console.WriteLine($"    {rCount} Reads per thread ({inputs.ReadCount * inputs.ThreadCount:N0} total) ({((double)inputs.ReadCount / inputs.TotalOperationCount) * 100:N2}%)");
                Console.WriteLine($"    {mCount} RMWs per thread ({inputs.RMWCount * inputs.ThreadCount:N0} total) ({((double)inputs.RMWCount / inputs.TotalOperationCount) * 100:N2}%)");
            }
            else
            {
                Console.WriteLine($"{tCount} per thread ({inputs.TotalOperationCount * inputs.ThreadCount:N0} total) Separate Operations in {totalOpSec:N3} sec" +
                                  $" ({outputs.TotalOperations.AllThreadsFull.Mean:N2} Operations/sec;" +
                                  $" {outputs.TotalOperations.PerThreadFull.Mean:N2} thread/sec)");
                Console.WriteLine($"{uCount} Upserts per thread ({inputs.UpsertCount * inputs.ThreadCount:N0} total) in {upsertSec:N3} sec" +
                                  $" ({outputs.Upserts.AllThreadsFull.Mean:N2} Upserts/sec; {outputs.Upserts.PerThreadFull.Mean:N2} thread/sec)");
                Console.WriteLine($"{rCount} Reads per thread ({inputs.ReadCount * inputs.ThreadCount:N0} total) in {readSec:N3} sec ({outputs.Reads.AllThreadsFull.Mean:N2} Reads/sec;" +
                                  $" {outputs.Reads.PerThreadFull.Mean:N2} thread/sec)");
                Console.WriteLine($"{mCount} RMWs per thread ({inputs.RMWCount * inputs.ThreadCount:N0} total) in {rmwSec:N3} sec ({outputs.RMWs.AllThreadsFull.Mean:N2} RMWs/sec;" +
                                  $" {outputs.RMWs.PerThreadFull.Mean:N2} thread/sec)");
            }
            Console.WriteLine();
            Console.Out.Flush();
        }
    }
}
