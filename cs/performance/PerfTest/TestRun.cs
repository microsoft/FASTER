// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;

namespace FASTER.PerfTest
{
    internal class TestRun
    {
        internal TestResult TestResult = new TestResult();

        // Time for the initial upserts (which will be inserts)
        internal ulong InitializeMs;

        // We'll use either OpsMs or Upsert/Read/RMWMs depending on whether MixOoperations was specified
        internal ulong TotalOpsMs;
        internal ulong TotalUpsertMs;
        internal ulong TotalReadMs;
        internal ulong TotalRMWMs;

        // Current iteration of the test.
        internal int currentIter;

        internal TestRun(TestResult testResult) => this.TestResult = testResult;

        internal void Finish()
        {
            // Calculate per-second stats
            var initSec = this.InitializeMs / (TestResult.IterationCount * 1000.0);
            this.TestResult.InitialInsertsPerSecond = TestResult.InitKeyCount / initSec;
            this.TestResult.InitialInsertsPerSecondPerThread = this.TestResult.InitialInsertsPerSecond / this.TestResult.ThreadCount;

            var totalOpSec = this.TotalOpsMs / (TestResult.IterationCount * 1000.0);
            this.TestResult.TotalOperationsPerSecond = TestResult.TotalOpCount / totalOpSec;
            this.TestResult.TotalOperationsPerSecondPerThread = this.TestResult.TotalOperationsPerSecond / this.TestResult.ThreadCount;

            var upsertSec = this.TotalUpsertMs / (TestResult.IterationCount * 1000.0);
            this.TestResult.UpsertsPerSecond = TestResult.MixOperations || TestResult.UpsertCount == 0 
                                                ? 0 : TestResult.UpsertCount / upsertSec;
            this.TestResult.UpsertsPerSecondPerThread = this.TestResult.UpsertsPerSecond / this.TestResult.ThreadCount;

            var readSec = this.TotalReadMs / (TestResult.IterationCount * 1000.0);
            this.TestResult.ReadsPerSecond = TestResult.MixOperations || TestResult.ReadCount == 0 
                                                ? 0 : TestResult.ReadCount / readSec;
            this.TestResult.ReadsPerSecondPerThread = this.TestResult.ReadsPerSecond / this.TestResult.ThreadCount;

            var rmwSec = this.TotalRMWMs / (TestResult.IterationCount * 1000.0);
            this.TestResult.RMWsPerSecond = TestResult.MixOperations || TestResult.RMWCount == 0 
                                                ? 0 : TestResult.RMWCount / rmwSec;
            this.TestResult.RMWsPerSecondPerThread = this.TestResult.RMWsPerSecond / this.TestResult.ThreadCount;

            var valueType = TestResult.UseVarLenValue
                ? "varLenVals"
                : TestResult.UseObjectValue ? "objVals" : "fixBlitVals";

            // Try to keep this in one line...
            var tCount = TestResult.TotalOpCount > 1_000_000 ? $"{(TestResult.TotalOpCount / 1_000_000)}m" : $"{TestResult.TotalOpCount}";
            var uCount = TestResult.UpsertCount > 1_000_000 ? $"{(TestResult.UpsertCount / 1_000_000)}m" : $"{TestResult.UpsertCount}";
            var rCount = TestResult.ReadCount > 1_000_000 ? $"{(TestResult.ReadCount / 1_000_000)}m" : $"{TestResult.ReadCount}";
            var mCount = TestResult.RMWCount > 1_000_000 ? $"{(TestResult.RMWCount / 1_000_000)}m" : $"{TestResult.RMWCount}";
            Console.WriteLine($"----- Summary: totOps {tCount} (u {uCount}, r {rCount}, m {mCount})," +
                              $" inkeys {TestResult.InitKeyCount}, opKeys {TestResult.OperationKeyCount}," +
                              $" {valueType}, data {Globals.DataSize}, threads {TestResult.ThreadCount}," + 
                              $" rCache {TestResult.UseReadCache}, log.{TestResult.LogMode}, iters {currentIter}");

            Console.WriteLine($"{TestResult.InitKeyCount} Initial Keys inserted in {initSec:0.000} sec ({this.TestResult.InitialInsertsPerSecond:0.00} Inserts/sec; {this.TestResult.InitialInsertsPerSecondPerThread:0.00} thread/sec)");
            if (TestResult.MixOperations)
            {
                Console.WriteLine($"{TestResult.TotalOpCount} Mixed Operations in {totalOpSec:0.000} sec ({this.TestResult.TotalOperationsPerSecond:0.00} Operations/sec; {this.TestResult.TotalOperationsPerSecondPerThread:0.00} thread/sec)");
                Console.WriteLine($"    {TestResult.UpsertCount} Upserts ({((double)TestResult.UpsertCount / TestResult.TotalOpCount) * 100:0.00}%)");
                Console.WriteLine($"    {TestResult.ReadCount} Reads ({((double)TestResult.ReadCount / TestResult.TotalOpCount) * 100:0.00}%)");
                Console.WriteLine($"    {TestResult.RMWCount} RMWs ({((double)TestResult.RMWCount/ TestResult.TotalOpCount) * 100:0.00}%)");
            }
            else
            {
                Console.WriteLine($"{TestResult.TotalOpCount} Separate Operations in {totalOpSec:0.000} sec ({this.TestResult.TotalOperationsPerSecond:0.00} Operations/sec; {this.TestResult.TotalOperationsPerSecondPerThread:0.00} thread/sec)");
                Console.WriteLine($"{TestResult.UpsertCount} Upserts in {upsertSec:0.000} sec ({this.TestResult.UpsertsPerSecond:0.00} Upserts/sec; {this.TestResult.UpsertsPerSecondPerThread:0.00} thread/sec)");
                Console.WriteLine($"{TestResult.ReadCount} Reads in {readSec:0.000} sec ({this.TestResult.ReadsPerSecond:0.00} Reads/sec; {this.TestResult.ReadsPerSecondPerThread:0.00} thread/sec)");
                Console.WriteLine($"{TestResult.RMWCount} RMWs in {rmwSec:0.000} sec ({this.TestResult.RMWsPerSecond:0.00} RMWs/sec; {this.TestResult.RMWsPerSecondPerThread:0.00} thread/sec)");
            }
            Console.WriteLine();
        }
    }
}
