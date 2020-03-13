// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;

namespace PerfTest
{
    internal class TestRun
    {
        internal TestResult TestResult = new TestResult();

        // Results in progress
        internal int numReadsPerIteration;
        internal ulong totalUpsertMs;
        internal ulong totalWorkingSetMB;
        internal ulong totalReadMs;
        internal ulong totalReadsPending;
        internal int currentIter;

        internal TestRun(TestResult testResult)
        {
            this.TestResult = testResult;
            numReadsPerIteration = testResult.UpsertCount * testResult.ReadMultiple * testResult.ReadThreadCount;
        }

        internal void Finish()
        {
            Console.WriteLine($"----- Summary: upserts {TestResult.UpsertCount}, data {CacheGlobals.DataSize}, readMult {TestResult.ReadMultiple}," + 
                              $" readThreads {TestResult.ReadThreadCount}, useVarLenVal {TestResult.UseVarLenValue}, useObjVal {TestResult.UseObjectValue}," + 
                              $" readCache {TestResult.UseReadCache}, log.{TestResult.LogMode}, iterations {currentIter}");
            
            var numSec = totalUpsertMs / (TestResult.IterationCount * 1000.0);
            this.TestResult.UpsertsPerSecond = TestResult.UpsertCount / numSec;
            Console.WriteLine("Average time to upsert {0} elements: {1:0.000} secs ({2:0.00} inserts/sec); working set {3}MB",
                              TestResult.UpsertCount, numSec, this.TestResult.UpsertsPerSecond, totalWorkingSetMB / (ulong)TestResult.IterationCount);

            numSec = totalReadMs / (TestResult.IterationCount * 1000.0);
            this.TestResult.ReadsPerSecond = numReadsPerIteration / numSec;
            this.TestResult.PendingReadsPerIteration = (double)totalReadsPending / TestResult.IterationCount;
            this.TestResult.PendingReadsPerIterationPercent = (this.TestResult.PendingReadsPerIteration / numReadsPerIteration) * 100;
            Console.WriteLine("Average time to read {0} elements: {1:0.000} secs ({2:0.00} reads/sec); PENDING {3:0.00} ({4:0.00}%)",
                              numReadsPerIteration, numSec, numReadsPerIteration / numSec, this.TestResult.PendingReadsPerIteration,
                              (double)totalReadsPending / (numReadsPerIteration * TestResult.IterationCount) * 100);
            Console.WriteLine();
        }
    }
}
