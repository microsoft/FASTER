// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using Newtonsoft.Json;
using System;
using System.Linq;
using System.Reflection.Metadata.Ecma335;

namespace Performance.Common
{
    [JsonObject(MemberSerialization.OptIn)]
    internal class ResultStats
    {
        [JsonProperty]
        [JsonConverter(typeof(DoubleRoundingConverter))]
        internal double[] OperationsPerSecond { get; set; } = new double[0];

        [JsonProperty]
        [JsonConverter(typeof(DoubleRoundingConverter))]
        internal double Mean;

        [JsonProperty]
        [JsonConverter(typeof(DoubleRoundingConverter))]
        internal double StdDev;

        internal static ResultStats Create(long opsPerIter, double[] opsPerSecond)
        {
            // opsPerIter allows a true overall mean, rather than an average of averages
            var stats = new ResultStats { OperationsPerSecond = opsPerSecond };
            var totalSec = stats.OperationsPerSecond.Length == 0
                ? 0.0
                : stats.OperationsPerSecond.Sum(opsPerSec => opsPerSec == 0 ? 0.0 : opsPerIter / opsPerSec);
            stats.Mean = totalSec == 0
                ? 0.0
                : (opsPerIter * opsPerSecond.Length) / totalSec;
            stats.StdDev = totalSec == 0 || stats.OperationsPerSecond.Length <= 1
                ? 0.0
                : Math.Sqrt(stats.OperationsPerSecond.Sum(n => Math.Pow(n - stats.Mean, 2)) / stats.OperationsPerSecond.Length);
            return stats;
        }

        internal static ResultStats Create(ulong[] runMs, long[] runOps)
        {
            var runSec = runMs.Select(ms => ms / 1000.0).ToArray();
            var opsPerSecond = Enumerable.Range(0, runMs.Length).Select(ii => runSec[ii] == 0 ? 0.0 : runOps[ii] / runSec[ii]).ToArray();
            var stats = new ResultStats { OperationsPerSecond = opsPerSecond };
            var runSecSum = runSec.Sum();
            stats.Mean = runSecSum == 0
                ? 0.0
                : runOps.Sum() / runSecSum;
            stats.StdDev = runSecSum == 0 || stats.OperationsPerSecond.Length <= 1
                ? 0.0
                : Math.Sqrt(stats.OperationsPerSecond.Sum(n => Math.Pow(n - stats.Mean, 2)) / stats.OperationsPerSecond.Length);
            return stats;
        }

        private static (int, int) GetMinMaxIndexes(ulong[] runMs)
        {
            // Get the index of the highest and lowest values to trim. 
            // There may be ties so we can't just delete at min/max value.
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
            return (minIndex, maxIndex);
        }

        // Initial inserts split the keyCount among the threads; all other operations execute the full count on each thread.
        // opsPerIter allows a true overall mean, rather than an average of averages
        private static ResultStats CalcPerSecondStatsFull(int threadCount, long opsPerIter,
                                                          ulong[] runMs, long opCount, bool isInit)
            => Create(opsPerIter, 
                      runMs.Select(ms => ms == 0 ? 0.0 : (opCount * (isInit ? 1 : threadCount)) / (ms / 1000.0)).ToArray());

        private static ResultStats CalcPerSecondStatsTrimmed(int threadCount, long opsPerIter,
                                                             ulong[] runMs, long opCount, bool isInit)
        {
            if (runMs.Length < 3)
                return CalcPerSecondStatsFull(threadCount, opsPerIter, runMs, opCount, isInit);

            var (minIndex, maxIndex) = GetMinMaxIndexes(runMs);
            var trimmedMs = runMs.Where((_, ii) => ii != maxIndex && ii != minIndex).ToArray();
            return CalcPerSecondStatsFull(threadCount, opsPerIter, trimmedMs, opCount, isInit);
        }

        private static ResultStats CalcPerThreadStats(int threadCount, long opsPerIter, ResultStats allThreads)
            => Create(opsPerIter, allThreads.OperationsPerSecond.Select(n => n / threadCount).ToArray());

        internal static void Calc(OperationResults opResults, int threadCount, long opsPerIter,
                                  ulong[] runMs, long opCount, bool isInit = false)
        {
            opResults.AllThreadsFull = CalcPerSecondStatsFull(threadCount, opsPerIter, runMs, opCount, isInit);
            opResults.PerThreadFull = CalcPerThreadStats(threadCount, opsPerIter, opResults.AllThreadsFull);
            opResults.AllThreadsTrimmed = CalcPerSecondStatsTrimmed(threadCount, opsPerIter, runMs, opCount, isInit);
            opResults.PerThreadTrimmed = CalcPerThreadStats(threadCount, opsPerIter, opResults.AllThreadsTrimmed);
        }

        // For tests that run for a given period of time rather than for a specific number of operations.
        internal static ResultStats CalcPerSecondStatsFull(ulong[] runMs, long[] runOps)
            => Create(runMs, runOps);

        private static (ResultStats, ulong[], long[]) CalcPerSecondStatsTrimmed(ulong[] runMs, long[] runOps)
        {
            if (runMs.Length < 3)
                return (CalcPerSecondStatsFull(runMs, runOps), runMs, runOps);

            var (minIndex, maxIndex) = GetMinMaxIndexes(runMs);
            var trimmedMs = runMs.Where((_, ii) => ii != maxIndex && ii != minIndex).ToArray();
            var trimmedOps = runOps.Where((_, ii) => ii != maxIndex && ii != minIndex).ToArray();
            return (CalcPerSecondStatsFull(trimmedMs, trimmedOps), trimmedMs, trimmedOps);
        }

        internal static ResultStats CalcPerThreadStats(int threadCount, ulong[] runMs, long[] runOps)
            => Create(runMs.Select(ms => ms / (ulong)threadCount).ToArray(), runOps.Select(ops => ops / threadCount).ToArray());

        internal static (ulong[], long[]) Calc(OperationResults opResults, int threadCount, ulong[] runMs, long[] runOps)
        {
            opResults.AllThreadsFull = CalcPerSecondStatsFull(runMs, runOps);
            opResults.PerThreadFull = CalcPerThreadStats(threadCount, runMs, runOps);
            var (trimmedResult, trimmedMs, trimmedOps) = CalcPerSecondStatsTrimmed(runMs, runOps);
            opResults.AllThreadsTrimmed = trimmedResult;
            opResults.PerThreadTrimmed = CalcPerThreadStats(threadCount, trimmedMs, trimmedOps);
            return (trimmedMs, trimmedOps);
        }
    }
}
