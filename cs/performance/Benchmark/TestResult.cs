// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using Newtonsoft.Json;
using Performance.Common;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace FASTER.Benchmark
{
    [JsonObject(MemberSerialization.OptIn)]
    internal partial class TestResult
    {
        [JsonProperty]
        internal TestInputs Inputs { get; set; } = new TestInputs();

        [JsonProperty]
        internal TestOutputs Outputs { get; set; } = new TestOutputs();

        // Override equivalence testing
        public override int GetHashCode() => this.Inputs.MemberTuple.GetHashCode();

        public override bool Equals(object obj) => this.Equals(obj as TestResult);

        public bool Equals(TestResult other) => other is null ? false : this.Inputs.MemberTuple == other.Inputs.MemberTuple;

        // Unlike PerfTest, Benchmark has only a single TestResult, not a collection of TestResults,
        // so we merge directly into the TestResult object.

        internal void Write(string filename)
            => JsonUtils.WriteAllText(filename, JsonConvert.SerializeObject(this, JsonUtils.OutputSerializerSettings));

        internal static TestResult Read(string filename)
            => JsonConvert.DeserializeObject<TestResult>(File.ReadAllText(filename), JsonUtils.InputSerializerSettings);

        internal static void Merge(string[] fileSpecs, string resultsFilename)
        {
            static IEnumerable<string> enumFiles(string fileSpec)
                => Directory.EnumerateFiles(Path.GetDirectoryName(fileSpec), Path.GetFileName(fileSpec));

            var filenames = fileSpecs.SelectMany(spec => enumFiles(spec)).ToArray();
            if (filenames.Length < 2)
            {
                Console.WriteLine("Merge file specification did not evaluate to multiple files");
                return;
            }

            Console.WriteLine($"Merging results files");
            Console.WriteLine($"Initial file: {filenames[0]}");
            var mergedResult = Read(filenames[0]);
            int mismatches = 0;
            for (var ii = 1; ii < filenames.Length; ++ii)
            {
                var filename = filenames[ii];
                var other = Read(filename);
                if (!other.Equals(mergedResult))    // Compares Inputs only
                {
                    Console.WriteLine($"Skipping mismatched file: {filename}");
                    ++mismatches;
                    continue;
                }
                Console.WriteLine($"Merging file: {filename}");
                mergedResult = mergedResult.Merge(other);
            }

            Console.WriteLine($"Merge complete: matched {filenames.Length - mismatches}, {mismatches} mismatches");
            mergedResult.Write(resultsFilename);
        }

        internal TestResult Merge(TestResult other)
        {
            var merged = new TestResult() { Inputs = this.Inputs };

            void mergeInitialInserts()
            {
                ResultStats mergeStats(ResultStats first, ResultStats second)
                    => ResultStats.Create(this.Inputs.InitKeyCount,
                                          (first.OperationsPerSecond.Length, second.OperationsPerSecond.Length) switch
                                          {
                                              (0, _) => second.OperationsPerSecond,
                                              (_, 0) => first.OperationsPerSecond,
                                              _ => first.OperationsPerSecond.Concat(second.OperationsPerSecond).ToArray()
                                          });

                var firstOpResults = this.Outputs.InitialInserts;
                var secondOpResults = other.Outputs.InitialInserts;
                var mergedOpResults = merged.Outputs.InitialInserts;
                mergedOpResults.AllThreadsFull = mergeStats(firstOpResults.AllThreadsFull, secondOpResults.AllThreadsFull);
                mergedOpResults.AllThreadsTrimmed = mergeStats(firstOpResults.AllThreadsTrimmed, secondOpResults.AllThreadsTrimmed);
                mergedOpResults.PerThreadFull = mergeStats(firstOpResults.PerThreadFull, secondOpResults.PerThreadFull);
                mergedOpResults.PerThreadTrimmed = mergeStats(firstOpResults.PerThreadTrimmed, secondOpResults.PerThreadTrimmed);
            }

            void mergeTransactions()
            {
                merged.Outputs.Counts.TransactionSecondsFull = this.Outputs.Counts.TransactionSecondsFull.Concat(other.Outputs.Counts.TransactionSecondsFull).ToArray();
                merged.Outputs.Counts.TransactionCountsFull = this.Outputs.Counts.TransactionCountsFull.Concat(other.Outputs.Counts.TransactionCountsFull).ToArray();
                merged.Outputs.Counts.TransactionSecondsTrimmed = this.Outputs.Counts.TransactionSecondsTrimmed.Concat(other.Outputs.Counts.TransactionSecondsTrimmed).ToArray();
                merged.Outputs.Counts.TransactionCountsTrimmed = this.Outputs.Counts.TransactionCountsTrimmed.Concat(other.Outputs.Counts.TransactionCountsTrimmed).ToArray();

                // Convert back to ms for ResultStats calculation
                static ulong[] toMs(double[] seconds) => seconds.Select(sec => (ulong)(sec * 1000)).ToArray();
                var fullMs = toMs(merged.Outputs.Counts.TransactionSecondsFull);
                var trimmedMs = toMs(merged.Outputs.Counts.TransactionSecondsTrimmed);

                var opResults = this.Outputs.Transactions;
                var threadCount = this.Inputs.ThreadCount;

                opResults.AllThreadsFull = ResultStats.CalcPerSecondStatsFull(fullMs, merged.Outputs.Counts.TransactionCountsFull);
                opResults.PerThreadFull = ResultStats.CalcPerThreadStats(threadCount, fullMs, opResults.AllThreadsFull);
                opResults.AllThreadsTrimmed = ResultStats.CalcPerSecondStatsFull(trimmedMs, merged.Outputs.Counts.TransactionCountsTrimmed);
                opResults.PerThreadTrimmed = ResultStats.CalcPerThreadStats(threadCount, trimmedMs, opResults.AllThreadsTrimmed);
            }

            mergeInitialInserts();
            mergeTransactions();
            return merged;
        }
    }
}
