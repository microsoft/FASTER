// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace FASTER.PerfTest
{
    using ResultStats = TestOutputs.ResultStats;
    using OperationResults = TestOutputs.OperationResults;

    [JsonObject(MemberSerialization.OptIn)]
    internal partial class TestResult
    {
        [JsonProperty]
        internal TestInputs Inputs { get; set; } = new TestInputs();

        [JsonProperty]
        internal TestOutputs Outputs { get; set; } = new TestOutputs();

        public override int GetHashCode() => this.Inputs.MemberTuple.GetHashCode();

        public override bool Equals(object obj) => this.Equals(obj as TestResult);

        public bool Equals(TestResult other) => other is null ? false : this.Inputs.MemberTuple == other.Inputs.MemberTuple;
    }

    [JsonObject(MemberSerialization.OptIn)]
    class TestResults
    {
        [JsonProperty]
        public TestResult[] Results { get; set; }

        internal void Add(TestResult result)
            => this.Results = this.Results is null ? new[] { result } : this.Results.Concat(new[] { result }).ToArray();

        internal void Write(string filename) 
            => File.WriteAllText(filename, JsonConvert.SerializeObject(this, Globals.outputJsonSerializerSettings));

        internal static TestResults Read(string filename) 
            => JsonConvert.DeserializeObject<TestResults>(File.ReadAllText(filename), Globals.inputJsonSerializerSettings);

        internal (IEnumerable<(TestResult, TestResult)>, IEnumerable<TestResult>, IEnumerable<TestResult>) Match(TestResults other)
        {
            var thisResults = new HashSet<TestResult>(this.Results);
            var otherResults = new HashSet<TestResult>(other.Results);

            IEnumerable<(TestResult, TestResult)> zipMatchingResults()
            {
                foreach (var otherResult in other.Results)
                {
                    if (thisResults.TryGetValue(otherResult, out TestResult thisResult))
                        yield return (thisResult, otherResult);
                }
            }
            return (zipMatchingResults(), thisResults.Except(otherResults), otherResults.Except(this.Results));
        }

        internal TestResultComparisons CompareExact(TestResults other)
            => CompareSequence(Match(other).Item1);        // Compare only TestResults with matching parameters.

        internal TestResultComparisons CompareSequence(TestResults other) 
            => CompareSequence(Enumerable.Range(0, Math.Min(this.Results.Length, other.Results.Length)).Select(ii => (this.Results[ii], other.Results[ii])));

        internal static TestResultComparisons CompareSequence(IEnumerable<(TestResult, TestResult)> results) 
            => new TestResultComparisons { ResultComparisons = results.Select(result => new TestResultComparison(result.Item1, result.Item2)).ToArray() };

        internal static void Merge(string[] filespecs, string resultsFilename)
        {
            static IEnumerable<string> enumFiles(string filespec) 
                => Directory.EnumerateFiles(Path.GetDirectoryName(filespec), Path.GetFileName(filespec));

            var filenames = filespecs.SelectMany(spec => enumFiles(spec)).ToArray();
            if (filenames.Length < 2)
            {
                Console.WriteLine($"{PerfTest.MergeResultsArg} file specification did not evaluate to multiple files");
                return;
            }

            Console.WriteLine($"Merging results files");
            Console.WriteLine($"Initial file: {filenames[0]}");
            var mergedResults = Read(filenames[0]);
            for (var ii = 1; ii < filenames.Length; ++ii)
            {
                Console.WriteLine($"Merging file: {filenames[ii]}");
                mergedResults = mergedResults.Merge(Read(filenames[ii]));
            }
            mergedResults.Write(resultsFilename);
        }

        internal TestResults Merge(TestResults other)
        {
            var (matchPairs, thisOnly, otherOnly) = Match(other);

            var results = new List<TestResult>();

            static ResultStats mergeStats(ResultStats first, ResultStats second) 
                => ResultStats.Create((first.OperationsPerSecond.Length, second.OperationsPerSecond.Length) switch
                                       {
                                           (0, _) => second.OperationsPerSecond,
                                           (_, 0) => first.OperationsPerSecond,
                                           _      => first.OperationsPerSecond.Concat(second.OperationsPerSecond).ToArray()
                                       });

            foreach (var (first, second) in matchPairs)
            {
                var merged = new TestResult() { Inputs = first.Inputs };

                void merge(Func<TestOutputs, OperationResults> opResultsSelector)
                {
                    var firstOpResults = opResultsSelector(first.Outputs);
                    var secondOpResults = opResultsSelector(second.Outputs);
                    var mergedOpResults = opResultsSelector(merged.Outputs);
                    mergedOpResults.AllThreadsFull = mergeStats(firstOpResults.AllThreadsFull, secondOpResults.AllThreadsFull);
                    mergedOpResults.PerThreadFull = mergeStats(firstOpResults.PerThreadFull, secondOpResults.PerThreadFull);
                }

                merge(outputs => outputs.InitialInserts);
                merge(outputs => outputs.TotalOperations);
                merge(outputs => outputs.Upserts);
                merge(outputs => outputs.Reads);
                merge(outputs => outputs.RMWs);
                results.Add(merged);
            }

            var matchCount = results.Count;
            results.AddRange(thisOnly);
            var thisOnlyCount = results.Count - matchCount;
            results.AddRange(otherOnly);
            var otherOnlyCount = results.Count - thisOnlyCount - matchCount;

            // Report how many on either side did not match.
            Console.WriteLine($"Merge complete: matched {matchCount}, first file unmatched {thisOnlyCount}," +
                              $" second file unmatched {otherOnlyCount}");

            return new TestResults { Results = results.ToArray() };
        }
    }

    public class DoubleRoundingConverter : JsonConverter
    {
        private const int Precision = 2;

        public DoubleRoundingConverter() { }

        public override bool CanRead => false;

        public override bool CanWrite => true;

        public override bool CanConvert(Type propertyType) => propertyType == typeof(double);

        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
            => throw new NotImplementedException();

        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            if (value is double[] vector)
            {
                writer.WriteStartArray();
                foreach (var d in vector)
                    writer.WriteValue(Math.Round(d, Precision));
                writer.WriteEndArray();
                return;
            }
            writer.WriteValue(Math.Round((double)value, Precision));
        }
    }
}
