// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace FASTER.PerfTest
{
    [JsonObject(MemberSerialization.OptIn)]
    class TestResult
    {
        // Inputs
        [JsonProperty]
        public int UpsertCount { get; set; } = 1000000;

        [JsonProperty]
        public int DataSize { get; set; } = CacheGlobals.MinDataSize;

        [JsonProperty]
        public int ReadMultiple { get; set; } = 1;

        [JsonProperty]
        public int ReadThreadCount { get; set; } = 1;

        [JsonProperty]
        public bool UseVarLenValue { get; set; } = false;

        [JsonProperty]
        public bool UseObjectValue { get; set; } = false;

        [JsonProperty]
        public bool UseReadCache { get; set; } = true;

        [JsonProperty]
        public LogMode LogMode { get; set; } = LogMode.Flush;

        [JsonProperty]
        public int IterationCount { get; set; } = 1;

        public bool Verify()
        {
            bool fail(string message)
            {
                Console.WriteLine(message);
                return false;
            }
            if (UpsertCount < 0)
                return fail($"Invalid {nameof(UpsertCount)}: {UpsertCount}. Must be > 0");
            if (DataSize < CacheGlobals.MinDataSize)
                return fail($"Invalid {nameof(DataSize)}: {DataSize}. Must be > {CacheGlobals.MinDataSize}");
            if (ReadMultiple < 0)
                return fail($"Invalid {nameof(ReadMultiple)}: {ReadMultiple}. Must be > 0");
            if (ReadThreadCount < 0)
                return fail($"Invalid {nameof(ReadThreadCount)}: {ReadThreadCount}. Must be > 0");
            if (IterationCount < 0)
                return fail($"Invalid {nameof(IterationCount)}: {IterationCount}. Must be > 0");

            if (this.UseVarLenValue && this.UseObjectValue)
                return fail($"Cannot specify both {nameof(this.UseVarLenValue)} and {nameof(this.UseObjectValue)}");
            if (!this.UseObjectValue && !CacheGlobals.BlittableDataSizes.Contains(this.DataSize))
                return fail($"Fixed-length blittable data values ({nameof(UseVarLenValue)} and {nameof(UseObjectValue)} are false) require a data size in [{string.Join(", ", CacheGlobals.BlittableDataSizes)}]");

            return true;
        }

        // Outputs
        [JsonProperty]
        [JsonConverter(typeof(DoubleRoundingConverter))]
        public double UpsertsPerSecond { get; set; }

        [JsonProperty]
        [JsonConverter(typeof(DoubleRoundingConverter))]
        public double ReadsPerSecond { get; set; }

        [JsonProperty]
        [JsonConverter(typeof(DoubleRoundingConverter))]
        public double PendingReadsPerIteration { get; set; }

        [JsonProperty]
        [JsonConverter(typeof(DoubleRoundingConverter))]
        public double PendingReadsPerIterationPercent { get; set; }

        internal void Write(string filename) => File.WriteAllText(filename, JsonConvert.SerializeObject(this, PerfTest.jsonSerializerSettings));

        internal static TestResult Read(string filename) => JsonConvert.DeserializeObject<TestResult>(File.ReadAllText(filename));

        public override string ToString() => MemberTuple.ToString();

        (int, int, int, int, int, bool, bool, bool, LogMode) MemberTuple
            => (this.UpsertCount, this.DataSize, this.ReadMultiple, this.ReadThreadCount, this.IterationCount, 
                this.UseVarLenValue, this.UseObjectValue, this.UseReadCache, this.LogMode);

        public override int GetHashCode() => this.MemberTuple.GetHashCode();

        public override bool Equals(object obj) => this.Equals(obj as TestResult);

        public bool Equals(TestResult other) => other is null ? false : this.MemberTuple == other.MemberTuple;
    }

    [JsonObject(MemberSerialization.OptIn)]
    class TestResults
    {
        [JsonProperty]
        public TestResult[] Results { get; set; }

        internal void Add(TestResult result) 
            => this.Results = this.Results is null ? new[] { result } : this.Results.Concat(new[] { result }).ToArray();

        internal void Write(string filename) => File.WriteAllText(filename, JsonConvert.SerializeObject(this, PerfTest.jsonSerializerSettings));

        internal static TestResults Read(string filename) => JsonConvert.DeserializeObject<TestResults>(File.ReadAllText(filename));

        internal TestResultComparisons CompareExact(TestResults other)
        {
            // Compare only TestResults with matching parameters.
            var thisResults = new HashSet<TestResult>(this.Results);

            IEnumerable<(TestResult, TestResult)> zipMatchingResults()
            {
                foreach (var otherResult in other.Results)
                {
                    if (thisResults.TryGetValue(otherResult, out TestResult thisResult))
                        yield return (thisResult, otherResult);
                }
            }

            return CompareSequence(zipMatchingResults());
        }

        internal TestResultComparisons CompareSequence(TestResults other) 
            => CompareSequence(Enumerable.Range(0, Math.Min(this.Results.Length, other.Results.Length)).Select(ii => (this.Results[ii], other.Results[ii])));

        internal static TestResultComparisons CompareSequence(IEnumerable<(TestResult, TestResult)> results) 
            => new TestResultComparisons { Comparisons = results.Select(result => new TestResultComparison(result.Item1, result.Item2)).ToArray() };
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
            => writer.WriteValue(Math.Round((double)value, Precision));
    }
}
