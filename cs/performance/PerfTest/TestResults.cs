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
        public int HashSizeShift { get; set; } = Globals.DefaultHashSizeShift;

        [JsonProperty]
        public NumaMode NumaMode { get; set; } = NumaMode.None;

        [JsonProperty]
        public Distribution Distribution { get; set; } = Distribution.Uniform;

        [JsonProperty]
        public double DistributionParameter { get; set; } = Globals.DefaultDistributionParameter;

        [JsonProperty]
        public int DistributionSeed { get; set; } = Globals.DefaultDistributionSeed;

        [JsonProperty]
        public int ThreadCount { get; set; } = 1;

        [JsonProperty]
        public int InitKeyCount { get; set; } = Globals.DefaultInitKeyCount;

        [JsonProperty]
        public int OperationKeyCount { get; set; } = Globals.DefaultOpKeyCount;

        [JsonProperty]
        public int UpsertCount { get; set; } = Globals.DefaultOpCount;

        [JsonProperty]
        public int ReadCount { get; set; } = Globals.DefaultOpCount;

        [JsonProperty]
        public int RMWCount { get; set; } = Globals.DefaultOpCount;

        [JsonProperty]
        public bool MixOperations { get; set; } = false;

        [JsonProperty]
        public int DataSize { get; set; } = Globals.MinDataSize;

        [JsonProperty]
        public bool UseVarLenValue { get; set; } = false;

        [JsonProperty]
        public bool UseObjectValue { get; set; } = false;

        [JsonProperty]
        public bool UseReadCache { get; set; } = true;

        [JsonProperty]
        public LogMode LogMode { get; set; } = LogMode.None;

        [JsonProperty]
        public int IterationCount { get; set; } = 1;

        public bool Verify(bool isFileInput = false)
        {
            static bool fail(string message)
            {
                Console.WriteLine(message);
                return false;
            }
            if (HashSizeShift < 10)
                return fail($"Invalid {nameof(HashSizeShift)}: {HashSizeShift}. Must be >= 10");
            if (DistributionParameter <= 0.0 || DistributionParameter == 1.0)
                return fail($"Invalid {nameof(DistributionParameter)}: {DistributionParameter}. Must be > 0.0 and != 1.0");
            if (DistributionSeed <= 0.0)
                return fail($"Invalid {nameof(DistributionSeed)}: {DistributionSeed}. Must be > 0.0");
            if (ThreadCount <= 0)
                return fail($"Invalid {nameof(ThreadCount)}: {ThreadCount}. Must be > 0");
            if (InitKeyCount <= 0 || InitKeyCount % Globals.ChunkSize != 0)
                return fail($"Invalid {nameof(InitKeyCount)}: {InitKeyCount}. Must be > 0 and a multiple of {Globals.ChunkSize}");
            if (OperationKeyCount <= 0 || OperationKeyCount % Globals.ChunkSize != 0)
                return fail($"Invalid {nameof(OperationKeyCount)}: {OperationKeyCount}. Must be > 0 and a multiple of {Globals.ChunkSize}");
            if (UpsertCount < 0 || UpsertCount % Globals.ChunkSize != 0)
                return fail($"Invalid {nameof(UpsertCount)}: {UpsertCount}. Must be > 0 and a multiple of {Globals.ChunkSize}");
            if (ReadCount < 0 || ReadCount % Globals.ChunkSize != 0)
                return fail($"Invalid {nameof(ReadCount)}: {ReadCount}. Must be > 0 and a multiple of {Globals.ChunkSize}");
            if (RMWCount < 0 || RMWCount % Globals.ChunkSize != 0)
                return fail($"Invalid {nameof(RMWCount)}: {RMWCount}. Must be > 0 and a multiple of {Globals.ChunkSize}");
            if (!isFileInput && UpsertCount == 0 && ReadCount == 0 && RMWCount == 0)
                return fail($"Invalid operations: At least one operation must be specified");
            if (MixOperations && (UpsertCount == TotalOpCount || ReadCount == TotalOpCount || RMWCount == TotalOpCount))
                return fail($"Invalid {nameof(MixOperations)}: More than one operation must be specified");
            if (DataSize < Globals.MinDataSize)
                return fail($"Invalid {nameof(DataSize)}: {DataSize}. Must be >= {Globals.MinDataSize}");
            if (IterationCount < 1)
                return fail($"Invalid {nameof(IterationCount)}: {IterationCount}. Must be >= 1");

            if (this.UseVarLenValue && this.UseObjectValue)
                return fail($"Cannot specify both {nameof(this.UseVarLenValue)} and {nameof(this.UseObjectValue)}");
            if (!Globals.ValidDataSizes.Contains(this.DataSize))
                return fail($"Data sizes must be in [{string.Join(", ", Globals.ValidDataSizes)}]");

            return true;
        }

        // Outputs
        [JsonProperty]
        [JsonConverter(typeof(DoubleRoundingConverter))]
        public double InitialInsertsPerSecond { get; set; }

        [JsonProperty]
        [JsonConverter(typeof(DoubleRoundingConverter))]
        public double InitialInsertsPerSecondPerThread { get; set; }

        [JsonProperty]
        [JsonConverter(typeof(DoubleRoundingConverter))]
        public double TotalOperationsPerSecond { get; set; }

        [JsonProperty]
        [JsonConverter(typeof(DoubleRoundingConverter))]
        public double TotalOperationsPerSecondPerThread { get; set; }

        [JsonProperty]
        [JsonConverter(typeof(DoubleRoundingConverter))]
        public double UpsertsPerSecond { get; set; }

        [JsonProperty]
        [JsonConverter(typeof(DoubleRoundingConverter))]
        public double UpsertsPerSecondPerThread { get; set; }

        [JsonProperty]
        [JsonConverter(typeof(DoubleRoundingConverter))]
        public double ReadsPerSecond { get; set; }

        [JsonProperty]
        [JsonConverter(typeof(DoubleRoundingConverter))]
        public double ReadsPerSecondPerThread { get; set; }

        [JsonProperty]
        [JsonConverter(typeof(DoubleRoundingConverter))]
        public double RMWsPerSecond { get; set; }

        [JsonProperty]
        [JsonConverter(typeof(DoubleRoundingConverter))]
        public double RMWsPerSecondPerThread { get; set; }

        internal void Write(string filename) 
            => File.WriteAllText(filename, JsonConvert.SerializeObject(this, Globals.jsonSerializerSettings));

        internal static TestResult Read(string filename) 
            => JsonConvert.DeserializeObject<TestResult>(File.ReadAllText(filename));

        public override string ToString()
            => $" {PerfTest.HashSizeArg} {this.HashSizeShift}" +
               $" {PerfTest.NumaArg} {this.NumaMode}" +
               $" {PerfTest.DistArg} {this.Distribution}" +
               $" {PerfTest.DistParamArg} {this.DistributionParameter}" +
               $" {PerfTest.HashSizeArg} {this.DistributionSeed}" +
               $" {PerfTest.ThreadsArg} {this.ThreadCount}" +
               $" {PerfTest.InitKeysArg} {this.InitKeyCount}" +
               $" {PerfTest.OpKeysArg} {this.OperationKeyCount}" +
               $" {PerfTest.UpsertsArg} {this.UpsertCount}" +
               $" {PerfTest.ReadsArg} {this.ReadCount}" +
               $" {PerfTest.RMWsArg} {this.RMWCount}" +
               $" {PerfTest.MixOpsArg} {this.MixOperations}" +
               $" {PerfTest.DataArg} {this.DataSize}" +
               $" {PerfTest.UseVarLenArg} {this.UseVarLenValue}" +
               $" {PerfTest.UseObjArg} {this.UseObjectValue}" +
               $" {PerfTest.NoRcArg} {this.UseReadCache}" +
               $" {PerfTest.LogArg} {this.LogMode}" +
               $" {PerfTest.ItersArg} {this.IterationCount}"
            ;

        (int, NumaMode, Distribution, double, int, int, int, int, int, int, int, bool, int, bool, bool, bool, LogMode, int) MemberTuple
            => (this.HashSizeShift, this.NumaMode,
                this.Distribution, this.DistributionParameter, this.DistributionSeed, this.ThreadCount,
                this.InitKeyCount, this.OperationKeyCount, this.UpsertCount, this.ReadCount, this.RMWCount, this.MixOperations,
                this.DataSize, this.UseVarLenValue, this.UseObjectValue, this.UseReadCache, this.LogMode, this.IterationCount);

        internal int TotalOpCount => this.UpsertCount + this.ReadCount + this.RMWCount;

        internal (Distribution, double, double) DistributionInfo 
            => (this.Distribution, this.DistributionParameter, this.DistributionSeed);

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

        internal void Write(string filename) 
            => File.WriteAllText(filename, JsonConvert.SerializeObject(this, Globals.jsonSerializerSettings));

        internal static TestResults Read(string filename) 
            => JsonConvert.DeserializeObject<TestResults>(File.ReadAllText(filename));

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
