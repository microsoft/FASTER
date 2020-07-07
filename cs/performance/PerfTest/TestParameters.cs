// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using Newtonsoft.Json;
using Performance.Common;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;

namespace FASTER.PerfTest
{
    // Used to override TestParameter values with command-line arguments, to reduce
    // proliferation/duplication of test files.
    [Flags] enum TestParameterFlags : ulong
    {
        None =                                0x0000_0000,
        HashSize =                            0x0000_0001,
        NumaMode =                            0x0000_0002,
        Distribution =                        0x0000_0004,
        DistributionParameter =               0x0000_0008,
        DistributionSeed =                    0x0000_0010,
        ThreadCount =                         0x0000_0020,
        InitKeyCount =                        0x0000_0040,
        OpKeyCount =                          0x0000_0080,
        UpsertCount =                         0x0000_0100,
        ReadCount =                           0x0000_0200,
        RmwCount =                            0x0000_0400,
        MixOperations =                       0x0000_0800,
        KeySize =                             0x0000_1000,
        ValueSize =                           0x0000_2000,
        UseVarLenKey =                        0x0000_4000,
        UseVarLenValue =                      0x0000_8000,
        UseObjectKey =                        0x0001_0000,
        UseObjectValue =                      0x0002_0000,
        UseReadCache =                        0x0004_0000,
        ThreadMode =                          0x0008_0000,
        AsyncReadBatchSize =                  0x0010_0000,
        CheckpointMode =                      0x0020_0000,
        CheckpointMs =                        0x0040_0000,
        LogMode =                             0x0080_0000,
        LogPageSizeBits =                     0x0100_0000,
        LogSegmentSizeBits =                  0x0200_0000,
        LogMemorySizeBits =                   0x0400_0000,
        LogMutableFraction =                  0x0800_0000,
        LogCopyReadsToTail =                  0x1000_0000,
        ReadCachePageSizeBits =          0x0000_2000_0000,
        ReadCacheMemorySizeBits =        0x0000_4000_0000,
        ReadCacheSecondChanceFraction =  0x0000_8000_0000,
        IterationCount =                 0x0001_0000_0000
    }

    [JsonObject(MemberSerialization.OptIn)]
    public class TestParameterSweep
    {
        [JsonProperty]
        public int[] HashSizeShifts { get; set; } = new[] { Globals.DefaultHashSizeShift };

        [JsonProperty]
        public NumaMode[] NumaModes { get; set; } = new[] { Globals.DefaultNumaMode };

        [JsonProperty]
        public Distribution[] Distributions { get; set; } = new[] { Globals.DefaultDistribution };

        [JsonProperty]
        public double[] DistributionParameters { get; set; } = new[] { Globals.DefaultDistributionParameter };

        [JsonProperty]
        public uint DistributionSeed { get; set; } = RandomGenerator.DefaultDistributionSeed;

        [JsonProperty]
        public int[] ThreadCounts { get; set; } = new[] { 1 };

        [JsonProperty]
        public int[] InitKeyCounts { get; set; } = new[] { Globals.DefaultInitKeyCount };

        [JsonProperty]
        public int[] OperationKeyCounts { get; set; } = new[] { Globals.DefaultOpKeyCount };

        [JsonProperty]
        public int[] UpsertCounts { get; set; } = new[] { Globals.DefaultOpCount };

        [JsonProperty]
        public int[] ReadCounts { get; set; } = new[] { Globals.DefaultOpCount };

        [JsonProperty]
        public int[] RMWCounts { get; set; } = new[] { Globals.DefaultOpCount };

        [JsonProperty]
        public bool[] MixOperations { get; set; } = new[] { false };

        [JsonProperty]
        public int[] KeySizes { get; set; } = new[] { Globals.MinDataSize };

        [JsonProperty]
        public int[] ValueSizes { get; set; } = new[] { Globals.MinDataSize };

        [JsonProperty]
        public bool[] UseVarLenKeys { get; set; } = new[] { false };

        [JsonProperty]
        public bool[] UseVarLenValues { get; set; } = new[] { false };

        [JsonProperty]
        public bool[] UseObjectKeys { get; set; } = new[] { false };

        [JsonProperty]
        public bool[] UseObjectValues { get; set; } = new[] { false };

        [JsonProperty]
        public bool[] UseReadCaches { get; set; } = new[] { true };

        [JsonProperty]
        public ThreadMode[] ThreadModes { get; set; } = new[] { Globals.DefaultThreadMode };

        [JsonProperty]
        public int[] AsyncReadBatchSizes { get; set; } = new[] { Globals.DefaultAsyncReadBatchSize };

        [JsonProperty]
        public Checkpoint.Mode[] CheckpointModes { get; set; } = new[] { Checkpoint.Mode.None };

        [JsonProperty]
        public int[] CheckpointMs { get; set; } = new[] { Globals.DefaultCheckpointMs };

        [JsonProperty]
        public LogMode[] LogModes { get; set; } = new[] { LogMode.None };

        [JsonProperty]
        public int[] LogPageSizeBits { get; set; } = new[] { Globals.DefaultLogSettings.PageSizeBits };

        [JsonProperty]
        public int[] LogSegmentSizeBits { get; set; } = new[] { Globals.DefaultLogSettings.SegmentSizeBits };

        [JsonProperty]
        public int[] LogMemorySizeBits { get; set; } = new[] { Globals.DefaultLogSettings.MemorySizeBits };

        [JsonProperty]
        public double[] LogMutableFractions { get; set; } = new[] { Globals.DefaultLogSettings.MutableFraction };

        [JsonProperty]
        public bool[] LogCopyReadsToTails { get; set; } = new[] { Globals.DefaultLogSettings.CopyReadsToTail };

        [JsonProperty]
        public int[] ReadCachePageSizeBits { get; set; } = new[] { Globals.DefaultLogSettings.ReadCacheSettings.PageSizeBits };

        [JsonProperty]
        public int[] ReadCacheMemorySizeBits { get; set; } = new[] { Globals.DefaultLogSettings.ReadCacheSettings.MemorySizeBits };

        [JsonProperty]
        public double[] ReadCacheSecondChanceFractions { get; set; } = new[] { Globals.DefaultLogSettings.ReadCacheSettings.SecondChanceFraction };

        [JsonProperty]
        public int[] IterationCounts { get; set; } = new int[] { 1 };

        internal IEnumerable<TestResult> GetParamSweeps()
        {
            IEnumerable<TestResult> getResults()
            {
                foreach (var hashSize in this.HashSizeShifts)
                foreach (var numa in this.NumaModes)
                foreach (var dist in this.Distributions)
                foreach (var distParam in this.DistributionParameters)
                foreach (var threadCount in this.ThreadCounts)
                foreach (var initKeyCount in this.InitKeyCounts)
                foreach (var opKeyCount in this.OperationKeyCounts)
                foreach (var upsertCount in this.UpsertCounts)
                foreach (var readCount in this.ReadCounts)
                foreach (var rmwCount in this.RMWCounts)
                foreach (var mixOps in this.MixOperations)
                foreach (var keySize in this.KeySizes)
                foreach (var valueSize in this.ValueSizes)
                foreach (var useVarLenKey in this.UseVarLenKeys)
                foreach (var useVarLenValue in this.UseVarLenValues)
                foreach (var useObjectKey in this.UseObjectKeys)
                foreach (var useObjectValue in this.UseObjectValues)
                foreach (var useReadCache in this.UseReadCaches)
                foreach (var threadMode in this.ThreadModes)
                foreach (var asyncReadBatchSize in this.AsyncReadBatchSizes)
                foreach (var checkpointMode in this.CheckpointModes)
                foreach (var checkpointMs in this.CheckpointMs)
                foreach (var logMode in this.LogModes)
                foreach (var logPageSizeBits in this.LogPageSizeBits)
                foreach (var logSegmentSizeBits in this.LogSegmentSizeBits)
                foreach (var logMemorySizeBits in this.LogMemorySizeBits)
                foreach (var logMutableFraction in this.LogMutableFractions)
                foreach (var logCopyReadsToTail in this.LogCopyReadsToTails)
                foreach (var readCachePageSizeBits in this.ReadCachePageSizeBits)
                foreach (var readCacheMemorySizeBits in this.ReadCacheMemorySizeBits)
                foreach (var readCacheSecondChanceFraction in this.ReadCacheSecondChanceFractions)
                foreach (var iters in this.IterationCounts)
                { 
                    yield return new TestResult
                    {
                        Inputs = new TestInputs
                        {
                            HashSizeShift = hashSize,
                            NumaMode = numa,
                            Distribution = dist,
                            DistributionParameter = distParam,
                            DistributionSeed = this.DistributionSeed,
                            ThreadCount = threadCount,
                            InitKeyCount = initKeyCount,
                            OperationKeyCount = opKeyCount,
                            UpsertCount = upsertCount,
                            ReadCount = readCount,
                            RMWCount = rmwCount,
                            MixOperations = mixOps,
                            KeySize = keySize,
                            ValueSize = valueSize,
                            UseVarLenKey = useVarLenKey,
                            UseVarLenValue = useVarLenValue,
                            UseObjectKey = useObjectKey,
                            UseObjectValue = useObjectValue,
                            UseReadCache = useReadCache,
                            ThreadMode = threadMode,
                            AsyncReadBatchSize = asyncReadBatchSize,
                            CheckpointMode = checkpointMode,
                            CheckpointMs = checkpointMs,
                            LogMode = logMode,
                            LogPageSizeBits = logPageSizeBits,
                            LogSegmentSizeBits = logSegmentSizeBits,
                            LogMemorySizeBits = logMemorySizeBits,
                            LogMutableFraction = logMutableFraction,
                            LogCopyReadsToTail = logCopyReadsToTail,
                            ReadCachePageSizeBits = readCachePageSizeBits,
                            ReadCacheMemorySizeBits = readCacheMemorySizeBits,
                            ReadCacheSecondChanceFraction = readCacheSecondChanceFraction,
                            IterationCount = iters
                        },
                    };
                }
            }

            var results = getResults().ToArray();
            return results.Any(result => !result.Inputs.Verify()) ? Enumerable.Empty<TestResult>() : results;
        }
    }

    [JsonObject(MemberSerialization.OptIn)]
    class TestParameters
    {
        [JsonProperty]
        public TestParameterSweep[] ParameterSweeps { get; set; }

        internal void Add(TestParameterSweep sweep)
            => this.ParameterSweeps = this.ParameterSweeps is null ? new[] { sweep } : this.ParameterSweeps.Concat(new[] { sweep }).ToArray();

        internal IEnumerable<TestResult> GetParamSweeps() 
            => this.ParameterSweeps.SelectMany(sweep => sweep.GetParamSweeps());

        internal void Write(string filename) 
            => JsonUtils.WriteAllText(filename, JsonConvert.SerializeObject(this, JsonUtils.OutputSerializerSettings));

        internal static TestParameters Read(string filename) 
            => JsonConvert.DeserializeObject<TestParameters>(File.ReadAllText(filename), JsonUtils.InputSerializerSettings);

        internal static TestParameterFlags CommandLineOverrides = TestParameterFlags.None;

        internal void Override(TestResult commandLineArgs) {
            if (CommandLineOverrides == TestParameterFlags.None)
                return;
            foreach (var parameter in this.ParameterSweeps)
            {
                if (CommandLineOverrides.HasFlag(TestParameterFlags.HashSize))
                    parameter.HashSizeShifts = new[] { commandLineArgs.Inputs.HashSizeShift };
                if (CommandLineOverrides.HasFlag(TestParameterFlags.NumaMode))
                    parameter.NumaModes = new[] { commandLineArgs.Inputs.NumaMode };
                if (CommandLineOverrides.HasFlag(TestParameterFlags.Distribution))
                    parameter.Distributions = new[] { commandLineArgs.Inputs.Distribution };
                if (CommandLineOverrides.HasFlag(TestParameterFlags.DistributionParameter))
                    parameter.DistributionParameters = new[] { commandLineArgs.Inputs.DistributionParameter };
                if (CommandLineOverrides.HasFlag(TestParameterFlags.DistributionSeed))
                    parameter.DistributionSeed = commandLineArgs.Inputs.DistributionSeed;
                if (CommandLineOverrides.HasFlag(TestParameterFlags.ThreadCount))
                    parameter.ThreadCounts = new[] { commandLineArgs.Inputs.ThreadCount };
                if (CommandLineOverrides.HasFlag(TestParameterFlags.InitKeyCount))
                    parameter.InitKeyCounts = new[] { commandLineArgs.Inputs.InitKeyCount };
                if (CommandLineOverrides.HasFlag(TestParameterFlags.OpKeyCount))
                    parameter.OperationKeyCounts = new[] { commandLineArgs.Inputs.OperationKeyCount };
                if (CommandLineOverrides.HasFlag(TestParameterFlags.UpsertCount))
                    parameter.UpsertCounts = new[] { commandLineArgs.Inputs.UpsertCount };
                if (CommandLineOverrides.HasFlag(TestParameterFlags.ReadCount))
                    parameter.ReadCounts = new[] { commandLineArgs.Inputs.ReadCount };
                if (CommandLineOverrides.HasFlag(TestParameterFlags.RmwCount))
                    parameter.RMWCounts = new[] { commandLineArgs.Inputs.RMWCount };
                if (CommandLineOverrides.HasFlag(TestParameterFlags.MixOperations))
                    parameter.MixOperations = new[] { commandLineArgs.Inputs.MixOperations};
                if (CommandLineOverrides.HasFlag(TestParameterFlags.KeySize))
                    parameter.KeySizes = new[] { commandLineArgs.Inputs.KeySize };
                if (CommandLineOverrides.HasFlag(TestParameterFlags.ValueSize))
                    parameter.ValueSizes = new[] { commandLineArgs.Inputs.ValueSize };
                if (CommandLineOverrides.HasFlag(TestParameterFlags.UseVarLenKey))
                    parameter.UseVarLenKeys = new[] { commandLineArgs.Inputs.UseVarLenKey };
                if (CommandLineOverrides.HasFlag(TestParameterFlags.UseVarLenValue))
                    parameter.UseVarLenValues = new[] { commandLineArgs.Inputs.UseVarLenValue };
                if (CommandLineOverrides.HasFlag(TestParameterFlags.UseObjectKey))
                    parameter.UseObjectKeys = new[] { commandLineArgs.Inputs.UseObjectKey };
                if (CommandLineOverrides.HasFlag(TestParameterFlags.UseObjectValue))
                    parameter.UseObjectValues = new[] { commandLineArgs.Inputs.UseObjectValue };
                if (CommandLineOverrides.HasFlag(TestParameterFlags.UseReadCache))
                    parameter.UseReadCaches = new[] { commandLineArgs.Inputs.UseReadCache };
                if (CommandLineOverrides.HasFlag(TestParameterFlags.ThreadMode))
                    parameter.ThreadModes = new[] { commandLineArgs.Inputs.ThreadMode };
                if (CommandLineOverrides.HasFlag(TestParameterFlags.AsyncReadBatchSize))
                    parameter.AsyncReadBatchSizes = new[] { commandLineArgs.Inputs.AsyncReadBatchSize };
                if (CommandLineOverrides.HasFlag(TestParameterFlags.CheckpointMode))
                    parameter.CheckpointModes = new[] { commandLineArgs.Inputs.CheckpointMode };
                if (CommandLineOverrides.HasFlag(TestParameterFlags.CheckpointMs))
                    parameter.CheckpointMs = new[] { commandLineArgs.Inputs.CheckpointMs };
                if (CommandLineOverrides.HasFlag(TestParameterFlags.LogMode))
                    parameter.LogModes = new[] { commandLineArgs.Inputs.LogMode };
                if (CommandLineOverrides.HasFlag(TestParameterFlags.LogPageSizeBits))
                    parameter.LogPageSizeBits = new[] { commandLineArgs.Inputs.LogPageSizeBits };
                if (CommandLineOverrides.HasFlag(TestParameterFlags.LogSegmentSizeBits))
                    parameter.LogSegmentSizeBits = new[] { commandLineArgs.Inputs.LogSegmentSizeBits };
                if (CommandLineOverrides.HasFlag(TestParameterFlags.LogMemorySizeBits))
                    parameter.LogMemorySizeBits = new[] { commandLineArgs.Inputs.LogMemorySizeBits };
                if (CommandLineOverrides.HasFlag(TestParameterFlags.LogMutableFraction))
                    parameter.LogMutableFractions = new[] { commandLineArgs.Inputs.LogMutableFraction };
                if (CommandLineOverrides.HasFlag(TestParameterFlags.LogCopyReadsToTail))
                    parameter.LogCopyReadsToTails = new[] { commandLineArgs.Inputs.LogCopyReadsToTail };
                if (CommandLineOverrides.HasFlag(TestParameterFlags.ReadCachePageSizeBits))
                    parameter.ReadCachePageSizeBits = new[] { commandLineArgs.Inputs.ReadCachePageSizeBits };
                if (CommandLineOverrides.HasFlag(TestParameterFlags.ReadCacheMemorySizeBits))
                    parameter.ReadCacheMemorySizeBits = new[] { commandLineArgs.Inputs.ReadCacheMemorySizeBits };
                if (CommandLineOverrides.HasFlag(TestParameterFlags.ReadCacheSecondChanceFraction))
                    parameter.ReadCacheSecondChanceFractions = new[] { commandLineArgs.Inputs.ReadCacheSecondChanceFraction };
                if (CommandLineOverrides.HasFlag(TestParameterFlags.IterationCount))
                    parameter.IterationCounts = new[] { commandLineArgs.Inputs.IterationCount };
            }
        }
    }
}
