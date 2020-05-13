// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using Newtonsoft.Json;
using Performance.Common;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace FASTER.PerfTest
{
    // Used to override TestParameter values with command-line arguments, to reduce
    // proliferation/duplication of test files.
    [Flags] enum TestParameterFlags
    {
        None =                  0x00000000,
        HashSize =              0x00000001,
        NumaMode =              0x00000002,
        Distribution =          0x00000004,
        DistributionParameter = 0x00000008,
        DistributionSeed =      0x00000010,
        ThreadCount =           0x00000020,
        InitKeyCount =          0x00000040,
        OpKeyCount =            0x00000080,
        UpsertCount =           0x00000100,
        ReadCount =             0x00000200,
        RmwCount =              0x00000400,
        MixOperations =         0x00000800,
        KeySize =               0x00001000,
        ValueSize =             0x00002000,
        UseVarLenKey =          0x00004000,
        UseVarLenValue =        0x00008000,
        UseObjectKey =          0x00010000,
        UseObjectValue =        0x00020000,
        UseReadCache =          0x00040000,
        UseAsync =              0x00080000,
        AsyncReadBatchSize =    0x00100000,
        CheckpointMode =        0x00200000,
        CheckpointMs =          0x00400000,
        LogMode =               0x00800000,
        IterationCount =        0x01000000
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
        public int DistributionSeed { get; set; } = Globals.DefaultDistributionSeed;

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
        public bool[] MixOperations { get; set; } = new [] { false };

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
        public bool[] UseAsyncs { get; set; } = new[] { false };

        [JsonProperty]
        public int[] AsyncReadBatchSizes { get; set; } = new[] { Globals.DefaultAsyncReadBatchSize };

        [JsonProperty]
        public Checkpoint.Mode[] CheckpointModes { get; set; } = new[] { Checkpoint.Mode.None };

        [JsonProperty]
        public int[] CheckpointMs { get; set; } = new[] { Globals.DefaultCheckpointMs };

        [JsonProperty]
        public LogMode[] LogModes { get; set; } = new[] { LogMode.None };

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
                foreach (var useAsync in this.UseAsyncs)
                foreach (var asyncReadBatchSize in this.AsyncReadBatchSizes)
                foreach (var checkpointMode in this.CheckpointModes)
                foreach (var checkpointMs in this.CheckpointMs)
                foreach (var logMode in this.LogModes)
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
                            UseAsync = useAsync,
                            AsyncReadBatchSize = asyncReadBatchSize,
                            CheckpointMode = checkpointMode,
                            CheckpointMs = checkpointMs,
                            LogMode = logMode,
                            IterationCount = iters,
                            DistributionSeed = this.DistributionSeed
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
            => File.WriteAllText(filename, JsonConvert.SerializeObject(this, Globals.outputJsonSerializerSettings));

        internal static TestParameters Read(string filename) 
            => JsonConvert.DeserializeObject<TestParameters>(File.ReadAllText(filename));

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
                if (CommandLineOverrides.HasFlag(TestParameterFlags.UseAsync))
                    parameter.UseAsyncs = new[] { commandLineArgs.Inputs.UseAsync };
                if (CommandLineOverrides.HasFlag(TestParameterFlags.AsyncReadBatchSize))
                    parameter.AsyncReadBatchSizes = new[] { commandLineArgs.Inputs.AsyncReadBatchSize };
                if (CommandLineOverrides.HasFlag(TestParameterFlags.CheckpointMode))
                    parameter.CheckpointModes = new[] { commandLineArgs.Inputs.CheckpointMode };
                if (CommandLineOverrides.HasFlag(TestParameterFlags.CheckpointMs))
                    parameter.CheckpointMs = new[] { commandLineArgs.Inputs.CheckpointMs };
                if (CommandLineOverrides.HasFlag(TestParameterFlags.LogMode))
                    parameter.LogModes = new[] { commandLineArgs.Inputs.LogMode };
                if (CommandLineOverrides.HasFlag(TestParameterFlags.IterationCount))
                    parameter.IterationCounts = new[] { commandLineArgs.Inputs.IterationCount };
            }
        }
    }
}
