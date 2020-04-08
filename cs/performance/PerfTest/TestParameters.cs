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
        DataSize =              0x00001000,
        UseVarLenValue =        0x00002000,
        UseObjectValue =        0x00004000,
        UseReadCache =          0x00008000,
        LogMode =               0x00010000,
        IterationCount =        0x00020000
    }

    [JsonObject(MemberSerialization.OptIn)]
    public class TestParameterSweep
    {
        [JsonProperty]
        public int[] HashSizeShifts { get; set; } = new[] { Globals.DefaultHashSizeShift };

        [JsonProperty]
        public NumaMode[] NumaModes { get; set; } = new[] { NumaMode.None };

        [JsonProperty]
        public Distribution[] Distributions { get; set; } = new[] { Distribution.Uniform };

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
        public int[] DataSizes { get; set; } = new[] { Globals.MinDataSize };

        [JsonProperty]
        public bool[] UseVarLenValues { get; set; } = new[] { false };

        [JsonProperty]
        public bool[] UseObjectValues { get; set; } = new[] { false };

        [JsonProperty]
        public bool[] UseReadCaches { get; set; } = new[] { true };

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
                foreach (var dataSize in this.DataSizes)
                foreach (var useVarLenValue in this.UseVarLenValues)
                foreach (var useObjectValue in this.UseObjectValues)
                foreach (var useReadCache in this.UseReadCaches)
                foreach (var logMode in this.LogModes)
                foreach (var iters in this.IterationCounts)
                { 
                    yield return new TestResult
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
                        DataSize = dataSize,
                        UseVarLenValue = useVarLenValue,
                        UseObjectValue = useObjectValue,
                        UseReadCache = useReadCache,
                        LogMode = logMode,
                        IterationCount = iters,
                        DistributionSeed = this.DistributionSeed
                    };
                }
            }

            var results = getResults().ToArray();
            return results.Any(result => !result.Verify()) ? Enumerable.Empty<TestResult>() : results;
        }

        internal void Write(string filename) 
            => File.WriteAllText(filename, JsonConvert.SerializeObject(this, Globals.jsonSerializerSettings));

        internal static TestParameterSweep Read(string filename) 
            => JsonConvert.DeserializeObject<TestParameterSweep>(File.ReadAllText(filename));
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
            => File.WriteAllText(filename, JsonConvert.SerializeObject(this, Globals.jsonSerializerSettings));

        internal static TestParameters Read(string filename) 
            => JsonConvert.DeserializeObject<TestParameters>(File.ReadAllText(filename));

        internal static TestParameterFlags CommandLineOverrides = TestParameterFlags.None;

        internal void Override(TestResult commandLineArgs) {
            if (CommandLineOverrides == TestParameterFlags.None)
                return;
            foreach (var parameter in this.ParameterSweeps)
            {
                if (CommandLineOverrides.HasFlag(TestParameterFlags.HashSize))
                    parameter.HashSizeShifts = new[] { commandLineArgs.HashSizeShift };
                if (CommandLineOverrides.HasFlag(TestParameterFlags.NumaMode))
                    parameter.NumaModes = new[] { commandLineArgs.NumaMode };
                if (CommandLineOverrides.HasFlag(TestParameterFlags.Distribution))
                    parameter.Distributions = new[] { commandLineArgs.Distribution };
                if (CommandLineOverrides.HasFlag(TestParameterFlags.DistributionParameter))
                    parameter.DistributionParameters = new[] { commandLineArgs.DistributionParameter };
                if (CommandLineOverrides.HasFlag(TestParameterFlags.DistributionSeed))
                    parameter.DistributionSeed = commandLineArgs.DistributionSeed;
                if (CommandLineOverrides.HasFlag(TestParameterFlags.ThreadCount))
                    parameter.ThreadCounts = new[] { commandLineArgs.ThreadCount };
                if (CommandLineOverrides.HasFlag(TestParameterFlags.InitKeyCount))
                    parameter.InitKeyCounts = new[] { commandLineArgs.InitKeyCount };
                if (CommandLineOverrides.HasFlag(TestParameterFlags.OpKeyCount))
                    parameter.OperationKeyCounts = new[] { commandLineArgs.OperationKeyCount };
                if (CommandLineOverrides.HasFlag(TestParameterFlags.UpsertCount))
                    parameter.UpsertCounts = new[] { commandLineArgs.UpsertCount };
                if (CommandLineOverrides.HasFlag(TestParameterFlags.ReadCount))
                    parameter.ReadCounts = new[] { commandLineArgs.ReadCount };
                if (CommandLineOverrides.HasFlag(TestParameterFlags.RmwCount))
                    parameter.RMWCounts = new[] { commandLineArgs.RMWCount };
                if (CommandLineOverrides.HasFlag(TestParameterFlags.MixOperations))
                    parameter.MixOperations = new[] { commandLineArgs.MixOperations};
                if (CommandLineOverrides.HasFlag(TestParameterFlags.DataSize))
                    parameter.DataSizes = new[] { commandLineArgs.DataSize };
                if (CommandLineOverrides.HasFlag(TestParameterFlags.UseVarLenValue))
                    parameter.UseVarLenValues = new[] { commandLineArgs.UseVarLenValue };
                if (CommandLineOverrides.HasFlag(TestParameterFlags.UseObjectValue))
                    parameter.UseObjectValues = new[] { commandLineArgs.UseObjectValue };
                if (CommandLineOverrides.HasFlag(TestParameterFlags.UseReadCache))
                    parameter.UseReadCaches = new[] { commandLineArgs.UseReadCache };
                if (CommandLineOverrides.HasFlag(TestParameterFlags.LogMode))
                    parameter.LogModes = new[] { commandLineArgs.LogMode };
                if (CommandLineOverrides.HasFlag(TestParameterFlags.IterationCount))
                    parameter.IterationCounts = new[] { commandLineArgs.IterationCount };
            }
        }
    }
}
