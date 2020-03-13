// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace PerfTest
{
    // Used to override TestParameter values with command-line arguments, to reduce
    // proliferation/duplication of test files.
    [Flags] enum TestParameterFlags
    {
        None =              0x0000,
        Upsert =            0x0001,
        DataSize =          0x0002,
        ReadMultiple =      0x0004,
        ReadThreadCount =   0x0008,
        UseVarLenValue =    0x0010,
        UseObjectValue =    0x0020,
        UseReadCache =      0x0040,
        LogMode =           0x0080,
        IterationCount =    0x0100
    }

    [JsonObject(MemberSerialization.OptIn)]
    public class TestParameterSweep
    {
        [JsonProperty]
        public int[] UpsertCounts { get; set; }

        [JsonProperty]
        public int[] DataSizes { get; set; }

        [JsonProperty]
        public int[] ReadMultiples { get; set; }

        [JsonProperty]
        public int[] ReadThreadCounts { get; set; }

        [JsonProperty]
        public bool[] UseVarLenValues { get; set; } = new[] { false };

        [JsonProperty]
        public bool[] UseObjectValues { get; set; } = new[] { false };

        [JsonProperty]
        public bool[] UseReadCaches { get; set; } = new[] { true };

        [JsonProperty]
        public LogMode[] LogModes { get; set; } = new[] { LogMode.Flush };

        [JsonProperty]
        public int[] IterationCounts { get; set; }

        public override string ToString() 
            => $"ups[{string.Join(", ", this.UpsertCounts)}]" +
               $" ds[{string.Join(", ", this.DataSizes)}]" +
               $" rm[{string.Join(", ", this.ReadMultiples)}]" +
               $" rtc[{string.Join(", ", this.ReadThreadCounts)}]" +
               $" it[{string.Join(", ", this.IterationCounts)}]" +
               $" var[{string.Join(", ", this.UseVarLenValues)}]" +
               $" obj[{string.Join(", ", this.UseObjectValues)}]" +
               $" urc[{string.Join(", ", this.UseReadCaches)}]" +
               $" lm[{string.Join(", ", this.LogModes)}]";

        internal IEnumerable<TestResult> GetParamSweeps()
        {
            IEnumerable<TestResult> getResults()
            {
                foreach (var upsertCount in this.UpsertCounts)
                {
                    foreach (var dataSize in this.DataSizes)
                    {
                        foreach (var readMult in this.ReadMultiples)
                        {
                            foreach (var readThreads in this.ReadThreadCounts)
                            {
                                foreach (var useVarLenValue in this.UseVarLenValues)
                                {
                                    foreach (var useObjectValue in this.UseObjectValues)
                                    {
                                        foreach (var useReadCache in this.UseReadCaches)
                                        {
                                            foreach (var logMode in this.LogModes)
                                            {
                                                foreach (var iters in this.IterationCounts)
                                                {
                                                    yield return new TestResult
                                                    {
                                                        UpsertCount = upsertCount,
                                                        DataSize = dataSize,
                                                        ReadMultiple = readMult,
                                                        ReadThreadCount = readThreads,
                                                        UseVarLenValue = useVarLenValue,
                                                        UseObjectValue = useObjectValue,
                                                        UseReadCache = useReadCache,
                                                        LogMode = logMode,
                                                        IterationCount = iters
                                                    };
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            var results = getResults().ToArray();
            return results.Any(result => !result.Verify()) ? Enumerable.Empty<TestResult>() : results;
        }

        internal void Write(string filename) => File.WriteAllText(filename, JsonConvert.SerializeObject(this, PerfTest.jsonSerializerSettings));

        internal static TestParameterSweep Read(string filename) => JsonConvert.DeserializeObject<TestParameterSweep>(File.ReadAllText(filename));
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

        internal void Write(string filename) => File.WriteAllText(filename, JsonConvert.SerializeObject(this, PerfTest.jsonSerializerSettings));

        internal static TestParameters Read(string filename) => JsonConvert.DeserializeObject<TestParameters>(File.ReadAllText(filename));

        internal static TestParameterFlags CommandLineOverrides = TestParameterFlags.None;

        internal void Override(TestResult commandLineArgs) {
            if (CommandLineOverrides == TestParameterFlags.None)
                return;
            foreach (var parameter in this.ParameterSweeps)
            {
                if (CommandLineOverrides.HasFlag(TestParameterFlags.Upsert))
                    parameter.UpsertCounts = new[] { commandLineArgs.UpsertCount };
                if (CommandLineOverrides.HasFlag(TestParameterFlags.DataSize))
                    parameter.DataSizes = new[] { commandLineArgs.DataSize };
                if (CommandLineOverrides.HasFlag(TestParameterFlags.ReadMultiple))
                    parameter.ReadMultiples = new[] { commandLineArgs.ReadMultiple };
                if (CommandLineOverrides.HasFlag(TestParameterFlags.ReadThreadCount))
                    parameter.ReadThreadCounts = new[] { commandLineArgs.ReadThreadCount };
                if (CommandLineOverrides.HasFlag(TestParameterFlags.IterationCount))
                    parameter.IterationCounts = new[] { commandLineArgs.IterationCount };
                if (CommandLineOverrides.HasFlag(TestParameterFlags.UseVarLenValue))
                    parameter.UseVarLenValues = new[] { commandLineArgs.UseVarLenValue };
                if (CommandLineOverrides.HasFlag(TestParameterFlags.UseObjectValue))
                    parameter.UseObjectValues = new[] { commandLineArgs.UseObjectValue };
                if (CommandLineOverrides.HasFlag(TestParameterFlags.UseReadCache))
                    parameter.UseReadCaches = new[] { commandLineArgs.UseReadCache };
                if (CommandLineOverrides.HasFlag(TestParameterFlags.LogMode))
                    parameter.LogModes = new[] { commandLineArgs.LogMode };
            }
        }
    }
}
