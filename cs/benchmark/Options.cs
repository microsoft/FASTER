// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using CommandLine;

namespace FASTER.benchmark
{
    class Options
    {
        [Option('b', "benchmark", Required = false, Default = 0,
        HelpText = "Benchmark to run:" +
                        "\n    0 = YCSB" +
                        "\n    1 = YCSB with SpanByte" +
                        "\n    2 = ConcurrentDictionary")]
        public int Benchmark { get; set; }

        [Option('t', "threads", Required = false, Default = 8,
         HelpText = "Number of threads to run the workload on")]
        public int ThreadCount { get; set; }

        [Option('n', "numa", Required = false, Default = 0,
             HelpText = "NUMA options:" +
                        "\n    0 = No sharding across NUMA sockets" +
                        "\n    1 = Sharding across NUMA sockets")]
        public int NumaStyle { get; set; }

        [Option('k', "recover", Required = false, Default = false,
             HelpText = "Enable Backup and Restore of FasterKV for fast test startup." +
                        "\n    True = Recover FasterKV if a Checkpoint is available, else populate FasterKV from data and Checkpoint it so it can be Restored in a subsequent run" +
                        "\n    False = Populate FasterKV from data and do not Checkpoint a backup" +
                        "\n    (Checkpoints are stored in directories under " + TestLoader.DataPath + " in directories named by distribution, ycsb vs. synthetic data, and key counts)")]
        public bool BackupAndRestore { get; set; }

        [Option('z', "locking", Required = false, Default = 0,
             HelpText = "Locking Implementation:" +
                        "\n    0 = None (default)" +
                        "\n    1 = RecordInfo.SpinLock()")]
        public int LockImpl { get; set; }

        [Option('i', "iterations", Required = false, Default = 1,
         HelpText = "Number of iterations of the test to run")]
        public int IterationCount { get; set; }

        [Option('r', "read_percent", Required = false, Default = 50,
         HelpText = "Percentage of reads (-1 for 100% read-modify-write")]
        public int ReadPercent { get; set; }

        [Option('d', "distribution", Required = false, Default = YcsbConstants.UniformDist,
            HelpText = "Distribution of keys in workload")]
        public string DistributionName { get; set; }

        [Option('s', "seed", Required = false, Default = 211,
            HelpText = "Seed for synthetic data distribution")]
        public int RandomSeed { get; set; }

        [Option("synth", Required = false, Default = false,
            HelpText = "Use synthetic data")]
        public bool UseSyntheticData { get; set; }

        [Option("runsec", Required = false, Default = 30,
            HelpText = "Number of seconds to execute experiment")]
        public int RunSeconds { get; set; }

        [Option("sd", Required = false, Default = false,
            HelpText = "Use SmallData in experiment")]
        public bool UseSmallData { get; set; }

        [Option("sm", Required = false, Default = false,
            HelpText = "Use Small Memory log in experiment")]
        public bool UseSmallMemoryLog { get; set; }

        [Option("noaff", Required = false, Default = false,
            HelpText = "Do not use thread affinitization in experiment")]
        public bool NoThreadAffinity { get; set; }

        [Option("chkptms", Required = false, Default = 0,
            HelpText = "If > 0, the number of milliseconds between checkpoints in experiment (else checkpointing is not done")]
        public int PeriodicCheckpointMilliseconds { get; set; }

        [Option("dumpdist", Required = false, Default = false,
            HelpText = "Dump the distribution of each non-empty bucket in the hash table")]
        public bool DumpDistribution { get; set; }

        public string GetOptionsString()
        {
            static string boolStr(bool value) => value ? "y" : "n";
            return $"d: {DistributionName.ToLower()}; n: {NumaStyle}; r: {ReadPercent}; t: {ThreadCount}; z: {LockImpl}; i: {IterationCount};"
                        + $" sd: {boolStr(UseSmallData)}; sm: {boolStr(UseSmallMemoryLog)}; sy: {boolStr(this.UseSyntheticData)};" 
                        + $" noaff: {boolStr(this.NoThreadAffinity)}; chkpt: {this.PeriodicCheckpointMilliseconds}";
        }
    }
}
