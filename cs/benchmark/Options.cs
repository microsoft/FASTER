// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using CommandLine;
using FASTER.core;
using System.Collections.Generic;

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
            HelpText = "NUMA options (Windows only):" +
                        "\n    0 = No sharding across NUMA sockets" +
                        "\n    1 = Sharding across NUMA sockets")]
        public int NumaStyle { get; set; }

        [Option('k', "recover", Required = false, Default = false,
            HelpText = "Enable Backup and Restore of FasterKV for fast test startup." +
                        "\n    True = Recover FasterKV if a Checkpoint is available, else populate FasterKV from data and Checkpoint it so it can be Restored in a subsequent run" +
                        "\n    False = Populate FasterKV from data and do not Checkpoint a backup" +
                        "\n    (Checkpoints are stored in directories under " + TestLoader.DataPath + " in directories named by distribution, ycsb vs. synthetic data, and key counts)")]
        public bool BackupAndRestore { get; set; }

        [Option('z', "locking", Required = false, Default = LockingMode.None,
            HelpText = "Locking Implementation:" +
                        $"\n    {nameof(LockingMode.None)} = No locking (default)" +
                        $"\n    {nameof(LockingMode.Standard)} = Locking using main HashTable buckets" +
                        $"\n    {nameof(LockingMode.Ephemeral)} = Locking only within concurrent IFunctions callbacks")]
        public LockingMode LockingMode { get; set; }

        [Option('i', "iterations", Required = false, Default = 1,
            HelpText = "Number of iterations of the test to run")]
        public int IterationCount { get; set; }

        [Option('d', "distribution", Required = false, Default = YcsbConstants.UniformDist,
            HelpText = "Distribution of keys in workload")]
        public string DistributionName { get; set; }

        [Option('s', "seed", Required = false, Default = 211,
            HelpText = "Seed for synthetic data distribution")]
        public int RandomSeed { get; set; }

        [Option("rumd", Separator = ',', Required = false, Default = new[] {50,50,0,0},
            HelpText = "#,#,#,#: Percentages of [(r)eads,(u)pserts,r(m)ws,(d)eletes] (summing to 100) operations in this run")]
        public IEnumerable<int> RumdPercents { get; set; }

        [Option("reviv", Required = false, Default = RevivificationLevel.None,
            HelpText = "Revivification of tombstoned records:" +
                        $"\n    {nameof(RevivificationLevel.None)} = No revivification" +
                        $"\n    {nameof(RevivificationLevel.Chain)} = Revivify tombstoned records in tag chain only" +
                        $"\n    {nameof(RevivificationLevel.Full)} = Tag chain and FreeList")]
        public RevivificationLevel RevivificationLevel { get; set; }

        [Option("reviv-bin-record-count", Separator = ',', Required = false, Default = 128,
            HelpText = "#,#,...,#: Number of records in each bin:" +
                       "    Default (not specified): All bins are 128 records" +
                       "    # (one value): All bins have this number of records, else error")]
        public int RevivBinRecordCount { get; set; }

        [Option("synth", Required = false, Default = false,
            HelpText = "Use synthetic data")]
        public bool UseSyntheticData { get; set; }

        [Option("runsec", Required = false, Default = 30,
            HelpText = "Number of seconds to execute experiment")]
        public int RunSeconds { get; set; }

        [Option("epoch-refresh", Required = false, Default = 512,
            HelpText = "Number of operations between epoch refreshes for unsafe contexts")]
        public int EpochRefreshOpCount { get; set; }

        [Option("sd", Required = false, Default = false,
            HelpText = "Use SmallData in experiment")]
        public bool UseSmallData { get; set; }

        [Option("sm", Required = false, Default = false,
            HelpText = "Use Small Memory log in experiment")]
        public bool UseSmallMemoryLog { get; set; }

        [Option("hashpack", Required = false, Default = 2.0,
            HelpText = "The hash table packing; divide the number of keys by this to cause hash collisions")]
        public double HashPacking { get; set; }

        [Option("safectx", Required = false, Default = false,
            HelpText = "Use 'safe' context (slower, per-operation epoch control) in experiment")]
        public bool UseSafeContext { get; set; }

        [Option("chkptms", Required = false, Default = 0,
            HelpText = "If > 0, the number of milliseconds between checkpoints in experiment (else checkpointing is not done")]
        public int PeriodicCheckpointMilliseconds { get; set; }

        [Option("chkptsnap", Required = false, Default = false,
            HelpText = "Use Snapshot checkpoint if doing periodic checkpoints (default is FoldOver)")]
        public bool PeriodicCheckpointUseSnapshot { get; set; }

        [Option("chkptincr", Required = false, Default = false,
            HelpText = "Try incremental checkpoint if doing periodic checkpoints")]
        public bool PeriodicCheckpointTryIncremental { get; set; }

        [Option("dumpdist", Required = false, Default = false,
            HelpText = "Dump the distribution of each non-empty bucket in the hash table")]
        public bool DumpDistribution { get; set; }

        internal CheckpointType PeriodicCheckpointType => this.PeriodicCheckpointUseSnapshot ? CheckpointType.Snapshot : CheckpointType.FoldOver;

        public string GetOptionsString()
        {
            static string boolStr(bool value) => value ? "y" : "n";
            return $"d: {DistributionName.ToLower()}; n: {NumaStyle}; rumd: {string.Join(',', RumdPercents)}; reviv: {RevivificationLevel}; revivbinrecs: {RevivBinRecordCount}; t: {ThreadCount}; z: {LockingMode}; i: {IterationCount};"
                        + $" hp: {HashPacking}; epoch-refresh {EpochRefreshOpCount}; sd: {boolStr(UseSmallData)}; sm: {boolStr(UseSmallMemoryLog)}; sy: {boolStr(this.UseSyntheticData)}; safectx: {boolStr(this.UseSafeContext)};"
                        + $" chkptms: {this.PeriodicCheckpointMilliseconds}; chkpttype: {(this.PeriodicCheckpointMilliseconds > 0 ? this.PeriodicCheckpointType.ToString() : "None")};"
                        + $" chkptincr: {boolStr(this.PeriodicCheckpointTryIncremental)}";
        }
    }
}
