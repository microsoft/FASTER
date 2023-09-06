// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using CommandLine;
using FASTER.core;

namespace FASTER.stress
{
    public enum DataType { Long, String, SpanByte };
    public enum Verbose { None, Low, Med, High };

    public enum RevivificationLevel
    {
        None = 0,
        Chain = 1,
        Full = 2
    }

    class Options
    {
        #region Shortname options

        [Option('t', "Threads", Required = false, Default = 8, HelpText = "Number of threads to run the workload on")]
        public int ThreadCount { get; set; }

        [Option('i', "Iterations", Required = false, Default = 1, HelpText = "Number of iterations of the test to run")]
        public int IterationCount { get; set; }

        [Option('c', "Collisions", Required = false, Default = 1, HelpText = "Average number of key hash collisions")]
        public int CollisionCount { get; set; }

        [Option('s', "Seed", Required = false, Default = 0, HelpText = "Seed for random key selection; if not specified, sequential key accesses are done")]
        public int RandomSeed { get; set; }

        [Option('v', "Verbose", Required = false, Default = 1, HelpText = "Verbose status output level during the run")]
        public Verbose Verbose { get; set; }

        [Option('o', "OutputDir", Required = false, Default = "D:/data/FasterStress", HelpText = "Output directory for FasterKV log and snapshot checkpoints")]
        public string OutputDirectory { get; set; } = string.Empty;

        #endregion Shortname options

        #region Longname-only options

        [Option("CheckpointSec", Separator = ',', Required = false, Default = new int[0],
            HelpText = "Checkpoint interval; default is none. Format is <sec>[,<initial_delay_sec>] where:"+
                        "\n     <sec> is the number of seconds between checkpoints" +
                        "\n     <initial_delay_sec>, if specified, is the number of seconds to delay initially before starting the timer")]
        public IEnumerable<int> CheckpointSecAndDelay { get; set; }
        internal int CheckpointIntervalSec { get; set; }
        internal int CheckpointDelaySec { get; set; }

        [Option(Required = false, Default = CheckpointType.FoldOver, HelpText = "Type of Checkpoint if doing periodic Checkpoints")]
        public CheckpointType CheckpointType { get; set; }

        [Option("Inc", Required = false, Default = false, HelpText = "Try incremental Checkpoint if doing periodic Checkpoints")]
        public bool CheckpointIncremental { get; set; }

        [Option("CompactSec", Separator = ',', Required = false, Default = new int[0],
            HelpText = "Compaction interval; default is none. Format is <sec>[,<initial_delay_sec>] where:" +
                        "\n     <sec> is the number of milliseconds between compactions" +
                        "\n     <initial_delay_sec>, if specified, is the number of seconds to delay initially before starting the timer")]
        public IEnumerable<int> CompactSecAndDelay { get; set; } = Array.Empty<int>();
        internal int CompactIntervalSec { get; set; }
        internal int CompactDelaySec { get; set; }

        [Option(Required = false, Default = CompactionType.Scan, HelpText = "Type of Compact if doing periodic Compacts")]
        public CompactionType CompactType { get; set; }

        [Option(Required = false, Default = 20, HelpText = "Percentage of the log to Compact if doing periodic Compacts")]
        public int CompactPercent { get; set; }

        [Option("Trunc", Required = false, Default = false, HelpText = "Truncate the log on Compacts")]
        public bool CompactTruncate { get; set; }

        [Option("ReadCache", Required = false, Default = false, HelpText = "Use the read cache")]
        public bool ReadCache { get; set; }

        [Option("InitialEvict", Required = false, Default = false, HelpText = "FlushAndEvict after initial FasterKV population")]
        public bool InitialEvict { get; set; }

        [Option("Keys", Required = false, Default = 2000, HelpText = "Number of keys")]
        public int KeyCount { get; set; }

        [Option("KeyType", Required = false, Default = DataType.Long, HelpText = "Key datatype")]
        public DataType KeyType { get; set; }

        [Option("KeyLength", Required = false, Default = TestLoader.MinDataLen, HelpText = "Length of keys, if string or SpanByte; if RandomSeed is specified, this is the max length. Default is also the minimum")]
        public int KeyLength { get; set; }

        [Option("ValueType", Required = false, Default = DataType.Long, HelpText = "Value datatype")]
        public DataType ValueType { get; set; }

        [Option("ValueLength", Required = false, Default = TestLoader.MinDataLen, HelpText = "Length of values, if string or SpanByte; if RandomSeed is specified, this is the max length. Default is also the minimum")]
        public int ValueLength { get; set; }

        [Option("LogInMem%", Required = false, Default = 50, HelpText = "Log size as a percentage of total records; the log is big enough to hold this many records in memory (approximate; based on power of 2)")]
        public int LogInMemPercent { get; set; }

        [Option("LogPageSizeShift", Required = false, Default = 4, HelpText = "Log page size as a function of total in-memory size << this value")]
        public int LogPageSizeShift { get; set; }

        [Option("LogSegmentSizeShift", Required = false, Default = 2, HelpText = "Log segment size as a function of total in-memory size << this value (must be <= LogInMemPageSizeShift")]
        public int LogSegmentSizeShift { get; set; }

        [Option("ReadCacheInMem%", Required = false, Default = 25, HelpText = "ReadCache size as a percentage of total records; the ReadCache is big enough to hold this many records in memory (approximate; based on power of 2)")]
        public int ReadCacheInMemPercent { get; set; }

        [Option("ReadCachePageSizeShift", Required = false, Default = 4, HelpText = "ReadCache page size as a function of total in-memory size << this value")]
        public int ReadCachePageSizeShift { get; set; }

        [Option("Async%", Required = false, Default = 50, HelpText = "Percentage of operations that should use Async APIs")]
        public int AsyncPercent { get; set; }

        [Option("LUC%", Required = false, Default = 0, HelpText = "If nonzero, this will use LockableUnsafeContext on this percentage of sessions")]
        public int LUCPercent { get; set; }

        [Option("LUCLocks", Required = false, Default = 0, HelpText = "If nonzero, must be >= 2; this will use LockableUnsafeContext and lock this number of records. If RandomSeed is specified, this is the maximum value")]
        public int LUCLockCount { get; set; }

        [Option("rumd", Separator = ',', Required = false, Default = new[] { 50, 50, 0, 0 },
            HelpText = "#,#,#,#: Percentages of [(r)eads,(u)pserts,r(m)ws,(d)eletes] (summing to 100) operations in this run")]
        public IEnumerable<int> RumdPercents { get; set; }

        [Option("reviv-bin-record-sizes", Separator = ',', Required = false, Default = null,
            HelpText = $"#,#,...,#: Sizes of records in each revivification bin, in order of increasing size." +
                        "           Cannot be used with --reviv-bins-powerof2")]
        public IEnumerable<int> RevivBinRecordSizes { get; set; }

        [Option("reviv-bin-record-counts", Separator = ',', Required = false, Default = null,
            HelpText = "#,#,...,#: Number of records in each bin:" +
                       "    Default (not specified): If reviv-bin-record-sizes is specified, each bin is 128 records" +
                       "    # (one value): If reviv-bin-record-sizes is specified, then all bins have this number of records, else error" +
                       "    #,#,...,# (multiple values): If reviv-bin-record-sizes is specified, then it must be the same size as that array, else error" +
                       "                                 Cannot be used with reviv-bins-powerof2")]
        public IEnumerable<int> RevivBinRecordCounts { get; set; }

        [Option("reviv-mutable%", Separator = ',', Required = false, Default = 100,
            HelpText = $"#: Percentage of in-memory mutable space, from the highest log address down, that is eligible for revivification")]
        public int RevivMutablePercent { get; set; }

        [Option("reviv-bins-powerof2", Required = false, Default = false,
            HelpText = "A shortcut to specify revivification with power-of-2-sized bins." +
                       "    Cannot be used with reviv-bin-record-sizes" +
                       "    The empty or single-value form of reviv-bin-record-counts applies; the multiple-value form is an error")]
        public bool UseRevivBinsPowerOf2 { get; set; }

        [Option("reviv-bin-search-next-highest", Required = false, Default = false,
            HelpText = "Search the next-highest bin if the search cannot be satisfied in the best-fitting bin." +
                       "    Requires reviv-bin-record-sizes or reviv-bins-powerof2")]
        public bool RevivBinSearchNextHighest { get; set; }

        [Option("reviv-bin-best-fit-scan-limit", Required = false, Default = 0,
            HelpText = "Number of records to scan for best fit after finding first fit." +
                       "    Requires reviv-bin-record-sizes or reviv-bins-powerof2" +
                       "    0: Use first fit" +
                       "    #: Limit scan to this many records after first fit, up to the record count of the bin")]
        public int RevivBinBestFitScanLimit { get; set; }

        [Option("reviv-in-chain-only", Required = false, Default = false,
            HelpText = "Revivify tombstoned records in tag chains only (do not use free list)." +
                       "    Cannot be used with reviv-bin-record-sizes or reviv-bins-powerof2")]
        public bool RevivInChainOnly { get; set; }

        #endregion Longname-only options

        public override string ToString() => Parser.Default.FormatCommandLine(this);
    }
}
