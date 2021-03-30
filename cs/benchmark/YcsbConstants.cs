// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

namespace FASTER.benchmark
{
    enum BenchmarkType : int
    {
        Ycsb,
        SpanByte,
        ConcurrentDictionaryYcsb
    };

    enum LockImpl : int
    {
        None,
        RecordInfo
    };

    enum AddressLineNum : int
    {
        Before = 1,
        After = 2
    }

    enum AggregateType
    {
        Running = 0,
        FinalFull = 1,
        FinalTrimmed = 2
    }

    enum StatsLineNum : int
    {
        Iteration = 3,
        RunningIns = 4,
        RunningOps = 5,
        FinalFullIns = 10,
        FinalFullOps = 11,
        FinalTrimmedIns = 20,
        FinalTrimmedOps = 21
    }

    public enum Op : ulong
    {
        Upsert = 0,
        Read = 1,
        ReadModifyWrite = 2
    }

    public static class YcsbConstants
    {
        internal const string UniformDist = "uniform";    // Uniformly random distribution of keys
        internal const string ZipfDist = "zipf";          // Smooth zipf curve (most localized keys)

        internal const string SyntheticData = "synthetic";
        internal const string YcsbData = "ycsb";

        internal const string InsPerSec = "ins/sec";
        internal const string OpsPerSec = "ops/sec";

#if DEBUG
        internal const bool kUseSmallData = true;
        internal const bool kSmallMemoryLog = false;
        internal const int kRunSeconds = 30;
        internal const bool kDumpDistribution = false;
        internal const bool kAffinitizedSession = true;
        internal const int kPeriodicCheckpointMilliseconds = 0;
#else
        internal const bool kUseSmallData = false;
        internal const bool kSmallMemoryLog = true;//false;
        internal const int kRunSeconds = 30;
        internal const bool kDumpDistribution = false;
        internal const bool kAffinitizedSession = true;
        internal const int kPeriodicCheckpointMilliseconds = 0;
#endif

        internal const long kInitCount = kUseSmallData ? 2500480 : 250000000;
        internal const long kTxnCount = kUseSmallData ? 10000000 : 1000000000;
        internal const int kMaxKey = kUseSmallData ? 1 << 22 : 1 << 28;

        internal const int kFileChunkSize = 4096;
        internal const long kChunkSize = 640;
    }
}
