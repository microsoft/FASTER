// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using Newtonsoft.Json;
using Performance.Common;
using System;
using System.Linq;

namespace FASTER.PerfTest
{
    [JsonObject(MemberSerialization.OptIn)]
    internal class TestInputs
    {
        [JsonProperty]
        public int HashSizeShift { get; set; } = Globals.DefaultHashSizeShift;

        [JsonProperty]
        public NumaMode NumaMode { get; set; } = Globals.DefaultNumaMode;

        [JsonProperty]
        public Distribution Distribution { get; set; } = Globals.DefaultDistribution;

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
        public int TotalOperationCount => this.UpsertCount + this.ReadCount + this.RMWCount;

        [JsonProperty]
        public int UpsertCount { get; set; } = Globals.DefaultOpCount;

        [JsonProperty]
        public int ReadCount { get; set; } = Globals.DefaultOpCount;

        [JsonProperty]
        public int RMWCount { get; set; } = Globals.DefaultOpCount;

        [JsonProperty]
        public bool MixOperations { get; set; } = false;

        [JsonProperty]
        public int KeySize { get; set; } = Globals.MinDataSize;

        [JsonProperty]
        public int ValueSize { get; set; } = Globals.MinDataSize;

        [JsonProperty]
        public bool UseVarLenKey { get; set; } = false;

        [JsonProperty]
        public bool UseVarLenValue { get; set; } = false;

        [JsonProperty]
        public bool UseObjectKey { get; set; } = false;

        [JsonProperty]
        public bool UseObjectValue { get; set; } = false;

        [JsonProperty]
        public bool UseReadCache { get; set; } = true;

        [JsonProperty]
        public ThreadMode ThreadMode { get; set; } = Globals.DefaultThreadMode;

        [JsonProperty]
        public int AsyncReadBatchSize { get; set; } = Globals.DefaultAsyncReadBatchSize;

        [JsonProperty]
        public Checkpoint.Mode CheckpointMode { get; set; } = Checkpoint.Mode.None;

        [JsonProperty]
        public int CheckpointMs { get; set; } = Globals.DefaultCheckpointMs;

        [JsonProperty]
        public LogMode LogMode { get; set; } = LogMode.None;

        [JsonProperty]
        public int LogPageSizeBits { get; set; } = Globals.DefaultLogSettings.PageSizeBits;

        [JsonProperty]
        public int LogSegmentSizeBits { get; set; } = Globals.DefaultLogSettings.SegmentSizeBits;

        [JsonProperty]
        public int LogMemorySizeBits { get; set; } = Globals.DefaultLogSettings.MemorySizeBits;

        [JsonProperty]
        public double LogMutableFraction { get; set; } = Globals.DefaultLogSettings.MutableFraction;

        [JsonProperty]
        public bool LogCopyReadsToTail { get; set; } = Globals.DefaultLogSettings.CopyReadsToTail;

        [JsonProperty]
        public int ReadCachePageSizeBits { get; set; } = Globals.DefaultLogSettings.ReadCacheSettings.PageSizeBits;

        [JsonProperty]
        public int ReadCacheMemorySizeBits { get; set; } = Globals.DefaultLogSettings.ReadCacheSettings.MemorySizeBits;

        [JsonProperty]
        public double ReadCacheSecondChanceFraction { get; set; } = Globals.DefaultLogSettings.ReadCacheSettings.SecondChanceFraction;

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
            if (MixOperations && (UpsertCount == TotalOperationCount || ReadCount == TotalOperationCount || RMWCount == TotalOperationCount))
                return fail($"Invalid {nameof(MixOperations)}: More than one operation must be specified");
            if (KeySize < Globals.MinDataSize)
                return fail($"Invalid {nameof(KeySize)}: {KeySize}. Must be >= {Globals.MinDataSize}");
            if (ValueSize < Globals.MinDataSize)
                return fail($"Invalid {nameof(ValueSize)}: {ValueSize}. Must be >= {Globals.MinDataSize}");
            if (IterationCount < 1)
                return fail($"Invalid {nameof(IterationCount)}: {IterationCount}. Must be >= 1");

            if (this.UseVarLenKey && this.UseObjectKey)
                return fail($"Cannot specify both {nameof(this.UseVarLenKey)} and {nameof(this.UseObjectKey)}");
            if (this.UseVarLenValue && this.UseObjectValue)
                return fail($"Cannot specify both {nameof(this.UseVarLenValue)} and {nameof(this.UseObjectValue)}");
            if (!Globals.ValidDataSizes.Contains(this.KeySize))
                return fail($"Key Data sizes must be in [{string.Join(", ", Globals.ValidDataSizes)}]");
            if (!Globals.ValidDataSizes.Contains(this.ValueSize))
                return fail($"Value Data sizes must be in [{string.Join(", ", Globals.ValidDataSizes)}]");

            if (this.AsyncReadBatchSize < 0)
                return fail($"{nameof(this.AsyncReadBatchSize)} must be >= 0");
            if (this.CheckpointMs < 0)
                return fail($"{nameof(this.CheckpointMs)} must be >= 0");
            if (this.CheckpointMode != Checkpoint.Mode.None && this.CheckpointMs < 1)
                return fail($"{this.CheckpointMode} requires {nameof(this.CheckpointMs)} >= 1");

            if (this.LogPageSizeBits < LogSettings.kMinPageSizeBits || this.LogPageSizeBits > LogSettings.kMaxPageSizeBits)
                return fail($"{nameof(this.LogPageSizeBits)} must be between {LogSettings.kMinPageSizeBits} and {LogSettings.kMinPageSizeBits}");
            if (this.LogSegmentSizeBits < LogSettings.kMinSegmentSizeBits || this.LogSegmentSizeBits > LogSettings.kMaxSegmentSizeBits)
                return fail($"{nameof(this.LogSegmentSizeBits)} must be between {LogSettings.kMinSegmentSizeBits} and {LogSettings.kMaxSegmentSizeBits}");
            if (this.LogMemorySizeBits < LogSettings.kMinMemorySizeBits || this.LogMemorySizeBits > LogSettings.kMaxMemorySizeBits)
                return fail($"{nameof(this.LogMemorySizeBits)} must be between {LogSettings.kMinMemorySizeBits} and {LogSettings.kMaxMemorySizeBits}");
            if (this.LogMutableFraction <= 0.0 || this.LogMutableFraction >= 1.0)
                return fail($"{nameof(this.LogMutableFraction)} must be > 0.0 and < 1.0");
            if (this.ReadCachePageSizeBits < LogSettings.kMinPageSizeBits || this.ReadCachePageSizeBits > LogSettings.kMaxPageSizeBits)
                return fail($"{nameof(this.ReadCachePageSizeBits)} must be between {LogSettings.kMinPageSizeBits} and {LogSettings.kMaxPageSizeBits}");
            if (this.ReadCacheMemorySizeBits < LogSettings.kMinMemorySizeBits || this.ReadCacheMemorySizeBits > LogSettings.kMaxMemorySizeBits)
                return fail($"{nameof(this.ReadCacheMemorySizeBits)} must be between {LogSettings.kMinMemorySizeBits} and {LogSettings.kMaxMemorySizeBits}");
            if (this.ReadCacheSecondChanceFraction <= 0.0 || this.ReadCacheSecondChanceFraction >= 1.0)
                return fail($"{(this.ReadCacheSecondChanceFraction)} must be > 0.0 and < 1.0");

            return true;
        }

        public override string ToString()
            => $" {PerfTest.HashSizeArg} {this.HashSizeShift}" +
                $" {PerfTest.NumaArg} {this.NumaMode}" +
                $" {PerfTest.DistArg} {this.Distribution}" +
                $" {PerfTest.DistParamArg} {this.DistributionParameter}" +
                $" {PerfTest.DistSeedArg} {this.DistributionSeed}" +
                $" {PerfTest.ThreadsArg} {this.ThreadCount}" +
                $" {PerfTest.InitKeysArg} {this.InitKeyCount}" +
                $" {PerfTest.OpKeysArg} {this.OperationKeyCount}" +
                $" {PerfTest.UpsertsArg} {this.UpsertCount}" +
                $" {PerfTest.ReadsArg} {this.ReadCount}" +
                $" {PerfTest.RMWsArg} {this.RMWCount}" +
                $" {PerfTest.MixOpsArg} {this.MixOperations}" +
                $" {PerfTest.KeySizeArg} {this.KeySize}" +
                $" {PerfTest.ValueSizeArg} {this.ValueSize}" +
                $" {PerfTest.UseVarLenKeysArg} {this.UseVarLenKey}" +
                $" {PerfTest.UseVarLenValuesArg} {this.UseVarLenValue}" +
                $" {PerfTest.UseObjKeysArg} {this.UseObjectKey}" +
                $" {PerfTest.UseObjValuesArg} {this.UseObjectValue}" +
                $" {PerfTest.UseRcArg} {this.UseReadCache}" +
                $" {PerfTest.ThreadModeArg} {this.ThreadMode}" +
                $" {PerfTest.AsyncReadBatchSizeArg} {this.AsyncReadBatchSize}" +
                $" {PerfTest.CheckpointModeArg} {this.CheckpointMode}" +
                $" {PerfTest.CheckpointMsArg} {this.CheckpointMs}" +
                $" {PerfTest.LogArg} {this.LogMode}" +
                $" {PerfTest.LogPageSizeBitsArg} {this.LogPageSizeBits}" +
                $" {PerfTest.LogSegmentSizeBitsArg} {this.LogSegmentSizeBits}" +
                $" {PerfTest.LogMemorySizeBitsArg} {this.LogMemorySizeBits}" +
                $" {PerfTest.LogMutableFractionArg} {this.LogMutableFraction}" +
                $" {PerfTest.LogCopyReadsToTailArg} {this.LogCopyReadsToTail}" +
                $" {PerfTest.ReadCachePageSizeBitsArg} {this.ReadCachePageSizeBits}" +
                $" {PerfTest.ReadCacheMemorySizeBitsArg} {this.ReadCacheMemorySizeBits}" +
                $" {PerfTest.ReadCacheSecondChanceFractionArg} {this.ReadCacheSecondChanceFraction}" +
                $" {PerfTest.ItersArg} {this.IterationCount}"
            ;

        internal (int, NumaMode, Distribution, double, int, int, int, int, int, int, int, bool, int, int, bool, bool, bool, bool, bool, LogMode,
                 int, int, int, double, bool, int, int, double, int) MemberTuple
            => (this.HashSizeShift, this.NumaMode,
                this.Distribution, this.DistributionParameter, this.DistributionSeed, this.ThreadCount,
                this.InitKeyCount, this.OperationKeyCount, this.UpsertCount, this.ReadCount, this.RMWCount, this.MixOperations,
                this.KeySize, this.ValueSize, this.UseVarLenKey, this.UseVarLenValue, this.UseObjectKey, this.UseObjectValue, this.UseReadCache,
                this.LogMode, this.LogPageSizeBits, this.LogSegmentSizeBits, this.LogMemorySizeBits, this.LogMutableFraction, this.LogCopyReadsToTail,
                this.ReadCachePageSizeBits, this.ReadCacheMemorySizeBits, this.ReadCacheSecondChanceFraction, this.IterationCount);

        internal (Distribution, double, double) DistributionInfo
            => (this.Distribution, this.DistributionParameter, this.DistributionSeed);
    }
}
