// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using Newtonsoft.Json;
using Performance.Common;

namespace FASTER.PerfTest
{
    internal static class Globals
    {
        public const int DefaultHashSizeShift = 20;
        public const int DefaultInitKeyCount = 10_000_000;
        public const int DefaultOpKeyCount = 100_000_000;
        public const int DefaultOpCount = 0;    // Require specifying the desired operations
        public const NumaMode DefaultNumaMode = NumaMode.RoundRobin;
        public const Distribution DefaultDistribution = Distribution.Uniform;
        public const double DefaultDistributionParameter = 0.99; // For Zipf only, now; same as YCSB to match Benchmark
        public const int DefaultDistributionSeed = 10193;
        public const long ChunkSize = 500;

        internal static JsonSerializerSettings outputJsonSerializerSettings = new JsonSerializerSettings { Formatting = Formatting.Indented };
        internal static JsonSerializerSettings inputJsonSerializerSettings = new JsonSerializerSettings { ObjectCreationHandling = ObjectCreationHandling.Replace };

        // MinDataSize must be 8 and ValidDataSizes must be multiples of that.
        // Adding a larger MinDataSize beyond 8 * ushort.MaxValue would require 
        // changes to VarLenType which encodes length in a ushort.
        public static int[] ValidDataSizes = new[] { 8, 16, 32, 64, 128, 256 };
        public const int MinDataSize = 8;   // Cannot be less or VarLenType breaks
        public static int MaxDataSize = ValidDataSizes[^1];

        // Global variables
        public static int KeySize = MinDataSize;    // Key Data size for the current test iteration
        public static int ValueSize = MinDataSize;  // Value Data size for the current test iteration
        public static bool Verify;                  // If true, write values on insert and RMW and verify them on Read
        public static bool IsInitialInsertPhase;    // If true, we are doing initial inserts; do not Verify
        public static bool Verbose = false;
    }

    public interface IKey
    {
        long Value { get; }
    }

    public struct Input
    {
        internal int value;

        public override string ToString() => this.value.ToString();
    }

    interface IThreadValueRef<TValue, TOutput>
    {
        ref TValue GetRef(int threadIndex);

        TOutput GetOutput(int threadIndex);

        void SetInitialValue(ref TValue valueRef, long value);

        void SetUpsertValue(ref TValue valueRef, long value, long mod);
    }
}
