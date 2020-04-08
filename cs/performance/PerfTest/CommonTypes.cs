// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using Newtonsoft.Json;
using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace FASTER.PerfTest
{
    internal static class Globals
    {
        public const int DefaultHashSizeShift = 20;
        public const int DefaultInitKeyCount = 10_000_000;
        public const int DefaultOpKeyCount = 100_000_000;
        public const int DefaultOpCount = 0;    // Require specifying the desired operations
        public const double DefaultDistributionParameter = 0.99; // For Zipf only, now; same as YCSB to match benchmark
        public const int DefaultDistributionSeed = 10193;
        public const long ChunkSize = 500;

        internal static JsonSerializerSettings jsonSerializerSettings = new JsonSerializerSettings { Formatting = Formatting.Indented };

        public static int[] ValidDataSizes = new[] { 8, 16, 32, 64, 128, 256 };
        public const int MinDataSize = 8;   // Cannot be less or VarLenValue breaks
        public static int MaxDataSize = ValidDataSizes[ValidDataSizes.Length - 1];

        public static int DataSize = MinDataSize;   // This has the data size for the current test iteration
    }

    [StructLayout(LayoutKind.Explicit, Size = 8)]
    public struct Key : IFasterEqualityComparer<Key>
    {
        [FieldOffset(0)]
        public long key;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Key(long k) => this.key = k;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetHashCode64(ref Key key) => Utility.GetHashCode(key.key);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Equals(ref Key k1, ref Key k2) => k1.key == k2.key;
    }

    public class NoSerializer<T> : BinaryObjectSerializer<T>
    {
        // Should never be instantiated
        public override void Deserialize(ref T obj) => throw new NotImplementedException();

        public override void Serialize(ref T obj) => throw new NotImplementedException();
    }

    public struct Input
    {
        internal int value;
    }

    interface IGetValueRef<TValue, TOutput>
    {
        ref TValue GetRef(int threadIndex);

        TOutput GetOutput(int threadIndex);
    }
}
