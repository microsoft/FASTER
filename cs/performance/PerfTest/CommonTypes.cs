// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using Newtonsoft.Json;
using System;
using System.IO;
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

        internal static JsonSerializerSettings outputJsonSerializerSettings = new JsonSerializerSettings { Formatting = Formatting.Indented };
        internal static JsonSerializerSettings inputJsonSerializerSettings = new JsonSerializerSettings { ObjectCreationHandling = ObjectCreationHandling.Replace };

        // MinDataSize must be 8 and ValidDataSizes must be multiples of that.
        // Adding a larger MinDataSize beyond 8 * ushort.MaxValue would require 
        // changes to VarLenValue which encodes length in a ushort.
        public static int[] ValidDataSizes = new[] { 8, 16, 32, 64, 128, 256 };
        public const int MinDataSize = 8;   // Cannot be less or VarLenValue breaks
        public static int MaxDataSize = ValidDataSizes[ValidDataSizes.Length - 1];

        // Global variables
        public static int DataSize = MinDataSize;   // Data size for the current test iteration
        public static bool Verify;                  // If true, write values on insert and RMW and verify them on Read
        public static bool IsInitialInsertPhase;    // If true, we are doing initial inserts; do not Verify
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

        public override string ToString() => this.key.ToString();
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

        public override string ToString() => this.value.ToString();
    }

    interface IThreadValueRef<TValue, TOutput>
    {
        ref TValue GetRef(int threadIndex);

        TOutput GetOutput(int threadIndex);

        void SetInitialValue(ref TValue valueRef, long value);

        void SetUpsertValue(ref TValue valueRef, long value, long mod);
    }

    internal struct BlittableData
    {
        // Modified by RMW
        internal uint Modified;

        // This is key.key which must be an int as we allocate an array of sequential Keys
        internal int Value;

        public static int SizeOf => sizeof(uint) + sizeof(int);

        internal void Read(BinaryReader reader)
        {
            this.Modified = reader.ReadUInt32();
            this.Value = reader.ReadInt32();
        }

        internal void Write(BinaryWriter writer)
        {
            writer.Write(this.Modified);
            writer.Write(this.Value);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Equals(BlittableData other)
            => this.Modified == other.Modified && this.Value == other.Value;

        public override bool Equals(object other) => other is BlittableData bd && this.Equals(bd);

        public override int GetHashCode() => HashCode.Combine(Modified, Value);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool operator ==(BlittableData lhs, BlittableData rhs) 
            => lhs.Equals(rhs);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool operator !=(BlittableData lhs, BlittableData rhs)
            => !lhs.Equals(rhs);

        public override string ToString() => $"mod {Modified}, val {Value}";
    }
}
