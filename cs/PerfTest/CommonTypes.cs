// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;

namespace PerfTest
{
    internal static class CacheGlobals
    {
        public const int MinDataSize = 8;
        public static int DataSize = MinDataSize;

        public static int[] BlittableDataSizes = new[] { 8, 16, 32, 64, 128, 256 };
    }

    public struct CacheKey : IFasterEqualityComparer<CacheKey>
    {
        public long key;

        public CacheKey(long first) => key = first;

        public long GetHashCode64(ref CacheKey key) => Utility.GetHashCode(key.key);
        public bool Equals(ref CacheKey k1, ref CacheKey k2) => k1.key == k2.key;

        public class Serializer : BinaryObjectSerializer<CacheKey>
        {
            public override void Deserialize(ref CacheKey obj) => obj.key = reader.ReadInt64();

            public override void Serialize(ref CacheKey obj) => writer.Write(obj.key);
        }
    }

    public interface ICacheValue<TValue>
    {
        long Value { get; set; }
        bool CompareValue(long returnedValue);

        TValue Create(long first);
    }

    public class UnusedSerializer<T> : BinaryObjectSerializer<T>
    {
        // Should never be instantiated
        public override void Deserialize(ref T obj) => throw new NotImplementedException();

        public override void Serialize(ref T obj) => throw new NotImplementedException();
    }

    public struct CacheInput
    {
    }

    public interface ICacheOutput<T>
    {
        public T Value { get; set; }
    }

    public struct CacheContext
    {
        // For now we do not use Context in this test.
        public static readonly CacheContext None = default;
    }
}
