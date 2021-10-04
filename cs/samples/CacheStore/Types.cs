// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;
using System.Diagnostics;

namespace CacheStore
{
    public struct CacheKey : IFasterEqualityComparer<CacheKey>
    {
        public long key;

        public CacheKey(long first)
        {
            key = first;
        }

        public long GetHashCode64(ref CacheKey key)
        {
            return Utility.GetHashCode(key.key);
        }
        public bool Equals(ref CacheKey k1, ref CacheKey k2)
        {
            return k1.key == k2.key;
        }
    }

    public struct CacheValue
    {
        public long value;

        public CacheValue(long first)
        {
            value = first;
        }
    }

    /// <summary>
    /// Serializer for CacheKey - used if CacheKey is changed from struct to class
    /// </summary>
    public class CacheKeySerializer : BinaryObjectSerializer<CacheKey>
    {
        public override void Deserialize(out CacheKey obj)
        {
            obj = new CacheKey(reader.ReadInt64());
        }

        public override void Serialize(ref CacheKey obj)
        {
            writer.Write(obj.key);
        }
    }

    /// <summary>
    /// Serializer for CacheValue - used if CacheValue is changed from struct to class
    /// </summary>
    public class CacheValueSerializer : BinaryObjectSerializer<CacheValue>
    {
        public override void Deserialize(out CacheValue obj)
        {
            obj = new CacheValue(reader.ReadInt64());
        }

        public override void Serialize(ref CacheValue obj)
        {
            writer.Write(obj.value);
        }
    }

    /// <summary>
    /// User context to measure latency and/or check read result
    /// </summary>
    public struct CacheContext
    {
        public int type;
        public long ticks;
    }

    /// <summary>
    /// Callback for FASTER operations
    /// </summary>
    public class CacheFunctions : SimpleFunctions<CacheKey, CacheValue, CacheContext>
    {
        public override void ReadCompletionCallback(ref CacheKey key, ref CacheValue input, ref CacheValue output, CacheContext ctx, Status status, RecordInfo recordInfo)
        {
            if (ctx.type == 0)
            {
                if (output.value != key.key)
                    throw new Exception("Read error!");
            }
            else
            {
                long ticks = Stopwatch.GetTimestamp() - ctx.ticks;

                if (status == Status.NOTFOUND)
                    Console.WriteLine("Async: Value not found, latency = {0}ms", 1000 * (ticks - ctx.ticks) / (double)Stopwatch.Frequency);

                if (output.value != key.key)
                    Console.WriteLine("Async: Incorrect value {0} found, latency = {1}ms", output.value, new TimeSpan(ticks).TotalMilliseconds);
                else
                    Console.WriteLine("Async: Correct value {0} found, latency = {1}ms", output.value, new TimeSpan(ticks).TotalMilliseconds);
            }
        }
    }
}
