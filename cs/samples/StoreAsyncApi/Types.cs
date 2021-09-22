// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace StoreAsyncApi
{
    public class CacheKey : IFasterEqualityComparer<CacheKey>
    {
        public long key;

        public CacheKey() { }

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

    public class CacheValue
    {
        public long value;

        public CacheValue() { }

        public CacheValue(long first)
        {
            value = first;
        }
    }

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

    public struct CacheInput
    {
    }

    public struct CacheOutput
    {
        public CacheValue value;
    }

    public struct CacheContext
    {
        public int type;
        public long ticks;
    }

    public sealed class CacheFunctions : FunctionsBase<CacheKey, CacheValue, CacheInput, CacheOutput, CacheContext>
    {
        public override bool ConcurrentReader(ref CacheKey key, ref CacheInput input, ref CacheValue value, ref CacheOutput dst)
        {
            dst.value = value;
            return true;
        }

        public override void CheckpointCompletionCallback(string sessionId, CommitPoint commitPoint)
        {
            // Console.WriteLine("Session {0} reports persistence until {1}", sessionId, commitPoint.UntilSerialNo);
        }

        public override void ReadCompletionCallback(ref CacheKey key, ref CacheInput input, ref CacheOutput output, CacheContext ctx, Status status)
        {
            if (ctx.type == 0)
            {
                if (output.value.value != key.key)
                    throw new Exception("Read error!");
            }
            else
            {
                long ticks = DateTime.Now.Ticks - ctx.ticks;

                if (status == Status.NOTFOUND)
                    Console.WriteLine("Async: Value not found, latency = {0}ms", new TimeSpan(ticks).TotalMilliseconds);

                if (output.value.value != key.key)
                    Console.WriteLine("Async: Incorrect value {0} found, latency = {1}ms", output.value.value, new TimeSpan(ticks).TotalMilliseconds);
                else
                    Console.WriteLine("Async: Correct value {0} found, latency = {1}ms", output.value.value, new TimeSpan(ticks).TotalMilliseconds);
            }
        }

        public override bool SingleReader(ref CacheKey key, ref CacheInput input, ref CacheValue value, ref CacheOutput dst)
        {
            dst.value = value;
            return true;
        }
    }
}
