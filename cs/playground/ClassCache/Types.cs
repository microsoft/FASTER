// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ClassCache
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
        public override void Deserialize(ref CacheKey obj)
        {
            obj.key = reader.ReadInt64();
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
        public override void Deserialize(ref CacheValue obj)
        {
            obj.value = reader.ReadInt64();
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

    public class CacheFunctions : IFunctions<CacheKey, CacheValue, CacheInput, CacheOutput, CacheContext>
    {
        public void ConcurrentReader(ref CacheKey key, ref CacheInput input, ref CacheValue value, ref CacheOutput dst)
        {
            dst.value = value;
        }

        public bool ConcurrentWriter(ref CacheKey key, ref CacheValue src, ref CacheValue dst)
        {
            dst = src;
            return true;
        }

        public void CopyUpdater(ref CacheKey key, ref CacheInput input, ref CacheValue oldValue, ref CacheValue newValue)
        {
            throw new NotImplementedException();
        }

        public void InitialUpdater(ref CacheKey key, ref CacheInput input, ref CacheValue value)
        {
            throw new NotImplementedException();
        }

        public bool InPlaceUpdater(ref CacheKey key, ref CacheInput input, ref CacheValue value)
        {
            throw new NotImplementedException();
        }

        public void CheckpointCompletionCallback(Guid sessionId, CommitPoint commitPoint)
        {
            throw new NotImplementedException();
        }

        public void ReadCompletionCallback(ref CacheKey key, ref CacheInput input, ref CacheOutput output, CacheContext ctx, Status status)
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

        public void RMWCompletionCallback(ref CacheKey key, ref CacheInput input, CacheContext ctx, Status status)
        {
            throw new NotImplementedException();
        }

        public void SingleReader(ref CacheKey key, ref CacheInput input, ref CacheValue value, ref CacheOutput dst)
        {
            dst.value = value;
        }

        public void SingleWriter(ref CacheKey key, ref CacheValue src, ref CacheValue dst)
        {
            dst = src;
        }

        public void UpsertCompletionCallback(ref CacheKey key, ref CacheValue value, CacheContext ctx)
        {
            throw new NotImplementedException();
        }

        public void DeleteCompletionCallback(ref CacheKey key, CacheContext ctx)
        {
            throw new NotImplementedException();
        }
    }
}
