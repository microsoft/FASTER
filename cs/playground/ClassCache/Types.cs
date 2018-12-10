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
    public class CacheKey : IKey<CacheKey>
    {
        public long key;

        public CacheKey() { }

        public CacheKey(long first)
        {
            key = first;
        }

        public bool Equals(ref CacheKey other)
        {
            return key == other.key;
        }

        public long GetHashCode64()
        {
            return Utility.GetHashCode(key);
        }

        public void Deserialize(Stream fromStream)
        {
            throw new NotImplementedException();
        }

        public void Serialize(Stream toStream)
        {
            throw new NotImplementedException();
        }

        public int GetLength()
        {
            return 8;
        }

        public void ShallowCopy(ref CacheKey dst)
        {
            dst = this;
        }

        public bool HasObjectsToSerialize()
        {
            return true;
        }
    }

    public class CacheValue : IValue<CacheValue>
    {
        public long value;

        public CacheValue() { }

        public CacheValue(long first)
        {
            value = first;
        }

        public void Deserialize(Stream fromStream)
        {
        }

        public int GetLength()
        {
            return 8;
        }

        public bool HasObjectsToSerialize()
        {
            return true;
        }

        public void Serialize(Stream toStream)
        {
        }

        public void ShallowCopy(ref CacheValue dst)
        {
            dst = this;
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
    }

    public class CacheFunctions : IFunctions<CacheKey, CacheValue, CacheInput, CacheOutput, CacheContext>
    {
        public void ConcurrentReader(ref CacheKey key, ref CacheInput input, ref CacheValue value, ref CacheOutput dst)
        {
            dst.value = value;
        }

        public void ConcurrentWriter(ref CacheKey key, ref CacheValue src, ref CacheValue dst)
        {
            dst = src;
        }

        public void CopyUpdater(ref CacheKey key, ref CacheInput input, ref CacheValue oldValue, ref CacheValue newValue)
        {
            throw new NotImplementedException();
        }

        public void InitialUpdater(ref CacheKey key, ref CacheInput input, ref CacheValue value)
        {
            throw new NotImplementedException();
        }

        public int InitialValueLength(ref CacheKey key, ref CacheInput input)
        {
            throw new NotImplementedException();
        }

        public void InPlaceUpdater(ref CacheKey key, ref CacheInput input, ref CacheValue value)
        {
            throw new NotImplementedException();
        }

        public void PersistenceCallback(long thread_id, long serial_num)
        {
            throw new NotImplementedException();
        }

        public void ReadCompletionCallback(ref CacheKey key, ref CacheInput input, ref CacheOutput output, ref CacheContext ctx, Status status)
        {
            throw new NotImplementedException();
        }

        public void RMWCompletionCallback(ref CacheKey key, ref CacheInput input, ref CacheContext ctx, Status status)
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

        public void UpsertCompletionCallback(ref CacheKey key, ref CacheValue value, ref CacheContext ctx)
        {
            throw new NotImplementedException();
        }
    }
}
