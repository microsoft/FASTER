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
    public class CacheKey : IFASTERKey<CacheKey>
    {
        public long key;

        public CacheKey() { }

        public CacheKey(long first)
        {
            key = first;
        }

        public bool Equals(CacheKey other)
        {
            return key == other.key;
        }

        public long GetHashCode64()
        {
            return Utility.GetHashCode(key);
        }

        public CacheKey Clone()
        {
            return this;
        }

        public void Deserialize(Stream fromStream)
        {
            throw new NotImplementedException();
        }

        public void Serialize(Stream toStream)
        {
            throw new NotImplementedException();
        }
    }

    public class CacheValue : IFASTERValue<CacheValue>
    {
        public long value;

        public CacheValue() { }

        public CacheValue(long first)
        {
            value = first;
        }

        public CacheValue Clone()
        {
            return this;
        }

        public void Deserialize(Stream fromStream)
        {
            throw new NotImplementedException();
        }

        public void Serialize(Stream toStream)
        {
            throw new NotImplementedException();
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

    public class CacheFunctions : IUserFunctions<CacheKey, CacheValue, CacheInput, CacheOutput, CacheContext>
    {
        public void CopyUpdater(CacheKey key, CacheInput input, CacheValue oldValue, ref CacheValue newValue)
        {
        }

        public void InitialUpdater(CacheKey key, CacheInput input, ref CacheValue value)
        {
        }

        public void InPlaceUpdater(CacheKey key, CacheInput input, ref CacheValue value)
        {
        }

        public void ReadCompletionCallback(CacheContext ctx, CacheOutput output, Status status)
        {
        }

        public void Reader(CacheKey key, CacheInput input, CacheValue value, ref CacheOutput dst)
        {
            dst.value = value;
        }

        public void RMWCompletionCallback(CacheContext ctx, Status status)
        {
        }

        public void UpsertCompletionCallback(CacheContext ctx)
        {
        }
    }
}
