// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;

namespace MemOnlyCache
{
    public sealed class CacheKey : IFasterEqualityComparer<CacheKey>
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

    public sealed class CacheValue
    {
        public long value;

        public CacheValue(long first)
        {
            value = first;
        }
    }

    /// <summary>
    /// Callback for FASTER operations
    /// </summary>
    public sealed class CacheFunctions : SimpleFunctions<CacheKey, CacheValue> { }
}
