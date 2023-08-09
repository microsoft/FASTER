// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System.Threading;

namespace ResizableCacheStore
{
    public class CacheKey : IFasterEqualityComparer<CacheKey>, ISizeTracker
    {
        public long key;
        public byte[] extra;

        public CacheKey() { }

        public CacheKey(long key, int extraSize = 0)
        {
            this.key = key;
            extra = new byte[extraSize];
        }

        public long GetHashCode64(ref CacheKey key) => Utility.GetHashCode(key.key);

        public bool Equals(ref CacheKey k1, ref CacheKey k2) => k1.key == k2.key;

        public int GetSize => sizeof(long) + extra.Length + 48; // heap size incl. ~48 bytes ref/array overheads

        public override string ToString() => $"key {key}, len {extra.Length}";
    }

    public class CacheKeySerializer : BinaryObjectSerializer<CacheKey>
    {
        public override void Deserialize(out CacheKey obj)
        {
            obj = new CacheKey();
            obj.key = reader.ReadInt64();
            int size = reader.ReadInt32();
            obj.extra = reader.ReadBytes(size);
        }

        public override void Serialize(ref CacheKey obj)
        {
            writer.Write(obj.key);
            writer.Write(obj.extra.Length);
            writer.Write(obj.extra);
        }
    }

    public sealed class CacheValue : ISizeTracker
    {
        public byte[] value;

        public CacheValue(int size, byte firstByte)
        {
            value = new byte[size];
            value[0] = firstByte;
        }

        public CacheValue(byte[] serializedValue)
        {
            value = serializedValue;
        }

        public int GetSize => value.Length + 48; // heap size for byte array incl. ~48 bytes ref/array overheads

        public override string ToString() => $"value[0] {value[0]}, len {value.Length}";
    }

    public class CacheValueSerializer : BinaryObjectSerializer<CacheValue>
    {
        public override void Deserialize(out CacheValue obj)
        {
            int size = reader.ReadInt32();
            obj = new CacheValue(reader.ReadBytes(size));
        }

        public override void Serialize(ref CacheValue obj)
        {
            writer.Write(obj.value.Length);
            writer.Write(obj.value);
        }
    }

    /// <summary>
    /// Callback for FASTER operations
    /// </summary>
    public sealed class CacheFunctions : SimpleFunctions<CacheKey, CacheValue>
    {
        readonly CacheSizeTracker sizeTracker;

        public CacheFunctions(CacheSizeTracker sizeTracker)
        {
            this.sizeTracker = sizeTracker;
        }

        public override bool ConcurrentWriter(ref CacheKey key, ref CacheValue input, ref CacheValue src, ref CacheValue dst, ref CacheValue output, ref UpsertInfo upsertInfo)
        {
            var old = Interlocked.Exchange(ref dst, src);
            sizeTracker.AddTrackedSize(dst.GetSize - old.GetSize);
            return true;
        }

        public override void PostSingleWriter(ref CacheKey key, ref CacheValue input, ref CacheValue src, ref CacheValue dst, ref CacheValue output, ref UpsertInfo upsertInfo, WriteReason reason)
        {
            dst = src;
            sizeTracker.AddTrackedSize(key.GetSize + src.GetSize, reason == WriteReason.CopyToReadCache);
        }

        public override bool ConcurrentDeleter(ref CacheKey key, ref CacheValue value, ref DeleteInfo deleteInfo)
        {
            sizeTracker.AddTrackedSize(-value.GetSize);

            // If the record is marked invalid (failed to insert), dispose key as well
            if (deleteInfo.RecordInfo.Invalid)
                sizeTracker.AddTrackedSize(-key.GetSize);
            return true;
        }

        public override void PostSingleDeleter(ref CacheKey key, ref DeleteInfo deleteInfo)
        {
            sizeTracker.AddTrackedSize(-key.GetSize);
        }

        public override void PostInitialUpdater(ref CacheKey key, ref CacheValue input, ref CacheValue value, ref CacheValue output, ref RMWInfo rmwInfo)
        {
            value = input;
            sizeTracker.AddTrackedSize(key.GetSize + input.GetSize);
        }

        public override void PostCopyUpdater(ref CacheKey key, ref CacheValue input, ref CacheValue oldValue, ref CacheValue newValue, ref CacheValue output, ref RMWInfo rmwInfo)
        {
            newValue = oldValue;
            sizeTracker.AddTrackedSize(key.GetSize + oldValue.GetSize);
        }

        public override bool InPlaceUpdater(ref CacheKey key, ref CacheValue input, ref CacheValue value, ref CacheValue output, ref RMWInfo rmwInfo)
        {
            var old = Interlocked.Exchange(ref value, input);
            sizeTracker.AddTrackedSize(value.GetSize - old.GetSize);
            return true;
        }

        /// <inheritdoc/>
        public override void DisposeDeserializedFromDisk(ref CacheKey key, ref CacheValue value)
        {
            sizeTracker.AddTrackedSize(-key.GetSize - value.GetSize);
        }

        public override void DisposeForRevivification(ref CacheKey key, ref CacheValue value, int newKeySize)
        {
            if (newKeySize >= 0 && key is not null)
                sizeTracker.AddTrackedSize(-key.GetSize);
            if (value is not null)
                sizeTracker.AddTrackedSize(-value.GetSize);
        }
    }
}
