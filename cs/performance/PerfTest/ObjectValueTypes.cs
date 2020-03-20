// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;

namespace FASTER.PerfTest
{
    public class CacheObjectValue : ICacheValue<CacheObjectValue>
    {
        public static int VectorSize => CacheGlobals.DataSize - CacheGlobals.MinDataSize;

        public long Value
        {
            get => value;
            set
            {
                this.value = value;
                this.vector = VectorSize > 0 ? new byte[VectorSize] : null;
            }
        }
        internal byte[] vector;
        private long value;

        public CacheObjectValue() { }

        public bool CompareValue(long returnedValue) => returnedValue == this.Value;

        public CacheObjectValue Create(long first) => new CacheObjectValue { Value = first };
    }

    public class CacheObjectSerializer : BinaryObjectSerializer<CacheObjectValue>
    {
        public override void Deserialize(ref CacheObjectValue obj)
        {
            obj.Value = reader.ReadInt64();
            obj.vector = CacheObjectValue.VectorSize > 0 ? reader.ReadBytes(CacheObjectValue.VectorSize) : null;
        }

        public override void Serialize(ref CacheObjectValue obj)
        {
            writer.Write(obj.Value);
            if (!(obj.vector is null))
                writer.Write(obj.vector);
        }
    }

    public struct CacheObjectOutput : ICacheOutput<CacheObjectValue>
    {
        public CacheObjectValue Value { get; set; }
    }

    public class CacheObjectFunctions : IFunctions<CacheKey, CacheObjectValue, CacheInput, CacheObjectOutput, CacheContext>
    {
        public void ConcurrentReader(ref CacheKey key, ref CacheInput input, ref CacheObjectValue value, ref CacheObjectOutput dst) 
            => dst.Value = value;

        public bool ConcurrentWriter(ref CacheKey key, ref CacheObjectValue src, ref CacheObjectValue dst)
        {
            dst = src;
            return true;
        }

        public void CopyUpdater(ref CacheKey key, ref CacheInput input, ref CacheObjectValue oldValue, ref CacheObjectValue newValue) => throw new NotImplementedException();

        public void InitialUpdater(ref CacheKey key, ref CacheInput input, ref CacheObjectValue value) => throw new NotImplementedException();

        public bool InPlaceUpdater(ref CacheKey key, ref CacheInput input, ref CacheObjectValue value) => throw new NotImplementedException();

        public void CheckpointCompletionCallback(string sessionId, CommitPoint commitPoint) => throw new NotImplementedException();

        public void ReadCompletionCallback(ref CacheKey key, ref CacheInput input, ref CacheObjectOutput output, CacheContext ctx, Status status)
        {
            if (output.Value.Value != key.key)
                throw new Exception("Read error!");
        }

        public void RMWCompletionCallback(ref CacheKey key, ref CacheInput input, CacheContext ctx, Status status) => throw new NotImplementedException();

        public void SingleReader(ref CacheKey key, ref CacheInput input, ref CacheObjectValue value, ref CacheObjectOutput dst) 
            => dst.Value = value;

        public void SingleWriter(ref CacheKey key, ref CacheObjectValue src, ref CacheObjectValue dst) 
            => dst = src;

        public void UpsertCompletionCallback(ref CacheKey key, ref CacheObjectValue value, CacheContext ctx) => throw new NotImplementedException();

        public void DeleteCompletionCallback(ref CacheKey key, CacheContext ctx) => throw new NotImplementedException();
    }
}
