// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;

namespace PerfTest
{
    public struct CacheBlittableValue8 : ICacheValue<CacheBlittableValue8>
    {
        public long Value { get; set; }

        public bool CompareValue(long returnedValue) => returnedValue == this.Value;
        public CacheBlittableValue8 Create(long first) => new CacheBlittableValue8 { Value = first };
    }

    public struct CacheBlittableValue16 : ICacheValue<CacheBlittableValue16>
    {
        public long Value { get; set; }
        public readonly long extra1;

        public bool CompareValue(long returnedValue) => returnedValue == this.Value;
        public CacheBlittableValue16 Create(long first) => new CacheBlittableValue16 { Value = first };
    }

    public struct CacheBlittableValue32 : ICacheValue<CacheBlittableValue32>
    {
        public long Value { get; set; }
        public readonly long extra1, extra2, extra3;

        public bool CompareValue(long returnedValue) => returnedValue == this.Value;
        public CacheBlittableValue32 Create(long first) => new CacheBlittableValue32 { Value = first };
    }

    public struct CacheBlittableValue64 : ICacheValue<CacheBlittableValue64>
    {
        public long Value { get; set; }
        public readonly long extra1, extra2, extra3, extra4, extra5, extra6, extra7;

        public bool CompareValue(long returnedValue) => returnedValue == this.Value;
        public CacheBlittableValue64 Create(long first) => new CacheBlittableValue64 { Value = first };
    }

    public struct CacheBlittableValue128 : ICacheValue<CacheBlittableValue128>
    {
        public long Value { get; set; }
        public readonly long extra1, extra2, extra3, extra4, extra5, extra6, extra7;
        public readonly long extra10, extra11, extra12, extra13, extra14, extra15, extra16, extra17;

        public bool CompareValue(long returnedValue) => returnedValue == this.Value;
        public CacheBlittableValue128 Create(long first) => new CacheBlittableValue128 { Value = first };
    }

    public struct CacheBlittableValue256 : ICacheValue<CacheBlittableValue256>
    {
        public long Value { get; set; }
        public readonly long extra1, extra2, extra3, extra4, extra5, extra6, extra7;
        public readonly long extra10, extra11, extra12, extra13, extra14, extra15, extra16, extra17;
        public readonly long extra20, extra21, extra22, extra23, extra24, extra25, extra26, extra27;
        public readonly long extra30, extra31, extra32, extra33, extra34, extra35, extra36, extra37;

        public bool CompareValue(long returnedValue) => returnedValue == this.Value;
        public CacheBlittableValue256 Create(long first) => new CacheBlittableValue256 { Value = first };
    }

    public struct CacheBlittableOutput<T> : ICacheOutput<T>
    {
        public T Value { get; set; }
    }


    public class CacheBlittableFunctions<TBlittableValue> : IFunctions<CacheKey, TBlittableValue, CacheInput, CacheBlittableOutput<TBlittableValue>, CacheContext>
        where TBlittableValue : ICacheValue<TBlittableValue>
    {
        public void ConcurrentReader(ref CacheKey key, ref CacheInput input, ref TBlittableValue value, ref CacheBlittableOutput<TBlittableValue> dst)
            => dst.Value = value;

        public bool ConcurrentWriter(ref CacheKey key, ref TBlittableValue src, ref TBlittableValue dst)
        {
            dst = src;
            return true;
        }

        public void CopyUpdater(ref CacheKey key, ref CacheInput input, ref TBlittableValue oldValue, ref TBlittableValue newValue) => throw new NotImplementedException();

        public void InitialUpdater(ref CacheKey key, ref CacheInput input, ref TBlittableValue value) => throw new NotImplementedException();

        public bool InPlaceUpdater(ref CacheKey key, ref CacheInput input, ref TBlittableValue value) => throw new NotImplementedException();

        public void CheckpointCompletionCallback(string sessionId, CommitPoint commitPoint) => throw new NotImplementedException();

        public void ReadCompletionCallback(ref CacheKey key, ref CacheInput input, ref CacheBlittableOutput<TBlittableValue> output, CacheContext ctx, Status status)
        {
            if (output.Value.Value != key.key)
                throw new Exception("Read error!");
        }

        public void RMWCompletionCallback(ref CacheKey key, ref CacheInput input, CacheContext ctx, Status status) => throw new NotImplementedException();

        public void SingleReader(ref CacheKey key, ref CacheInput input, ref TBlittableValue value, ref CacheBlittableOutput<TBlittableValue> dst)
            => dst.Value = value;

        public void SingleWriter(ref CacheKey key, ref TBlittableValue src, ref TBlittableValue dst)
            => dst = src;

        public void UpsertCompletionCallback(ref CacheKey key, ref TBlittableValue value, CacheContext ctx) => throw new NotImplementedException();

        public void DeleteCompletionCallback(ref CacheKey key, CacheContext ctx) => throw new NotImplementedException();
    }
}
