// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;

namespace FASTER.core
{
    /// <summary>
    /// Extensions
    /// </summary>
    public static unsafe class SpanByteExtensions
    {
        /// <summary>
        /// Upsert with Span input
        /// </summary>
        /// <param name="clientSession"></param>
        /// <param name="key"></param>
        /// <param name="desiredValue"></param>
        /// <param name="userContext"></param>
        /// <param name="serialNo"></param>
        /// <returns></returns>
        public static Status Upsert<Input, Output, Context, Functions, StoreFunctions, Allocator>(this ClientSession<SpanByte, SpanByte, Input, Output, Context, Functions, StoreFunctions, Allocator> clientSession,
                ReadOnlySpan<byte> key, ReadOnlySpan<byte> desiredValue, Context userContext = default, long serialNo = 0)
            where Functions : IFunctions<SpanByte, SpanByte, Input, Output, Context>
            where StoreFunctions : IStoreFunctions<SpanByte, SpanByte>
            where Allocator : AllocatorBase<SpanByte, SpanByte, StoreFunctions>
        {
            fixed (byte* k = key)
            fixed (byte* v = desiredValue)
                return clientSession.Upsert(SpanByte.FromFixedSpan(key), SpanByte.FromFixedSpan(desiredValue), userContext, serialNo);
        }

        /// <summary>
        /// Read with Span input
        /// </summary>
        /// <param name="clientSession"></param>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="userContext"></param>
        /// <param name="serialNo"></param>
        /// <returns></returns>
        public static Status Read<Input, Output, Context, Functions, StoreFunctions, Allocator>(this ClientSession<SpanByte, SpanByte, Input, Output, Context, Functions, StoreFunctions, Allocator> clientSession,
                ReadOnlySpan<byte> key, ref Input input, ref Output output, Context userContext = default, long serialNo = 0)
            where Functions : IFunctions<SpanByte, SpanByte, Input, Output, Context>
            where StoreFunctions : IStoreFunctions<SpanByte, SpanByte>
            where Allocator : AllocatorBase<SpanByte, SpanByte, StoreFunctions>
        {
            fixed (byte* k = key)
            {
                var _key = SpanByte.FromFixedSpan(key);
                return clientSession.Read(ref _key, ref input, ref output, userContext, serialNo);
            }
        }

        /// <summary>
        /// Read-modify-write with Span input
        /// </summary>
        /// <param name="clientSession"></param>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="userContext"></param>
        /// <param name="serialNo"></param>
        /// <returns></returns>
        public static Status RMW<Input, Output, Context, Functions, StoreFunctions, Allocator>(this ClientSession<SpanByte, SpanByte, Input, Output, Context, Functions, StoreFunctions, Allocator> clientSession,
                ReadOnlySpan<byte> key, ref Input input, Context userContext = default, long serialNo = 0)
            where Functions : IFunctions<SpanByte, SpanByte, Input, Output, Context>
            where StoreFunctions : IStoreFunctions<SpanByte, SpanByte>
            where Allocator : AllocatorBase<SpanByte, SpanByte, StoreFunctions>
        {
            fixed (byte* k = key)
            {
                var _key = SpanByte.FromFixedSpan(key);
                return clientSession.RMW(ref _key, ref input, userContext, serialNo);
            }
        }

        /// <summary>
        /// Delete with Span input
        /// </summary>
        /// <param name="clientSession"></param>
        /// <param name="key"></param>
        /// <param name="userContext"></param>
        /// <param name="serialNo"></param>
        /// <returns></returns>
        public static Status Delete<Input, Output, Context, Functions, StoreFunctions, Allocator>(this ClientSession<SpanByte, SpanByte, Input, Output, Context, Functions, StoreFunctions, Allocator> clientSession,
                ReadOnlySpan<byte> key, Context userContext = default, long serialNo = 0)
            where Functions : IFunctions<SpanByte, SpanByte, Input, Output, Context>
            where StoreFunctions : IStoreFunctions<SpanByte, SpanByte>
            where Allocator : AllocatorBase<SpanByte, SpanByte, StoreFunctions>
        {
            fixed (byte* k = key)
            {
                var _key = SpanByte.FromFixedSpan(key);
                return clientSession.Delete(ref _key, userContext, serialNo);
            }
        }
    }
}
