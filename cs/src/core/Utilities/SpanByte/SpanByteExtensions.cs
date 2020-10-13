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
        public static Status Upsert<Input, Output, Context, Functions>(this ClientSession<SpanByte, SpanByte, Input, Output, Context, Functions> clientSession, Span<byte> key, Span<byte> desiredValue, Context userContext = default, long serialNo = 0)
            where Functions : IFunctions<SpanByte, SpanByte, Input, Output, Context>
        {
            fixed (byte* k = key)
            fixed (byte* v = desiredValue)
                return clientSession.Upsert(ref SpanByte.FromFixedSpan(key), ref SpanByte.FromFixedSpan(desiredValue), userContext, serialNo);
        }

        /// <summary>
        /// Read with Span input
        /// </summary>
        /// <typeparam name="Input"></typeparam>
        /// <typeparam name="Output"></typeparam>
        /// <typeparam name="Context"></typeparam>
        /// <typeparam name="Functions"></typeparam>
        /// <param name="clientSession"></param>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="output"></param>
        /// <param name="userContext"></param>
        /// <param name="serialNo"></param>
        /// <returns></returns>
        public static Status Read<Input, Output, Context, Functions>(this ClientSession<SpanByte, SpanByte, Input, Output, Context, Functions> clientSession, Span<byte> key, ref Input input, ref Output output, Context userContext = default, long serialNo = 0)
            where Functions : IFunctions<SpanByte, SpanByte, Input, Output, Context>
        {
            fixed (byte* k = key)
                return clientSession.Read(ref SpanByte.FromFixedSpan(key), ref input, ref output, userContext, serialNo);
        }

        /// <summary>
        /// Read-modify-write with Span input
        /// </summary>
        /// <typeparam name="Input"></typeparam>
        /// <typeparam name="Output"></typeparam>
        /// <typeparam name="Context"></typeparam>
        /// <typeparam name="Functions"></typeparam>
        /// <param name="clientSession"></param>
        /// <param name="key"></param>
        /// <param name="input"></param>
        /// <param name="userContext"></param>
        /// <param name="serialNo"></param>
        /// <returns></returns>
        public static Status RMW<Input, Output, Context, Functions>(this ClientSession<SpanByte, SpanByte, Input, Output, Context, Functions> clientSession, Span<byte> key, ref Input input, Context userContext = default, long serialNo = 0)
            where Functions : IFunctions<SpanByte, SpanByte, Input, Output, Context>
        {
            fixed (byte* k = key)
                return clientSession.RMW(ref SpanByte.FromFixedSpan(key), ref input, userContext, serialNo);
        }

        /// <summary>
        /// Delete with Span input
        /// </summary>
        /// <typeparam name="Input"></typeparam>
        /// <typeparam name="Output"></typeparam>
        /// <typeparam name="Context"></typeparam>
        /// <typeparam name="Functions"></typeparam>
        /// <param name="clientSession"></param>
        /// <param name="key"></param>
        /// <param name="userContext"></param>
        /// <param name="serialNo"></param>
        /// <returns></returns>
        public static Status Delete<Input, Output, Context, Functions>(this ClientSession<SpanByte, SpanByte, Input, Output, Context, Functions> clientSession, Span<byte> key, Context userContext = default, long serialNo = 0)
            where Functions : IFunctions<SpanByte, SpanByte, Input, Output, Context>
        {
            fixed (byte* k = key)
                return clientSession.Delete(ref SpanByte.FromFixedSpan(key), userContext, serialNo);
        }
    }
}
