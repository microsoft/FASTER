// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Runtime.InteropServices;
using FASTER.client;

namespace FASTER.benchmark
{
    [StructLayout(LayoutKind.Explicit, Size = 8)]
    public struct Key
    {
        [FieldOffset(0)]
        public long value;

        public override string ToString()
        {
            return "{ " + value + " }";
        }
    }

    [StructLayout(LayoutKind.Explicit, Size = 8)]
    public struct Value
    {
        [FieldOffset(0)]
        public long value;
    }

    public struct Input
    {
        public long value;
    }

    [StructLayout(LayoutKind.Explicit)]
    public struct Output
    {
        [FieldOffset(0)]
        public Value value;
    }

    /// <summary>
    /// Empty type
    /// </summary>
    public readonly struct Empty
    {
        /// <summary>
        /// Default
        /// </summary>
        public static readonly Empty Default = default;
    }

    public struct Functions : ICallbackFunctions<Key, Value, Input, Output, Empty>
    {
        public void DeleteCompletionCallback(ref Key key, Empty ctx)
        {
        }

        public void ReadCompletionCallback(ref Key key, ref Input input, ref Output output, Empty ctx, Status status)
        {
        }

        public void RMWCompletionCallback(ref Key key, ref Input input, ref Output output, Empty ctx, Status status)
        {
        }

        public void UpsertCompletionCallback(ref Key key, ref Value value, Empty ctx)
        {
        }

        public void SubscribeKVCallback(ref Key key, ref Input input, ref Output output, Empty ctx, Status status)
        {
        }

        public void PublishCompletionCallback(ref Key key, ref Value value, Empty ctx)
        {
        }
        public void SubscribeCallback(ref Key key, ref Value value, Empty ctx)
        {
        }
    }
}
