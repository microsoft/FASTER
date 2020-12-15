// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.common;
using System;
using System.Buffers;

namespace FASTER.client
{
    /// <summary>
    /// Base class for callback functions on Memory
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class MemoryFunctionsBase<T> : ICallbackFunctions<ReadOnlyMemory<T>, ReadOnlyMemory<T>, ReadOnlyMemory<T>, (IMemoryOwner<T>, int), byte>
        where T : unmanaged
    {
        /// <inheritdoc />
        public virtual void DeleteCompletionCallback(ref ReadOnlyMemory<T> key, byte ctx) { }

        /// <inheritdoc />
        public virtual void ReadCompletionCallback(ref ReadOnlyMemory<T> key, ref ReadOnlyMemory<T> input, ref (IMemoryOwner<T>, int) output, byte ctx, Status status) { }

        /// <inheritdoc />
        public virtual void RMWCompletionCallback(ref ReadOnlyMemory<T> key, ref ReadOnlyMemory<T> input, byte ctx, Status status) { }

        /// <inheritdoc />
        public virtual void UpsertCompletionCallback(ref ReadOnlyMemory<T> key, ref ReadOnlyMemory<T> value, byte ctx) { }
    }
}