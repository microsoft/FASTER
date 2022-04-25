// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Buffers;

namespace FASTER.core
{
    /// <summary>
    /// IFunctions base implementation for Memory{T} values, for blittable (unmanaged) type T 
    /// </summary>
    /// <remarks>Can also use <see cref="ReadOnlyMemory{T}"/> for Input, with <see cref="MemoryVarLenStructForReadOnlyMemoryInput{ThisAssembly}"/> for the SessionVarLen type</remarks>
    public class MemoryFunctions<Key, T, Context> : FunctionsBase<Key, Memory<T>, Memory<T>, (IMemoryOwner<T>, int), Context, MemoryVarLenStructForMemoryInput<T>>
        where T : unmanaged
    {
        readonly MemoryPool<T> memoryPool;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="memoryPool"></param>
        public MemoryFunctions(MemoryPool<T> memoryPool = default) : base(new MemoryVarLenStructForMemoryInput<T>())
        {
            this.memoryPool = memoryPool ?? MemoryPool<T>.Shared;
        }

        /// <inheritdoc/>
        public override bool SingleWriter(ref Key key, ref Memory<T> input, ref Memory<T> src, ref Memory<T> dst, ref (IMemoryOwner<T>, int) output, ref UpsertInfo upsertInfo, WriteReason reason)
        {
            src.CopyTo(dst);
            return true;
        }

        /// <inheritdoc/>
        public override bool ConcurrentWriter(ref Key key, ref Memory<T> input, ref Memory<T> src, ref Memory<T> dst, ref (IMemoryOwner<T>, int) output, ref UpsertInfo upsertInfo)
        {
            if (dst.Length < src.Length)
            {
                return false;
            }

            // Option 1: write the source data, leaving the destination size unchanged. You will need
            // to manage the actual space used by the value if you stop here.
            src.CopyTo(dst);

            // We can adjust the length header on the serialized log, if we wish to.
            // This method will also zero out the extra space to retain log scan correctness.
            dst.ShrinkSerializedLength(src.Length);
            return true;
        }

        /// <inheritdoc/>
        public override bool SingleReader(ref Key key, ref Memory<T> input, ref Memory<T> value, ref (IMemoryOwner<T>, int) dst, ref ReadInfo readInfo)
        {
            dst.Item1 = memoryPool.Rent(value.Length);
            dst.Item2 = value.Length;
            value.CopyTo(dst.Item1.Memory);
            return true;
        }

        /// <inheritdoc/>
        public override bool ConcurrentReader(ref Key key, ref Memory<T> input, ref Memory<T> value, ref (IMemoryOwner<T>, int) dst, ref ReadInfo readInfo)
        {
            dst.Item1 = memoryPool.Rent(value.Length);
            dst.Item2 = value.Length;
            value.CopyTo(dst.Item1.Memory);
            return true;
        }

        /// <inheritdoc/>
        public override bool InitialUpdater(ref Key key, ref Memory<T> input, ref Memory<T> value, ref (IMemoryOwner<T>, int) output, ref RMWInfo rmwInfo)
        {
            input.CopyTo(value);
            return true;
        }

        /// <inheritdoc/>
        public override bool CopyUpdater(ref Key key, ref Memory<T> input, ref Memory<T> oldValue, ref Memory<T> newValue, ref (IMemoryOwner<T>, int) output, ref RMWInfo rmwInfo)
        {
            oldValue.CopyTo(newValue);
            return true;
        }

        /// <inheritdoc/>
        public override bool InPlaceUpdater(ref Key key, ref Memory<T> input, ref Memory<T> value, ref (IMemoryOwner<T>, int) output, ref RMWInfo rmwInfo)
        {
            // The default implementation of IPU simply writes input to destination, if there is space
            UpsertInfo upsertInfo = new(ref rmwInfo);
            return ConcurrentWriter(ref key, ref input, ref input, ref value, ref output, ref upsertInfo);
        }
    }
}