// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;
using System.Buffers;
using System.Diagnostics;

namespace StoreVarLenTypes
{
    /// <summary>
    /// Callback functions for FASTER sum operations over SpanByte (non-negative ASCII numbers).
    /// </summary>
    public sealed class AsciiSumSpanByteFunctions : SpanByteFunctions<long>
    {
        /// <inheritdoc/>
        public AsciiSumSpanByteFunctions(MemoryPool<byte> memoryPool = null, bool locking = false) : base(memoryPool, locking) { }

        /// <inheritdoc/>
        public override void InitialUpdater(ref SpanByte key, ref SpanByte input, ref SpanByte value)
        {
            input.CopyTo(ref value);
        }

        /// <inheritdoc/>
        public override bool InPlaceUpdater(ref SpanByte key, ref SpanByte input, ref SpanByte value)
        {
            long curr = Utils.BytesToLong(value.AsSpan());
            long next = curr + Utils.BytesToLong(input.AsSpan());
            if (Utils.NumDigits(next) > value.Length) return false;
            Utils.LongToBytes(next, value.AsSpan());
            return true;
        }

        /// <inheritdoc/>
        public override void CopyUpdater(ref SpanByte key, ref SpanByte input, ref SpanByte oldValue, ref SpanByte newValue)
        {
            long curr = Utils.BytesToLong(oldValue.AsSpan());
            long next = curr + Utils.BytesToLong(input.AsSpan());
            Debug.Assert(Utils.NumDigits(next) == newValue.Length, "Unexpected destination length in CopyUpdater");
            Utils.LongToBytes(next, newValue.AsSpan());
        }
    }

    /// <summary>
    /// Callback for length computation based on value and input.
    /// </summary>
    public class AsciiSumVLS : IVariableLengthStruct<SpanByte, SpanByte>
    {
        /// <summary>
        /// Initial length of value, when populated using given input number.
        /// We include sizeof(int) for length header.
        /// </summary>
        public int GetInitialLength(ref SpanByte input) => sizeof(int) + input.Length;

        /// <summary>
        /// Length of resulting object when doing RMW with given value and input.
        /// For ASCII sum, output is one digit longer than the max of input and old value.
        /// </summary>
        public int GetLength(ref SpanByte t, ref SpanByte input)
        {
            long curr = Utils.BytesToLong(t.AsSpan());
            long next = curr + Utils.BytesToLong(input.AsSpan());
            return sizeof(int) + Utils.NumDigits(next);
        }
    }
}