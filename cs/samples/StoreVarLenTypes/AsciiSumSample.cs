// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Linq;
using FASTER.core;

#pragma warning disable CS0162 // Unreachable code detected

namespace StoreVarLenTypes
{
    /// <summary>
    /// Sample uses SpanByte key and value types, stores ASCII representation of numbers in values
    /// RMW is defined as the sum operation over non-negative ASCII numbers: (input, value) => input + value
    /// </summary>
    public class AsciiSumSample
    {
        public static void Run()
        {
            const bool useRmw = true;
            const int baseNumber = 45;

            // VarLen types do not need an object log
            using var log = Devices.CreateLogDevice("hlog.log", deleteOnClose: true);

            // Create store
            // For custom varlen (not SpanByte), you need to provide IVariableLengthStructSettings and IFasterEqualityComparer.
            // For this test we require record-level locking
            using var store = new FasterKV<SpanByte, SpanByte>(
                size: 1L << 20,
                logSettings: new LogSettings { LogDevice = log, MemorySizeBits = 15, PageSizeBits = 12 }, disableLocking: false);

            // Create session for ASCII sums. We require two callback function types to be provided:
            //    AsciiSumSpanByteFunctions implements RMW callback functions
            //    AsciiSumVLS implements the callback for computing the length of the result new value, given an old value and an input
            using var s = store.For(new AsciiSumSpanByteFunctions()).NewSession<AsciiSumSpanByteFunctions>
                (sessionVariableLengthStructSettings: new SessionVariableLengthStructSettings<SpanByte, SpanByte> { valueLength = new AsciiSumVLS() });

            // Create key
            Span<byte> key = stackalloc byte[10];
            key.Fill(23);
            var _key = SpanByte.FromFixedSpan(key);

            // Create input
            Span<byte> input = stackalloc byte[10];
            int inputLength = Utils.LongToBytes(baseNumber, input);
            var _input = SpanByte.FromFixedSpan(input.Slice(0, inputLength));

            if (useRmw)
            {
                s.RMW(_key, _input); // InitialUpdater to 45 (baseNumber)
                s.RMW(_key, _input); // InPlaceUpdater to 90
                s.RMW(_key, _input); // CopyUpdater to 135 (due to value size increase)
                s.RMW(_key, _input); // InPlaceUpdater to 180

                store.Log.Flush(true); // Flush by moving ReadOnly address to Tail (retain records in memory)
                s.RMW(_key, _input); // CopyUpdater to 225 (due to immutable source value in read-only region)

                store.Log.FlushAndEvict(true); // Flush and evict all records to disk
                var _status = s.RMW(_key, _input); // CopyUpdater to 270 (due to immutable source value on disk)

                if (_status.Pending)
                {
                    Console.WriteLine("Error!");
                    return;
                }
                s.CompletePending(true); // Wait for IO completion
            }
            else
            {
                s.Upsert(_key, _input);
            }

            // Create output space
            Span<byte> output = stackalloc byte[10];
            var outputWrapper = new SpanByteAndMemory(SpanByte.FromFixedSpan(output));

            var status = s.Read(ref _key, ref outputWrapper);

            // Read does not go pending, and the output should fit in the provided space (10 bytes)
            // Hence, no Memory will be allocated by FASTER
            if (!status.Found || !outputWrapper.IsSpanByte)
            {
                Console.WriteLine("Error!");
                return;
            }

            // Check result value correctness
            if (useRmw)
                inputLength = Utils.LongToBytes(baseNumber * 6, input);

            if (!outputWrapper.SpanByte.AsReadOnlySpan().SequenceEqual(input.Slice(0, inputLength)))
            {
                Console.WriteLine("Error!");
                return;
            }

            Console.WriteLine("AsciiSumSample: Success!");
        }
    }
}