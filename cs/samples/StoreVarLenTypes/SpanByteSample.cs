// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Linq;
using FASTER.core;

namespace StoreVarLenTypes
{
    /// <summary>
    /// This sample shows how our special type called SpanByte can be leverage to use FASTER
    /// with variable-length keys and/or values without a separate object log. SpanBytes can
    /// easily be created using pinned or fixed memory. A SpanByte is basically a sequence of
    /// bytes with a 4-byte integer length header that denotes the size of the payload.
    ///
    /// Underlying SpanByte is the use of "ref struct" as a proxy for pointers to variable-sized 
    /// memory in C# (we call these VariableLengthStructs).
    /// </summary>
    public class SpanByteSample
    {
        public static void Run()
        {
            // VarLen types do not need an object log
            var log = Devices.CreateLogDevice("hlog.log", deleteOnClose: true);

            FasterKVSettings<SpanByte, SpanByte> fkvSettings = new()
            {
                IndexSize = 1L << 26,
                LogDevice = log,
                MemorySize = 1L << 15,
                PageSize = 1L << 12
            };

            // Create store
            // For custom varlen (not SpanByte), you need to provide IVariableLengthStructSettings and IFasterEqualityComparer
            FasterKV<SpanByte, SpanByte> store = new(fkvSettings);

            // Create session
            var s = store.For(new CustomSpanByteFunctions()).NewSession<CustomSpanByteFunctions>();

            Random r = new(100);

            // Here, stackalloc implies fixed, so it can be used directly with SpanByte
            // For Span<byte> over heap data (e.g., strings or byte[]), make sure to use
            // fixed before the FASTER Read/Upsert/RMW operation.
            Span<byte> keyMem = stackalloc byte[1000];
            Span<byte> valueMem = stackalloc byte[1000];

            byte i;
            for (i = 0; i < 200; i++)
            {
                var keyLen = r.Next(1, 1000);
                var key = keyMem.Slice(0, keyLen);
                key.Fill(i);

                var valLen = r.Next(1, 1000);
                var value = valueMem.Slice(0, valLen);
                value.Fill((byte)valLen);

                // Option 1: Using overload for Span<byte>
                s.Upsert(key, value);
            }

            bool success = true;

            i = 0;
            using (IFasterScanIterator<SpanByte, SpanByte> iterator = store.Log.Scan(store.Log.BeginAddress, store.Log.TailAddress))
            {
                while (iterator.GetNext(out RecordInfo recordInfo))
                {
                    ref var key = ref iterator.GetKey();
                    if (key.ToByteArray()[0] != i++)
                    {
                        success = false;
                        break;
                    }
                }
            }

            if (i != 200)
            {
                success = false;
            }

            r = new Random(100);
            for (i = 0; i < 200; i++)
            {
                var keyLen = r.Next(1, 1000);
                Span<byte> key = keyMem.Slice(0, keyLen);
                key.Fill(i);

                var valLen = r.Next(1, 1000);

                // Option 2: Converting fixed Span<byte> to SpanByte
                var status = s.Read(SpanByte.FromFixedSpan(key), out byte[] output, userContext: (byte)valLen);

                var expectedValue = valueMem.Slice(0, valLen);
                expectedValue.Fill((byte)valLen);

                if (status.IsPending)
                {
                    s.CompletePending(true);
                }
                else
                {
                    if (!status.Found || (!output.SequenceEqual(expectedValue.ToArray())))
                    {
                        success = false;
                        break;
                    }
                }
            }

            if (success)
                Console.WriteLine("SpanByteSample: Success!");
            else
                Console.WriteLine("Error!");

            s.Dispose();
            store.Dispose();
            log.Dispose();
        }
    }
}