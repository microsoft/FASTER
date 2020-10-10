// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;
using System.Linq;

namespace StoreVarLenTypes
{
    public class Program
    {
        // This sample shows how our special type called SpanByte can be leverage to use FASTER
        // with variable-length keys and/or values without a separate object log. SpanBytes can
        // easily be created using pinned or fixed memory. A SpanByte is basically a sequence of
        // bytes with a 4-byte integer length header that denotes the size of the payload.
        //
        // Underlying SpanByte is the use of "ref struct" as a proxy for pointers to variable-sized 
        // memory in C# (we call these VariableLengthStructs).
        //
        // Objects are placed contiguously in a single log, leading to efficient packing while
        // avoiding the additional I/O (on reads and writes) that a separate object log entails.
        // Serializers are not required, as these are effectively value-types.

        static void Main()
        {
            // VarLen types do not need an object log
            var log = Devices.CreateLogDevice("hlog.log", deleteOnClose: true);

            // Create store
            // For custom varlen (not SpanByte), you need to provide IVariableLengthStructSettings and IFasterEqualityComparer
            var store = new FasterKV<SpanByte, SpanByte>(
                size: 1L << 20, 
                logSettings: new LogSettings { LogDevice = log, MemorySizeBits = 17, PageSizeBits = 12 });
            
            // Create session
            var s = store.For(new Functions()).NewSession<Functions>();

            Random r = new Random(100);

            for (byte i = 0; i < 100; i++)
            {
                var keyLen = r.Next(1, 10);
                Span<byte> keyData = stackalloc byte[keyLen + sizeof(int)];
                keyData.Slice(sizeof(int)).Fill(i);
                ref var key = ref SpanByte.FromFixedSpan(keyData);

                var valLen = r.Next(1, 10);
                Span<byte> valueData = stackalloc byte[valLen + sizeof(int)];
                valueData.Slice(sizeof(int)).Fill((byte)valLen);
                ref var value = ref SpanByte.FromFixedSpan(valueData);

                s.Upsert(ref key, ref value);
            }

            bool success = true;
            
            r = new Random(100);
            for (byte i = 0; i < 100; i++)
            {
                var keyLen = r.Next(1, 10);
                Span<byte> keyData = stackalloc byte[keyLen + sizeof(int)];
                keyData.Slice(sizeof(int)).Fill(i);
                ref var key = ref SpanByte.FromFixedSpan(keyData);

                byte[] output = default;
                var status = s.Read(ref key, ref output);

                var valLen = r.Next(1, 10);
                Span<byte> expectedValueData = stackalloc byte[valLen + sizeof(int)];
                expectedValueData.Slice(sizeof(int)).Fill((byte)valLen);
                ref var expectedValue = ref SpanByte.FromFixedSpan(expectedValueData);

                if (status == Status.PENDING)
                {
                    s.CompletePending(true);
                }
                else
                {
                    if ((status != Status.OK) || (!output.SequenceEqual(expectedValue.ToByteArray())))
                    {
                        success = false;
                        break;
                    }
                }
            }

            if (success)
                Console.WriteLine("Success!");
            else
                Console.WriteLine("Error!");

            s.Dispose();
            store.Dispose();
            log.Dispose();

            Console.WriteLine("Press <ENTER> to end");
            Console.ReadLine();
        }
    }
}