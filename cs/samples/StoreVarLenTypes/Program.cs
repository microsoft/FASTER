// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;
using System.Buffers;
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
            MemoryByteSample();
            SpanByteSample();
            return;
        }

        static void SpanByteSample()
        {
            // VarLen types do not need an object log
            var log = Devices.CreateLogDevice("hlog.log", deleteOnClose: true);

            // Create store
            // For custom varlen (not SpanByte), you need to provide IVariableLengthStructSettings and IFasterEqualityComparer
            var store = new FasterKV<SpanByte, SpanByte>(
                size: 1L << 20, 
                logSettings: new LogSettings { LogDevice = log, MemorySizeBits = 15, PageSizeBits = 12 });
            
            // Create session
            var s = store.For(new Functions()).NewSession<Functions>();

            Random r = new Random(100);

            for (byte i = 0; i < 200; i++)
            {
                var keyLen = r.Next(1, 1000);
                Span<byte> key = stackalloc byte[keyLen];
                key.Fill(i);

                var valLen = r.Next(1, 1000);
                Span<byte> value = stackalloc byte[valLen];
                value.Fill((byte)valLen);

                // Option 1: Using overload for Span<byte>
                s.Upsert(key, value);
            }

            bool success = true;
            
            r = new Random(100);
            for (byte i = 0; i < 200; i++)
            {
                var keyLen = r.Next(1,1000);
                Span<byte> keyData = stackalloc byte[keyLen];
                keyData.Fill(i);

                // Option 2: Converting fixed Span<byte> to SpanByte
                var status = s.Read(SpanByte.FromFixedSpan(keyData), out byte[] output);

                var valLen = r.Next(1, 1000);
                Span<byte> expectedValue = stackalloc byte[valLen];
                expectedValue.Fill((byte)valLen);

                if (status == Status.PENDING)
                {
                    s.CompletePending(true);
                }
                else
                {
                    if ((status != Status.OK) || (!output.SequenceEqual(expectedValue.ToArray())))
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

        static void MemoryByteSample()
        {
            // VarLen types do not need an object log
            var log = Devices.CreateLogDevice("hlog.log", deleteOnClose: true);

            // Create store
            // For custom varlen (not SpanByte), you need to provide IVariableLengthStructSettings and IFasterEqualityComparer
            var store = new FasterKV<Memory<byte>, Memory<byte>>(
                size: 1L << 20,
                logSettings: new LogSettings { LogDevice = log, MemorySizeBits = 15, PageSizeBits = 12 });

            // Create session
            var s = store.For(new MyMemoryFunctions()).NewSession<MemoryFunctions>();

            Random r = new Random(100);

            for (byte i = 0; i < 200; i++)
            {
                var keyLen = r.Next(1, 1000);
                var key = new Memory<byte>(new byte[keyLen]);
                key.Span.Fill(i);

                var valLen = r.Next(1, 1000);
                var value = new Memory<byte>(new byte[valLen]);
                value.Span.Fill((byte)valLen);

                // Option 1: Using overload for Span<byte>
                s.Upsert(key, value);
            }

            bool success = true;

            r = new Random(100);
            for (byte i = 0; i < 200; i++)
            {
                var keyLen = r.Next(1, 1000);
                var key = new Memory<byte>(new byte[keyLen]);
                key.Span.Fill(i);

                // Option 2: Converting fixed Span<byte> to SpanByte
                var status = s.Read(key, out var output);

                var valLen = r.Next(1, 1000);
                var expectedValue = new Memory<byte>(new byte[valLen]);
                expectedValue.Span.Fill((byte)valLen);

                if (status == Status.PENDING)
                {
                    s.CompletePending(true);
                }
                else
                {
                    if ((status != Status.OK) || (!output.Item1.Memory.Slice(0, output.Item2).Span.SequenceEqual(expectedValue.Span)))
                    {
                        output.Item1.Dispose();
                        success = false;
                        break;
                    }
                    output.Item1.Dispose();
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