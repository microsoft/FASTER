// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;
using System.Linq;

namespace StoreVarLenTypes
{
    /// <summary>
    /// In these samples, we support variable-length keys and values with a single log using
    /// (1) Memory&lt;T&gt; and ReadOnlyMemory&lt;T&gt; where T : unmanaged (e.g., byte, int, etc.)
    /// (2) Our wrapper over Span&lt;byte&gt;, called SpanByte (details below)
    ///
    /// Objects are placed contiguously in a single log, leading to efficient packing while
    /// avoiding the additional I/O (on reads and writes) that a separate object log entails.
    /// Serializers are not required, as these are effectively in-place allocated.
    /// </summary>
    public class Program
    {
        static void Main()
        {
            MemoryByteSample();
            MemoryIntSample();
            SpanByteSample();

            Console.WriteLine("Press <ENTER>");
            Console.ReadLine();
        }

        /// <summary>
        /// Sample showing how to use FASTER with Memory&lt;byte&gt;
        /// </summary>
        static void MemoryByteSample()
        {
            // VarLen types do not need an object log
            var log = Devices.CreateLogDevice("hlog.log", deleteOnClose: true);

            // Create store
            // For custom varlen (not SpanByte), you need to provide IVariableLengthStructSettings and IFasterEqualityComparer
            var store = new FasterKV<ReadOnlyMemory<byte>, Memory<byte>>(
                size: 1L << 20,
                logSettings: new LogSettings { LogDevice = log, MemorySizeBits = 15, PageSizeBits = 12 });

            // Create session
            var s = store.For(new MyMemoryFunctions<byte>()).NewSession<MyMemoryFunctions<byte>>();

            Random r = new Random(100);
            var keyMem = new Memory<byte>(new byte[1000]);
            var valueMem = new Memory<byte>(new byte[1000]);

            for (byte i = 0; i < 200; i++)
            {
                var keyLen = r.Next(1, 1000);
                var key = keyMem.Slice(0, keyLen);
                key.Span.Fill(i);

                var valLen = r.Next(1, 1000);
                var value = valueMem.Slice(0, valLen);
                value.Span.Fill((byte)valLen);

                s.Upsert(key, value);
            }

            bool success = true;

            r = new Random(100);
            for (byte i = 0; i < 200; i++)
            {
                var keyLen = r.Next(1, 1000);
                var key = keyMem.Slice(0, keyLen);
                key.Span.Fill(i);

                var valLen = r.Next(1, 1000);

                var status = s.Read(key, out var output, userContext: (byte)valLen);

                var expectedValue = valueMem.Slice(0, valLen);
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
        }

        /// <summary>
        /// Sample showing how to use FASTER with Memory&lt;int&gt;
        /// </summary>
        static void MemoryIntSample()
        {
            // VarLen types do not need an object log
            var log = Devices.CreateLogDevice("hlog.log", deleteOnClose: true);

            // Create store
            var store = new FasterKV<ReadOnlyMemory<int>, Memory<int>>(
                size: 1L << 20,
                logSettings: new LogSettings { LogDevice = log, MemorySizeBits = 16, PageSizeBits = 14 });

            // Create session
            var s = store.For(new MyMemoryFunctions<int>()).NewSession<MyMemoryFunctions<int>>();

            Random r = new Random(100);
            var keyMem = new Memory<int>(new int[1000]);
            var valueMem = new Memory<int>(new int[1000]);

            for (int i = 0; i < 2000; i++)
            {
                var keyLen = r.Next(1, 1000);
                var key = keyMem.Slice(0, keyLen);
                key.Span.Fill(i);

                var valLen = r.Next(1, 1000);
                var value = valueMem.Slice(0, valLen);
                value.Span.Fill(valLen);

                s.Upsert(key, value);
            }

            bool success = true;

            r = new Random(100);
            for (int i = 0; i < 2000; i++)
            {
                var keyLen = r.Next(1, 1000);
                var key = keyMem.Slice(0, keyLen);
                key.Span.Fill(i);

                var valLen = r.Next(1, 1000);

                var status = s.Read(key, out var output, userContext: valLen);

                var expectedValue = valueMem.Slice(0, valLen);
                expectedValue.Span.Fill(valLen);

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
        }

        /// <summary>
        /// This sample shows how our special type called SpanByte can be leverage to use FASTER
        /// with variable-length keys and/or values without a separate object log. SpanBytes can
        /// easily be created using pinned or fixed memory. A SpanByte is basically a sequence of
        /// bytes with a 4-byte integer length header that denotes the size of the payload.
        ///
        /// Underlying SpanByte is the use of "ref struct" as a proxy for pointers to variable-sized 
        /// memory in C# (we call these VariableLengthStructs).
        /// </summary>
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
            Span<byte> keyMem = stackalloc byte[1000];
            Span<byte> valueMem = stackalloc byte[1000];

            for (byte i = 0; i < 200; i++)
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

            r = new Random(100);
            for (byte i = 0; i < 200; i++)
            {
                var keyLen = r.Next(1, 1000);
                Span<byte> key = keyMem.Slice(0, keyLen);
                key.Fill(i);

                // Option 2: Converting fixed Span<byte> to SpanByte
                var status = s.Read(SpanByte.FromFixedSpan(key), out byte[] output);

                var valLen = r.Next(1, 1000);
                var expectedValue = valueMem.Slice(0, valLen);
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
        }
    }
}