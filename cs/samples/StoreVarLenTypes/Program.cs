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
    /// (2) Our advanced wrapper over Span&lt;byte&gt;, called SpanByte (details below)
    ///
    /// Objects are placed contiguously in a single log, leading to efficient packing while
    /// avoiding the additional I/O (on reads and writes) that a separate object log entails.
    /// Serializers are not required, as these are effectively in-place allocated.
    /// </summary>
    public class Program
    {
        static void Main()
        {
            // Samples using Memory<T> over byte and int as key/value types
            MemoryByteSample();
            MemoryIntSample();

            // Sample for advanced users; using SpanByte type
            SpanByteSample();

            Console.WriteLine("Press <ENTER>");
            Console.ReadLine();
        }

        /// <summary>
        /// Sample showing how to use FASTER with Memory&lt;byte&gt;
        /// </summary>
        static void MemoryByteSample()
        {
            // Memory<T> where T : unmanaged does not need an object log
            var log = Devices.CreateLogDevice("hlog.log", deleteOnClose: true);

            // Create store for Memory<byte> values. You can use any key type; we use ReadOnlyMemory<byte> here
            var store = new FasterKV<ReadOnlyMemory<byte>, Memory<byte>>(
                size: 1L << 20,
                logSettings: new LogSettings { LogDevice = log, MemorySizeBits = 15, PageSizeBits = 12 });

            // Create session with Memory<byte> as Input, (IMemoryOwner<byte> memoryOwner, int length) as Output, 
            // and byte as Context (to verify read result in callback)
            var s = store.For(new CustomMemoryFunctions<byte>()).NewSession<CustomMemoryFunctions<byte>>();

            Random r = new Random(100);

            // Allocate space for key and value operations
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
                    s.CompletePending(true);
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
            // Memory<T> where T : unmanaged does not need an object log
            var log = Devices.CreateLogDevice("hlog.log", deleteOnClose: true);

            // Create store for Memory<int> values. You can use any key type; we use ReadOnlyMemory<int> here
            var store = new FasterKV<ReadOnlyMemory<int>, Memory<int>>(
                size: 1L << 20,
                logSettings: new LogSettings { LogDevice = log, MemorySizeBits = 16, PageSizeBits = 14 });

            // Create session with Memory<int> as Input, (IMemoryOwner<int> memoryOwner, int length) as Output, 
            // and int as Context (to verify read result in callback)
            var s = store.For(new CustomMemoryFunctions<int>()).NewSession<CustomMemoryFunctions<int>>();

            Random r = new Random(100);

            // Allocate space for key and value operations
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
                    s.CompletePending(true);
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
            var s = store.For(new CustomSpanByteFunctions()).NewSession<CustomSpanByteFunctions>();

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

                var valLen = r.Next(1, 1000);

                // Option 2: Converting fixed Span<byte> to SpanByte
                var status = s.Read(SpanByte.FromFixedSpan(key), out byte[] output, userContext: (byte)valLen);

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