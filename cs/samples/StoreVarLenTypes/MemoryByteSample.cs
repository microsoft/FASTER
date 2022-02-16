// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Linq;
using FASTER.core;

namespace StoreVarLenTypes
{
    /// <summary>
    /// Sample showing how to use FASTER with Memory&lt;byte&gt;
    /// </summary>
    public class MemoryByteSample
    {
        public static void Run()
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

            Random r = new(100);

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

                if (status.IsPending)
                    s.CompletePending(true);
                else
                {
                    if (!status.IsFound || (!output.Item1.Memory.Slice(0, output.Item2).Span.SequenceEqual(expectedValue.Span)))
                    {
                        output.Item1.Dispose();
                        success = false;
                        break;
                    }
                    output.Item1.Dispose();
                }
            }

            if (success)
                Console.WriteLine("MemoryByteSample: Success!");
            else
                Console.WriteLine("Error!");

            s.Dispose();
            store.Dispose();
            log.Dispose();
        }
    }
}