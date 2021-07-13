// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Linq;
using FASTER.core;

namespace StoreVarLenTypes
{
    /// <summary>
    /// Sample showing how to use FASTER with Memory&lt;int&gt;
    /// </summary>
    public class MemoryIntSample
    {
        public static void Run()
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
                Console.WriteLine("MemoryIntSample: Success!");
            else
                Console.WriteLine("Error!");

            s.Dispose();
            store.Dispose();
            log.Dispose();
        }
    }
}