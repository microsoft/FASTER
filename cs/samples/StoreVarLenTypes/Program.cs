// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;

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
            // Sample using SpanByte and RMW to perform sums over non-negative ASCII numbers
            AsciiSumSample.Run();

            // Samples using Memory<T> over byte and int as key/value types
            MemoryByteSample.Run();
            MemoryIntSample.Run();

            // Sample using SpanByte (unsafe varlen wrapper) as data type
            SpanByteSample.Run();

            Console.WriteLine("Press <ENTER>");
            Console.ReadLine();
        }
    }
}