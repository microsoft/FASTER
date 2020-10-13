// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma warning disable 1591

using System;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using FASTER.core;

namespace FASTER.benchmark
{
    [StructLayout(LayoutKind.Explicit, Size = FasterSpanByteYcsbBenchmark.kKeySize)]
    public struct KeySpanByte
    {
        [FieldOffset(0)]
        public int length;
        [FieldOffset(4)]
        public long value;
    }
}
