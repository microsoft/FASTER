// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma warning disable 1591

#define EIGHT_BYTE_VALUE
//#define FIXED_SIZE_VALUE
//#define FIXED_SIZE_VALUE_WITH_LOCK

using System;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using FASTER.core;

namespace FASTER.benchmark
{
    [StructLayout(LayoutKind.Explicit, Size = 8)]
    public struct Value
    {
        [FieldOffset(0)]
        public long value;
    }
}
