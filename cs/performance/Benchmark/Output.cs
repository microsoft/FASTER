// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma warning disable 1591

using System.Runtime.InteropServices;

namespace FASTER.Benchmark
{
    [StructLayout(LayoutKind.Explicit)]
    public struct Output
    {
        [FieldOffset(0)]
        public Value value;
    }
}
