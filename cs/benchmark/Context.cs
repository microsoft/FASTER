// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma warning disable 1591

using System.Runtime.InteropServices;

namespace FASTER.benchmark
{
    [StructLayout(LayoutKind.Explicit, Size = 12)]
    public struct Context
    {
        [FieldOffset(0)]
        public ulong start;

        [FieldOffset(8)]
        public int threadId;
    }
}
