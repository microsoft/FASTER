// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Runtime.InteropServices;

namespace FASTER.common
{
    [StructLayout(LayoutKind.Explicit, Size = 8)]
    public unsafe struct BatchHeader
    {
        public const int Size = 8;
        [FieldOffset(0)]
        public int seqNo;
        [FieldOffset(4)]
        public int numMessages;
    }
}