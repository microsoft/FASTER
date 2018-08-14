// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Runtime.InteropServices;

namespace FASTER.core
{
    [StructLayout(LayoutKind.Explicit, Size = 12)]
    public unsafe struct Context
    {
        [FieldOffset(0)]
        public ulong start;

        [FieldOffset(8)]
        public int threadId;

        public static Context* MoveToContext(Context* context)
        {
            return context;
        }
    }
}
