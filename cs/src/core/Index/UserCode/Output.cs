// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;

namespace FASTER.core
{
    [StructLayout(LayoutKind.Explicit)]
    public unsafe struct Output
    {
        [FieldOffset(0)]
        public Value value;

        public static Output* MoveToContext(Output* value)
        {
            return value;
        }

    }
}
