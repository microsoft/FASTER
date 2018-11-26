// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma warning disable 1591

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

        public ref Output MoveToContext(ref Output value)
        {
            return ref value;
        }

    }
}
