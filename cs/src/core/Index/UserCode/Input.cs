// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma warning disable 1591

using System.Runtime.CompilerServices;

namespace FASTER.core
{
    public unsafe struct Input
    {
        public long value;

        public ref Input MoveToContext(ref Input input)
        {
            return ref input;
        }
    }
}
