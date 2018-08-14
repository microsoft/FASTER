// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;

namespace FASTER.core
{
    public unsafe struct Input
    {
        public long value;

        public static Input* MoveToContext(Input* input)
        {
            return input;
        }
    }
}
