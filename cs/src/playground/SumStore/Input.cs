// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;

namespace SumStore
{
    public unsafe struct Input
    {
        public AdId adId;
        public NumClicks numClicks;

        public static Input* MoveToContext(Input* value)
        {
            return value;
        }

    }
}
