// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

namespace SumStore
{
    public unsafe struct Output
    {
        public NumClicks value;

        public static Output* MoveToContext(Output* value)
        {
            return value;
        }

    }
}
