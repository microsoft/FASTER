// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;

namespace ManagedSample1
{
    public unsafe struct InputStruct
    {
        public long ifield1;
        public long ifield2;

        public static InputStruct* MoveToContext(InputStruct* input)
        {
            return input;
        }
    }
}
