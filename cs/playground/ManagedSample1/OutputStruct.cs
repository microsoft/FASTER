// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

namespace ManagedSampleCore
{
    public unsafe struct OutputStruct
    {
        public ValueStruct value;

        public static OutputStruct* MoveToContext(OutputStruct* output)
        {
            return output;
        }

    }
}
