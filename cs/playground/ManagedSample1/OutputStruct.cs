// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;

namespace ManagedSampleCore
{
    public struct OutputStruct : IMoveToContext<OutputStruct>
    {
        public ValueStruct value;

        public ref OutputStruct MoveToContext(ref OutputStruct output)
        {
            return ref output;
        }
    }
}
