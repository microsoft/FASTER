// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System.Runtime.CompilerServices;

namespace ManagedSampleCore
{
    public struct InputStruct : IMoveToContext<InputStruct>
    {
        public long ifield1;
        public long ifield2;

        public ref InputStruct MoveToContext(ref InputStruct input)
        {
            return ref input;
        }
    }
}
