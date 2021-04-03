// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;

namespace FASTER.benchmark
{
    public sealed class FunctionsSB : SpanByteFunctions<Empty>
    {
        public FunctionsSB(bool locking) : base(locking: locking) { }
    }
}
