// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;

namespace FASTER.PerfTest
{
    [Flags]
    public enum Operations
    {
        Mixed,
        Upsert,
        Read,
        RMW
    }
}
