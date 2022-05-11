// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#if !NET5_0_OR_GREATER
using System.Runtime.InteropServices;
#endif

namespace FASTER.core
{
    struct PageUnit
    {
        public byte[] value;
#if !NET5_0_OR_GREATER
        public GCHandle handle;
#endif
        public long pointer;
    }
}


