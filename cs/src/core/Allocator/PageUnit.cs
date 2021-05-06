// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Runtime.InteropServices;

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

namespace FASTER.core
{
    struct PageUnit
    {
        public byte[] value;
        public GCHandle handle;
        public long pointer;
    }
}


