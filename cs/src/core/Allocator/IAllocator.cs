// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;

namespace FASTER.core
{
    public interface IAllocator : IDisposable
    {
        long Allocate(int numSlots);
        long GetPhysicalAddress(long logicalAddress);
        void CheckForAllocateComplete(ref long address);
        void Dispose();
    }
}
