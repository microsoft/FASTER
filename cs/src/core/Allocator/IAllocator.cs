// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

namespace FASTER.core
{
    public interface IAllocator
    {
        long Allocate(int numSlots);
        long GetPhysicalAddress(long logicalAddress);
        void CheckForAllocateComplete(ref long address);
        int RecordSize {  get; }
        void Free();
    }
}
