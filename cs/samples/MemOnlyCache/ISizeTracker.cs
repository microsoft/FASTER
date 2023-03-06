// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

namespace MemOnlyCache
{
    public interface ISizeTracker
    {
        int GetSize { get; }
    }
}
