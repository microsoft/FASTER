// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

namespace ResizableCacheStore
{
    public interface ISizeTracker
    {
        int GetSize { get; }
    }
}
