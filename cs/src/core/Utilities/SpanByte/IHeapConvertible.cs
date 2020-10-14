// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

namespace FASTER.core
{
    /// <summary>
    /// Whether type supports converting to heap (e.g., when operation goes pending)
    /// </summary>
    public interface IHeapConvertible
    {
        /// <summary>
        /// Convert to heap
        /// </summary>
        public void ConvertToHeap();
    }
}
