// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;

namespace FASTER.core
{
    /// <summary>
    /// Interface to provide a batch of ReadOnlySpan[byte] data to FASTER
    /// </summary>
    public interface IReadOnlySpanBatch
    {
        /// <summary>
        /// Number of entries in provided batch
        /// </summary>
        /// <returns>Number of entries</returns>
        int TotalEntries();

        /// <summary>
        /// Retrieve batch entry at specified index
        /// </summary>
        /// <param name="index">Index</param>
        /// <returns></returns>
        ReadOnlySpan<byte> Get(int index);
    }
}
