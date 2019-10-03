// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;

namespace FASTER.core
{
    /// <summary>
    /// Interface to provide a batch of Span[byte] data to FASTER
    /// </summary>
    public interface ISpanBatch
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
        Span<byte> Get(int index);
    }
}
