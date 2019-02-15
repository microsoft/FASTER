// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;

namespace FASTER.core
{
    /// <summary>
    /// Scan iterator interface for FASTER log
    /// </summary>
    /// <typeparam name="Key"></typeparam>
    /// <typeparam name="Value"></typeparam>
    public interface IFasterScanIterator<Key, Value> : IDisposable
    {
        /// <summary>
        /// Get next record
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        /// <returns>True if record found, false if end of scan</returns>
        bool GetNext(out Key key, out Value value);
    }
}
