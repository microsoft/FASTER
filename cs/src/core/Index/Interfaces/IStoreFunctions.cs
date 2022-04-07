// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

namespace FASTER.core
{
    /// <summary>
    /// The interface to define functions on the FasterKV store itself (rather than a session).
    /// </summary>
    public interface IStoreFunctions<Key, Value>
    {
        /// <summary>
        /// Dispose the Key and Value of a record, if necessary.
        /// </summary>
        void Dispose(ref Key key, ref Value value, DisposeReason reason);
    }
}
