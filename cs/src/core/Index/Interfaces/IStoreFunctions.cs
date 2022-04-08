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
        /// If true, <see cref="Dispose(ref Key, ref Value, DisposeReason)"/> with <see cref="DisposeReason.PageEviction"/> 
        /// is called on page evictions from both readcache and main log. Otherwise, the user can register an Observer and
        /// do any needed disposal there.
        /// </summary>
        public bool DisposeOnPageEviction { get; }

        /// <summary>
        /// Dispose the Key and Value of a record, if necessary.
        /// </summary>
        void Dispose(ref Key key, ref Value value, DisposeReason reason);
    }
}
