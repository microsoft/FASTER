// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

namespace FASTER.core
{
    /// <summary>
    /// Default implementation of <see cref="IStoreFunctions{Key, Value}"/>
    /// </summary>
    /// <typeparam name="Key"></typeparam>
    /// <typeparam name="Value"></typeparam>
    public class DefaultStoreFunctions<Key, Value> : IStoreFunctions<Key, Value>
    {
        /// <summary>
        /// Default implementation does nothing
        /// </summary>
        public void Dispose(ref Key key, ref Value value, DisposeReason reason)
        {
        }

        /// <summary>
        /// Default implementation does nothing
        /// </summary>
        public bool DisposeOnPageEviction => false;
    }
}
