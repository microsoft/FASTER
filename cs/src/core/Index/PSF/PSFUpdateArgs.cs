// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

namespace FASTER.core
{
    /// <summary>
    /// Structure passed to and set by Upsert and RMW in the Primary FKV; specific to the FasterKV client.
    /// </summary>
    internal struct PSFUpdateArgs<Key, Value>
        where Key : new()
        where Value : new()
    {
        /// <summary>
        /// Set to the inserted or updated logical address, which is the RecordId for the FasterKV client
        /// </summary>
        /// <remarks>Having this outside the changeTracker means that a non-updating Upsert does not incur
        ///     allocation overhead.</remarks>
        internal long LogicalAddress;

        /// <summary>
        /// Created and populated with PreUpdate values on RMW or Upsert of an existing key
        /// </summary>
        internal PSFChangeTracker<FasterKVProviderData<Key, Value>, long> ChangeTracker;
    }
}
