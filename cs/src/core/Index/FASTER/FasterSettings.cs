// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

namespace FASTER.core
{
    /// <summary>
    /// Settings for the <see cref="FasterKV{Key, Value}"/> store
    /// </summary>
    public class FasterSettings
    {
        /// <summary>
        /// Whether this FasterKV instance supports locking. Iff so, FASTER will call the locking methods of <see cref="IFunctions{Key, Value, Input, Output, Context}"/>.
        /// </summary>
        public bool SupportsLocking { get; set; }
    }
}
