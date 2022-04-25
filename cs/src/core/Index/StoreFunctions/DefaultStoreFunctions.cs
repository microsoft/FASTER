// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

namespace FASTER.core
{
    /// <summary>
    /// Default implementation of <see cref="IStoreFunctions{Key, Value}"/>
    /// </summary>
    public class DefaultStoreFunctions<Key, Value> : StoreFunctions<Key, Value, DefaultFasterEqualityComparer<Key>, DefaultRecordDisposer<Key, Value>, DefaultVariableLengthStruct<Key>, DefaultVariableLengthStruct<Value>>
    {
        /// <summary>
        /// Default instance
        /// </summary>
        public static readonly DefaultStoreFunctions<Key, Value> Default = new();

        /// <summary>
        /// Constructor
        /// </summary>
        public DefaultStoreFunctions() : base(DefaultFasterEqualityComparer<Key>.Default, DefaultRecordDisposer<Key, Value>.Default, DefaultVariableLengthStruct<Key>.Default, DefaultVariableLengthStruct<Value>.Default)
        {
        }
    }
}
