// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

namespace FASTER.core
{
    /// <summary>
    /// Settings for variable length keys and values
    /// </summary>
    /// <typeparam name="Key"></typeparam>
    /// <typeparam name="Value"></typeparam>
    public class VariableLengthStructSettings<Key, Value>
    {
        /// <summary>
        /// Key length
        /// </summary>
        public IVariableLengthStruct<Key> keyLength;

        /// <summary>
        /// Value length
        /// </summary>
        public IVariableLengthStruct<Value> valueLength;
    }
}