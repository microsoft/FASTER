// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

namespace FASTER.core
{
    /// <summary>
    /// Session-specific settings for variable length structs
    /// </summary>
    /// <typeparam name="Value"></typeparam>
    /// <typeparam name="Input"></typeparam>
    public class SessionVariableLengthStructSettings<Value, Input>
    {
        /// <summary>
        /// Key length
        /// </summary>
        public IVariableLengthStruct<Value, Input> valueLength;

        /// <summary>
        /// Value length
        /// </summary>
        public IVariableLengthStruct<Input> inputLength;
    }
}