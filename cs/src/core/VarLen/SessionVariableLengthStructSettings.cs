// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

namespace FASTER.core
{
#if false // TODO remove
    /// <summary>
    /// Session-specific settings for variable length structs
    /// </summary>
    /// <typeparam name="Value"></typeparam>
    /// <typeparam name="Input"></typeparam>
    public class SessionVariableLengthStructSettings<Value, Input>
    {
        /// <summary>
        /// Value length, given input
        /// </summary>
        public IVariableLengthStruct<Value, Input> valueLength;

        /// <summary>
        /// Input length
        /// </summary>
        public IVariableLengthStruct<Input> inputLength;
    }
#endif
}