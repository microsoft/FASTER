// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

namespace FASTER.core
{
    internal readonly struct DefaultVariableLengthStruct<T, Input> : IVariableLengthStruct<T, Input>
    {
        private readonly IVariableLengthStruct<T> variableLengthStruct;

        /// <summary>
        /// Default instance of object
        /// </summary>
        /// <param name="variableLengthStruct"></param>
        public DefaultVariableLengthStruct(IVariableLengthStruct<T> variableLengthStruct)
        {
            this.variableLengthStruct = variableLengthStruct;
        }

        public int GetInitialLength(ref Input input)
        {
            return variableLengthStruct.GetInitialLength();
        }

        public int GetLength(ref T t, ref Input input)
        {
            return variableLengthStruct.GetLength(ref t);
        }
    }
}