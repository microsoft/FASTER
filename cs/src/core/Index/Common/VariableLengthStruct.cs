// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

namespace FASTER.core
{
    /// <summary>
    /// Interface for variable length in-place objects
    /// modeled as structs, in FASTER
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public interface IVariableLengthStruct<T>
    {
        /// <summary>
        /// Actual length of object
        /// </summary>
        /// <param name="t"></param>
        /// <returns></returns>
        int GetLength(ref T t);

        /// <summary>
        /// Average length of objects, make sure this includes the object
        /// header needed to compute the actual object length
        /// </summary>
        /// <returns></returns>
        int GetAverageLength();
    }

    /// <summary>
    /// Length specification for fixed size (normal) structs
    /// </summary>
    /// <typeparam name="T"></typeparam>
    internal readonly struct FixedLengthStruct<T> : IVariableLengthStruct<T>
    {
        private static readonly int size = Utility.GetSize(default(T));

        /// <summary>
        /// Get average length
        /// </summary>
        /// <returns></returns>
        public int GetAverageLength() => size;

        /// <summary>
        /// Get length
        /// </summary>
        /// <param name="t"></param>
        /// <returns></returns>
        public int GetLength(ref T t) => size;
    }

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

    /// <summary>
    /// Interface for variable length in-place objects
    /// modeled as structs, in FASTER
    /// </summary>
    /// <typeparam name="Value"></typeparam>
    /// <typeparam name="Input"></typeparam>
    public interface IVariableLengthStruct<Value, Input>
    {
        /// <summary>
        /// Length of resulting Value when performing RMW with
        /// </summary>
        /// <param name="t"></param>
        /// <param name="input"></param>
        /// <returns></returns>
        int GetLength(ref Value t, ref Input input);

        /// <summary>
        /// Initial length, when populating for RMW from given input
        /// </summary>
        /// <param name="input"></param>
        /// <returns></returns>
        int GetInitialLength(ref Input input);
    }

    internal readonly struct DefaultVariableLengthStruct<T, TInput> : IVariableLengthStruct<T, TInput>
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

        public int GetInitialLength(ref TInput input)
        {
            return variableLengthStruct.GetAverageLength();
        }

        public int GetLength(ref T t, ref TInput input)
        {
            return variableLengthStruct.GetLength(ref t);
        }
    }
}