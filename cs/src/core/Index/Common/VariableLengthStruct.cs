// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;

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
        /// Actual length of given object
        /// </summary>
        /// <param name="t"></param>
        /// <returns></returns>
        int GetLength(ref T t);

        /// <summary>
        /// Initial expected length of objects, make sure this includes the object
        /// header needed to compute the actual object length
        /// </summary>
        /// <returns></returns>
        int GetInitialLength();

        /// <summary>
        /// Serialize to given memory location
        /// </summary>
        /// <param name="source"></param>
        /// <param name="destination"></param>
        unsafe void Serialize(ref T source, void* destination)
#if NETSTANDARD21
            => Buffer.MemoryCopy(Unsafe.AsPointer(ref source), destination, GetLength(ref source), GetLength(ref source))
#endif
            ;

        /// <summary>
        /// Return serialized data as reference to T
        /// </summary>
        /// <param name="source"></param>
        /// <returns></returns>
        unsafe ref T AsRef(void* source)
#if NETSTANDARD21
            => ref Unsafe.AsRef<T>(source)
#endif
            ;

        /// <summary>
        /// Return address range as reference to T
        /// </summary>
        /// <param name="source"></param>
        /// <param name="end"></param>
        /// <returns></returns>
        unsafe ref T AsRef(void* source, void* end)
#if NETSTANDARD21
            => ref Unsafe.AsRef<T>(source)
#endif
            ;
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
        public int GetInitialLength() => size;

        /// <summary>
        /// Get length
        /// </summary>
        /// <param name="t"></param>
        /// <returns></returns>
        public int GetLength(ref T t) => size;

        public unsafe void Serialize(ref T source, void* destination)
            => Buffer.MemoryCopy(Unsafe.AsPointer(ref source), destination, GetLength(ref source), GetLength(ref source));

        public unsafe ref T AsRef(void* source) => ref Unsafe.AsRef<T>(source);
        public unsafe ref T AsRef(void* source, void* dest) => ref Unsafe.AsRef<T>(source);
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

    /// <summary>
    /// Input-specific interface for variable length in-place objects
    /// modeled as structs, in FASTER
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <typeparam name="Input"></typeparam>
    public interface IVariableLengthStruct<T, Input>
    {
        /// <summary>
        /// Length of resulting object when performing RMW with given input
        /// </summary>
        /// <param name="t"></param>
        /// <param name="input"></param>
        /// <returns></returns>
        int GetLength(ref T t, ref Input input);

        /// <summary>
        /// Initial expected length of object, when populated by RMW using given input
        /// </summary>
        /// <param name="input"></param>
        /// <returns></returns>
        int GetInitialLength(ref Input input);
    }

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