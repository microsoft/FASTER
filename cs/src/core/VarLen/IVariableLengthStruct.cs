// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;

namespace FASTER.core
{
    /// <summary>
    /// Interface for variable length objects stored in-place on the FASTER log. We have some calls that use void* - these should 
    /// eventually move to a safe Span-based API.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public interface IVariableLengthStruct<T>
    {
        /// <summary>
        /// Actual length of given object, when serialized on log
        /// </summary>
        /// <param name="t"></param>
        /// <returns></returns>
        int GetLength(ref T t);

        /// <summary>
        /// Initial expected length of objects when serialized on log, make sure this at least includes 
        /// the object header needed to compute the actual object length
        /// </summary>
        /// <returns></returns>
        int GetInitialLength();

        /// <summary>
        /// Serialize object to given memory location
        /// </summary>
        /// <param name="source"></param>
        /// <param name="destination"></param>
        unsafe void Serialize(ref T source, void* destination)
#if NETSTANDARD2_1 || NET
            => Buffer.MemoryCopy(Unsafe.AsPointer(ref source), destination, GetLength(ref source), GetLength(ref source))
#endif
            ;

        /// <summary>
        /// Return serialized data at given address, as a reference to object of type T
        /// </summary>
        /// <param name="source"></param>
        /// <returns></returns>
        unsafe ref T AsRef(void* source)
#if NETSTANDARD2_1 || NET
            => ref Unsafe.AsRef<T>(source)
#endif
            ;

        /// <summary>
        /// Initialize given address range [source, end) as a serialized object of type T
        /// </summary>
        /// <param name="source"></param>
        /// <param name="end"></param>
        /// <returns></returns>
        unsafe void Initialize(void* source, void* end)
#if NETSTANDARD2_1 || NET
        { }
#else
            ;
#endif
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
}