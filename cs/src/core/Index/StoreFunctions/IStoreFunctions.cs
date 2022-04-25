// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;

namespace FASTER.core
{
    /// <summary>
    /// The interface to define functions on the FasterKV store itself (rather than a session).
    /// </summary>
    public interface IStoreFunctions<Key, Value> : IRecordDisposer<Key, Value>
    {
        #region Key Comparison
        /// <summary>
        /// Get 64-bit hash code per <see cref="IFasterEqualityComparer{Key}.GetHashCode64(ref Key)"/>
        /// </summary>
        /// <remarks>A default implementation may be available via <see cref="FasterEqualityComparer.Get{Key}"/></remarks>
        long GetKeyHashCode64(ref Key key);

        /// <summary>
        /// Equality comparison per <see cref="IFasterEqualityComparer{Key}.Equals(ref Key, ref Key)"/>
        /// </summary>
        /// <param name="key1">Left side</param>
        /// <param name="key2">Right side</param>
        /// <remarks>A default implementation may be available via <see cref="FasterEqualityComparer.Get{Key}"/></remarks>
        bool KeyEquals(ref Key key1, ref Key key2);
        #endregion Key Comparison

        #region Input-independent Variable length Keys
        /// <summary>
        /// Indicates whether the Key is a variable-length struct
        /// </summary>
        bool IsVariableLengthKey { get; }

        /// <summary>
        /// Actual length of given key, when serialized on log
        /// </summary>
        /// <returns></returns>
        int GetKeyLength(ref Key key);

        /// <summary>
        /// Initial expected length of keys when serialized on log; make sure this at least includes 
        /// the object header needed to compute the actual object length
        /// </summary>
        int GetInitialKeyLength();

        /// <summary>
        /// Serialize object to given memory location
        /// </summary>
        unsafe void SerializeKey(ref Key source, void* destination)
#if NETSTANDARD2_1 || NET
        {
            var length = GetKeyLength(ref source);
            Buffer.MemoryCopy(Unsafe.AsPointer(ref source), destination, length, length);
        }
#else
        ;
#endif

        /// <summary>
        /// Return serialized data at given address, as a reference to object of type Key
        /// </summary>
        unsafe ref Key KeyAsRef(void* source)
#if NETSTANDARD2_1 || NET
            => ref Unsafe.AsRef<Key>(source)
#endif
            ;

        /// <summary>
        /// Initialize given address range [source, end) as a serialized object of type Key
        /// </summary>
        unsafe void InitializeKey(void* source, void* end)
#if NETSTANDARD2_1 || NET
        { }
#else
            ;
#endif
        #endregion Input-independent Variable length Keys

        #region Input-independent Variable length Values
        /// <summary>
        /// Indicates whether the Value is a variable-length struct
        /// </summary>
        bool IsVariableLengthValue { get; }

        /// <summary>
        /// Actual length of given value, when serialized on log
        /// </summary>
        /// <returns></returns>
        int GetValueLength(ref Value value);

        /// <summary>
        /// Initial expected length of keys when serialized on log; make sure this at least includes 
        /// the object header needed to compute the actual object length
        /// </summary>
        int GetInitialValueLength();

        /// <summary>
        /// Serialize object to given memory location
        /// </summary>
        unsafe void SerializeValue(ref Value source, void* destination)
#if NETSTANDARD2_1 || NET
        {
            var length = GetValueLength(ref source);
            Buffer.MemoryCopy(Unsafe.AsPointer(ref source), destination, length, length);
        }
#else
        ;
#endif

        /// <summary>
        /// Return serialized data at given address, as a reference to object of type Value
        /// </summary>
        unsafe ref Value ValueAsRef(void* source)
#if NETSTANDARD2_1 || NET
            => ref Unsafe.AsRef<Value>(source)
#endif
            ;

        /// <summary>
        /// Initialize given address range [source, end) as a serialized object of type Value
        /// </summary>
        unsafe void InitializeValue(void* source, void* end)
#if NETSTANDARD2_1 || NET
        { }
#else
            ;
#endif
#endregion Input-independent Variable length Values
    }
}
