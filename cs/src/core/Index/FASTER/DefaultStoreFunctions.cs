// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;

namespace FASTER.core
{
    /// <summary>
    /// Default implementation of <see cref="IStoreFunctions{Key, Value}"/>
    /// </summary>
    /// <typeparam name="Key"></typeparam>
    /// <typeparam name="Value"></typeparam>
    public class DefaultStoreFunctions<Key, Value> : IStoreFunctions<Key, Value>
    {
        IFasterEqualityComparer<Key> comparer;

        /// <summary>Constructor</summary>
        public DefaultStoreFunctions()
        {
            this.comparer = FasterEqualityComparer.Get<Key>();
        }

        /// <summary>Default implementation does nothing</summary>
        public bool DisposeOnPageEviction => false;

        /// <summary>Default implementation does nothing</summary>
        public void Dispose(ref Key key, ref Value value, DisposeReason reason) { }

        /// <summary>Default implementation does nothing</summary>
        public int GetKeyLength(ref Key key) => throw new System.NotImplementedException();

        /// <summary>Default implementation does nothing</summary>
        public int GetInitialKeyLength() => throw new System.NotImplementedException();

        /// <summary>Default implementation</summary>
        public unsafe void SerializeKey(ref Key source, void* destination)
        {
            var length = GetKeyLength(ref source);
            Buffer.MemoryCopy(Unsafe.AsPointer(ref source), destination, length, length);
        }

        /// <summary>Default implementation</summary>
        public unsafe ref Key KeyAsRef(void* source)
            => ref Unsafe.AsRef<Key>(source);

        /// <summary>Default implementation</summary>
        public unsafe void InitializeKey(void* source, void* end) { }

        /// <summary>Default implementation does nothing</summary>
        public int GetValueLength(ref Value value) => throw new System.NotImplementedException();

        /// <summary>Default implementation does nothing</summary>
        public int GetInitialValueLength() => throw new System.NotImplementedException();

        /// <summary>Default implementation</summary>
        public unsafe void SerializeValue(ref Value source, void* destination)
        {
            var length = GetValueLength(ref source);
            Buffer.MemoryCopy(Unsafe.AsPointer(ref source), destination, length, length);
        }

        /// <summary>Default implementation</summary>
        public unsafe ref Value ValueAsRef(void* source)
            => ref Unsafe.AsRef<Value>(source);

        /// <summary>Default implementation</summary>
        public unsafe void InitializeValue(void* source, void* end) { }


        /// <summary>Default implementation of comparer</summary>
        public long GetKeyHashCode64(ref Key key) => comparer.GetHashCode64(ref key);

        /// <summary>Default implementation of comparer</summary>
        public bool KeyEquals(ref Key key1, ref Key key2) => comparer.Equals(ref key1, ref key2);
    }
}
