// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace FASTER.core
{
    internal static class FasterEqualityComparer
    {
        public static IFasterEqualityComparer<T> Get<T>()
        {
            var t = typeof(T);
            if (t == typeof(string))
                return new StringFasterEqualityComparer() as IFasterEqualityComparer<T>;
            else if (t == typeof(byte[]))
                return new ByteArrayFasterEqualityComparer() as IFasterEqualityComparer<T>;
            else if (t == typeof(long))
                return new LongFasterEqualityComparer() as IFasterEqualityComparer<T>;
            else if (t == typeof(int))
                return new IntFasterEqualityComparer() as IFasterEqualityComparer<T>;
            else if (t == typeof(Guid))
                return new GuidFasterEqualityComparer() as IFasterEqualityComparer<T>;
            else if (t == typeof(SpanByte))
                return new SpanByteComparer() as IFasterEqualityComparer<T>;
            else if ((t.GetGenericTypeDefinition() == typeof(Memory<>)) && Utility.IsBlittableType(t.GetGenericArguments()[0]))
            {
                var m = typeof(MemoryComparer<>).MakeGenericType(t.GetGenericArguments());
                object o = Activator.CreateInstance(m);
                return o as IFasterEqualityComparer<T>;
            }
            else if ((t.GetGenericTypeDefinition()== typeof(ReadOnlyMemory<>)) && Utility.IsBlittableType(t.GetGenericArguments()[0]))
            {
                var m = typeof(ReadOnlyMemoryComparer<>).MakeGenericType(t.GetGenericArguments());
                object o = Activator.CreateInstance(m);
                return o as IFasterEqualityComparer<T>;
            }
            else
            {
                Debug.WriteLine("***WARNING*** Creating default FASTER key equality comparer based on potentially slow EqualityComparer<Key>.Default. To avoid this, provide a comparer (IFasterEqualityComparer<Key>) as an argument to FASTER's constructor, or make Key implement the interface IFasterEqualityComparer<Key>");
                return DefaultFasterEqualityComparer<T>.Default;
            }
        }
    }

    /// <summary>
    /// Deterministic equality comparer for strings
    /// </summary>
    public sealed class StringFasterEqualityComparer : IFasterEqualityComparer<string>
    {
        /// <summary>
        /// Equals
        /// </summary>
        /// <param name="k1"></param>
        /// <param name="k2"></param>
        /// <returns></returns>
        public bool Equals(ref string k1, ref string k2) => k1 == k2;

        /// <summary>
        /// GetHashCode64
        /// </summary>
        /// <param name="k"></param>
        /// <returns></returns>
        public unsafe long GetHashCode64(ref string k)
        {
            fixed (char* c = k)
            {
                return Utility.HashBytes((byte*)c, k.Length * sizeof(char));
            }
        }
    }

    /// <summary>
    /// Deterministic equality comparer for longs
    /// </summary>
    public sealed class LongFasterEqualityComparer : IFasterEqualityComparer<long>
    {
        /// <summary>
        /// Equals
        /// </summary>
        /// <param name="k1"></param>
        /// <param name="k2"></param>
        /// <returns></returns>
        public bool Equals(ref long k1, ref long k2) => k1 == k2;

        /// <summary>
        /// GetHashCode64
        /// </summary>
        /// <param name="k"></param>
        /// <returns></returns>
        public long GetHashCode64(ref long k) => Utility.GetHashCode(k);
    }

    /// <summary>
    /// Deterministic equality comparer for longs
    /// </summary>
    public sealed class IntFasterEqualityComparer : IFasterEqualityComparer<int>
    {
        /// <summary>
        /// Equals
        /// </summary>
        /// <param name="k1"></param>
        /// <param name="k2"></param>
        /// <returns></returns>
        public bool Equals(ref int k1, ref int k2) => k1 == k2;

        /// <summary>
        /// GetHashCode64
        /// </summary>
        /// <param name="k"></param>
        /// <returns></returns>
        public long GetHashCode64(ref int k) => Utility.GetHashCode(k);
    }

    /// <summary>
    /// Deterministic equality comparer for longs
    /// </summary>
    public sealed class GuidFasterEqualityComparer : IFasterEqualityComparer<Guid>
    {
        /// <summary>
        /// Equals
        /// </summary>
        /// <param name="k1"></param>
        /// <param name="k2"></param>
        /// <returns></returns>
        public bool Equals(ref Guid k1, ref Guid k2) => k1 == k2;

        /// <summary>
        /// GetHashCode64
        /// </summary>
        /// <param name="k"></param>
        /// <returns></returns>
        public unsafe long GetHashCode64(ref Guid k)
        {
            var _k = k;
            var pGuid = (long*)&_k;
            return pGuid[0] ^ pGuid[1];
        }
    }

    /// <summary>
    /// Deterministic equality comparer for byte[]
    /// </summary>
    public sealed class ByteArrayFasterEqualityComparer : IFasterEqualityComparer<byte[]>
    {
        /// <summary>
        /// Equals
        /// </summary>
        /// <param name="k1"></param>
        /// <param name="k2"></param>
        /// <returns></returns>
        public unsafe bool Equals(ref byte[] k1, ref byte[] k2)
        {
            int length = k1.Length;
            if (length != k2.Length)
                return false;

            fixed (byte* b1 = k1, b2 = k2)
            {
                return Utility.IsEqual(b1, b2, length);
            }
        }

        /// <summary>
        /// GetHashCode64
        /// </summary>
        /// <param name="k"></param>
        /// <returns></returns>
        public unsafe long GetHashCode64(ref byte[] k)
        {
            fixed (byte* b = k)
            {
                return Utility.HashBytes(b, k.Length);
            }
        }
    }

    /// <summary>
    /// Low-performance FASTER equality comparer wrapper around EqualityComparer.Default
    /// </summary>
    /// <typeparam name="T"></typeparam>
    internal sealed class DefaultFasterEqualityComparer<T> : IFasterEqualityComparer<T>
    {
        public static readonly DefaultFasterEqualityComparer<T> Default = new DefaultFasterEqualityComparer<T>();

        private static readonly EqualityComparer<T> DefaultEC = EqualityComparer<T>.Default;

        public bool Equals(ref T k1, ref T k2)
        {
            return DefaultEC.Equals(k1, k2);
        }

        public long GetHashCode64(ref T k)
        {
            return Utility.GetHashCode(DefaultEC.GetHashCode(k));
        }
    }
}
