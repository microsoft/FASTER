// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Collections.Generic;

namespace FASTER.core
{
    /// <summary>
    /// Low-performance FASTER equality comparer wrapper around EqualityComparer.Default
    /// </summary>
    /// <typeparam name="T"></typeparam>
    internal sealed class FasterEqualityComparer<T> : IFasterEqualityComparer<T>
    {
        public static readonly FasterEqualityComparer<T> Default = new FasterEqualityComparer<T>();

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
