// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.IO;

namespace FASTER.core
{
    /// <summary>
    /// Key interface
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public interface IKey<T>
    {
        /// <summary>
        /// Get 64-bit hash code
        /// </summary>
        /// <returns></returns>
        long GetHashCode64();

        /// <summary>
        /// Equality comparison
        /// </summary>
        /// <param name="k2"></param>
        /// <returns></returns>
        bool Equals(ref T k2);
    }
}