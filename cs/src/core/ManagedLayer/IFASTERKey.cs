// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.IO;

namespace FASTER.core
{
    /// <summary>
    /// Interface to key
    /// </summary>
    /// <typeparam name="TKey"></typeparam>
    public interface IFasterKey<TKey>
    {
        /// <summary>
        /// Clone
        /// </summary>
        /// <returns></returns>
        TKey Clone();

        /// <summary>
        /// Deserialize
        /// </summary>
        /// <param name="fromStream"></param>
        void Deserialize(Stream fromStream);

        /// <summary>
        /// Equals
        /// </summary>
        /// <param name="other"></param>
        /// <returns></returns>
        bool Equals(TKey other);

        /// <summary>
        /// Hashcode
        /// </summary>
        /// <returns></returns>
        long GetHashCode64();

        /// <summary>
        /// Serialize
        /// </summary>
        /// <param name="toStream"></param>
        void Serialize(Stream toStream);
    }
}
