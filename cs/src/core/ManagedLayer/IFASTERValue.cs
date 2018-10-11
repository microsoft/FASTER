// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.IO;

namespace FASTER.core
{
    /// <summary>
    /// Value interface
    /// </summary>
    /// <typeparam name="TValue"></typeparam>
    public interface IFasterValue<TValue>
    {
        /// <summary>
        /// Clone
        /// </summary>
        /// <returns></returns>
        TValue Clone();

        /// <summary>
        /// Deserialize
        /// </summary>
        /// <param name="fromStream"></param>
        void Deserialize(Stream fromStream);

        /// <summary>
        /// Serialize
        /// </summary>
        /// <param name="toStream"></param>
        void Serialize(Stream toStream);
    }
}
