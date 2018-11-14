// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Collections.Generic;
using System.IO;

namespace FASTER.core
{
    /// <summary>
    /// Interface for KVS to operate on pages
    /// </summary>
    public interface IPageHandlers
    {
        /// <summary>
        /// Clear page
        /// </summary>
        /// <param name="ptr">From pointer</param>
        /// <param name="endptr">Until pointer</param>
        void ClearPage(long ptr, long endptr);

        /// <summary>
        /// Deseialize part of page from stream
        /// </summary>
        /// <param name="ptr">From pointer</param>
        /// <param name="untilptr">Until pointer</param>
        /// <param name="stream">Stream</param>
        void Deserialize(long ptr, long untilptr, Stream stream);

        /// <summary>
        /// Serialize part of page to stream
        /// </summary>
        /// <param name="ptr">From pointer</param>
        /// <param name="untilptr">Until pointer</param>
        /// <param name="stream">Stream</param>
        /// <param name="objectBlockSize">Size of blocks to serialize in chunks of</param>
        /// <param name="addr">List of addresses that need to be updated with offsets</param>
        void Serialize(ref long ptr, long untilptr, Stream stream, int objectBlockSize, out List<long> addr);
    }
}
