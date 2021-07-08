// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

namespace FASTER.common
{
    /// <summary>
    /// Serializer interface for server-side processing
    /// </summary>
    /// <typeparam name="Key">Key</typeparam>
    public unsafe interface IKeySerializer<Key>
    {
        /// <summary>
        /// Read key by reference, from given location
        /// </summary>
        /// <param name="src">Memory location</param>
        /// <returns>Key</returns>
        ref Key ReadKeyByRef(ref byte* src);

        /// <summary>
        /// Match pattern with key used for pub-sub
        /// </summary>
        /// <param name="k">key to be published</param>
        /// <param name="pattern">pattern to check</param>
        bool Match(ref Key k, ref Key pattern);
    }
}
