// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

namespace FASTER.common
{
    /// <summary>
    /// Serializer interface for keys, needed for pub-sub
    /// </summary>
    /// <typeparam name="Key">Key</typeparam>
    /// <typeparam name="Input">Input</typeparam>
    public unsafe interface IKeyInputSerializer<Key, Input>
    {
        /// <summary>
        /// Read key by reference, from given location
        /// </summary>
        /// <param name="src">Memory location</param>
        /// <returns>Key</returns>
        ref Key ReadKeyByRef(ref byte* src);

        /// <summary>
        /// Read input by reference, from given location
        /// </summary>
        /// <param name="src">Memory location</param>
        /// <returns>Input</returns>
        ref Input ReadInputByRef(ref byte* src);

        /// <summary>
        /// Match pattern with key used for pub-sub
        /// </summary>
        /// <param name="k">key to be published</param>
        /// <param name="pattern">pattern to check</param>
        bool Match(ref Key k, ref Key pattern);
    }
}
