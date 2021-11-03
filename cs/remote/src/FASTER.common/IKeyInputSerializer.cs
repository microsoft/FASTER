// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

namespace FASTER.common
{
    /// <summary>
    /// Serializer interface for keys, needed for pub-sub
    /// </summary>
    /// <typeparam name="Key">Key</typeparam>
    /// <typeparam name="Input">Input</typeparam>
    public unsafe interface IKeyInputSerializer<Key, Input> : IKeySerializer<Key>
    {
        /// <summary>
        /// Read input by reference, from given location
        /// </summary>
        /// <param name="src">Memory location</param>
        /// <returns>Input</returns>
        ref Input ReadInputByRef(ref byte* src);
    }
}
