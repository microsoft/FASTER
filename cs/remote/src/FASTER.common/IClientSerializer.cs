// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

namespace FASTER.common
{
    /// <summary>
    /// Client serializer interface
    /// </summary>
    /// <typeparam name="Key">Key</typeparam>
    /// <typeparam name="Value">Value</typeparam>
    /// <typeparam name="Input">Input</typeparam>
    /// <typeparam name="Output">Output</typeparam>
    public unsafe interface IClientSerializer<Key, Value, Input, Output>
    {
        /// <summary>
        /// Write element to given destination, with length bytes of space available
        /// </summary>
        /// <param name="k">Element to write</param>
        /// <param name="dst">Destination memory</param>
        /// <param name="length">Space (bytes) available at destination</param>
        /// <returns>True if write succeeded, false if not (insufficient space)</returns>
        bool Write(ref Key k, ref byte* dst, int length);

        /// <summary>
        /// Write element to given destination, with length bytes of space available
        /// </summary>
        /// <param name="v">Element to write</param>
        /// <param name="dst">Destination memory</param>
        /// <param name="length">Space (bytes) available at destination</param>
        /// <returns>True if write succeeded, false if not (insufficient space)</returns>
        bool Write(ref Value v, ref byte* dst, int length);

        /// <summary>
        /// Write element to given destination, with length bytes of space available
        /// </summary>
        /// <param name="i">Element to write</param>
        /// <param name="dst">Destination memory</param>
        /// <param name="length">Space (bytes) available at destination</param>
        /// <returns>True if write succeeded, false if not (insufficient space)</returns>
        bool Write(ref Input i, ref byte* dst, int length);
        
        /// <summary>
        /// Read output from source memory location, increment pointer by amount read
        /// </summary>
        /// <param name="src">Source memory location</param>
        /// <returns>Output</returns>
        Output ReadOutput(ref byte* src);
    }
}