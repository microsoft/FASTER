// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

namespace FASTER.common
{
    /// <summary>
    /// Wire format for a session, you can add custom session types on the server and client side
    /// (e.g., one per distinct store and/or function types).
    /// </summary>
    public enum WireFormat : byte
    {
        /// <summary>
        /// Default varlen KV based on SpanByte (binary)
        /// </summary>
        DefaultVarLenKV = 0,

        /// <summary>
        /// Default fixed-len KV with 8-byte keys and values (binary)
        /// </summary>
        DefaultFixedLenKV = 1,

        /// <summary>
        /// Similar to DefaultVarLenKV but with WebSocket headers (binary)
        /// </summary>
        WebSocket = 2,
        
        DarqRead,
        DarqWrite,

        /// <summary>
        /// ASCII wire format (non-binary protocol)
        /// </summary>
        ASCII = 255
    }
}
