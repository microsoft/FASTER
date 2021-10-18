// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

namespace FASTER.common
{
    /// <summary>
    /// Wire format for a session, you can add custom session types on the server and client side
    /// (e.g., one per distinct store and/or function types).
    /// </summary>
    public enum NetworkProtocol : byte
    {
        /// <summary>
        /// Use TCP for communication between FASTER client and server
        /// </summary>
        TCP = 0,

        /// <summary>
        /// Use WebSockets for communication between FASTER client and server
        /// </summary>
        WebSocket = 1
    }
}
