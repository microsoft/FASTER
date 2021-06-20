// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

namespace FASTER.common
{
    /// <summary>
    /// Wire format
    /// </summary>
    public enum WireFormat : byte
    {
        /// <summary>
        /// Default varlen KV (binary)
        /// </summary>
        DefaultVarLenKV = 0,

        /// <summary>
        /// Default fixed-len KV with 8-byte keys and values (binary)
        /// </summary>
        DefaultFixedLenKV = 1,

        /// <summary>
        /// ASCII wire format (non-binary protocol)
        /// </summary>
        ASCII = 255
    }
}
