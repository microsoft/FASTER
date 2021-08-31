// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Linq;
using FASTER.core;

namespace FASTER.server
{
    /// <summary>
    /// Byte array equality comparer
    /// </summary>
    public class ByteArrayComparer : IEqualityComparer<byte[]>
    {
        /// <summary>
        /// Equals
        /// </summary>
        /// <param name="left"></param>
        /// <param name="right"></param>
        /// <returns></returns>
        public bool Equals(byte[] left, byte[] right)
            => new ReadOnlySpan<byte>(left).SequenceEqual(new ReadOnlySpan<byte>(right));

        /// <summary>
        /// Get hash code
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public unsafe int GetHashCode(byte[] key)
        {
            fixed (byte* k = key)
            {
                return (int)Utility.HashBytes(k, key.Length);
            }
        }
    }
}
