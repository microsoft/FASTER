// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;

namespace FASTER.common
{
    /// <summary>
    /// Byte array comparer
    /// </summary>
    public class ByteArrayComparer : IEqualityComparer<byte[]>, IComparer<byte[]>
    {
        /// <inheritdoc />
        public bool Equals(byte[] left, byte[] right)
        {
            if (left == null || right == null)
                return left == right;
            if (left.Length != right.Length)
                return false;
            for (int i = 0; i < left.Length; i++)
            {
                if (left[i] != right[i])
                    return false;
            }
            return true;
        }

        /// <inheritdoc />
        public int GetHashCode(byte[] key)
        {
            if (key == null)
                throw new ArgumentNullException("key");
            int sum = 0;
            foreach (byte cur in key)
                sum += cur;
            return sum;
        }

        /// <inheritdoc />
        int IComparer<byte[]>.Compare(byte[] x, byte[] y)
        {
            if (x.Length > y.Length)
                return 1;

            if (x.Length < y.Length)
                return -1;

            for (int idx = 0; idx < x.Length; idx++)
            {
                if (x[idx] > y[idx])
                    return 1;
                if (y[idx] > x[idx])
                    return -1;
            }
            return 0;
        }
    }
}
