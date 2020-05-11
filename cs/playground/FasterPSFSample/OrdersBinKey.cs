// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;

namespace FasterPSFSample
{
    public struct OrdersBinKey : IFasterEqualityComparer<OrdersBinKey>
    {
        internal const int BinSize = 100;
        internal const int MaxOrders = BinSize * 10;
        internal const int MaxBin = 8; // 0-based; skip the last one

        public int Bin;

        public OrdersBinKey(int bin) => this.Bin = bin;

        internal bool GetBin(int numOrders, out int bin)
        {
            bin = numOrders / BinSize;
            return bin < MaxBin;
        }

        // Make the hashcode for this distinct from size enum values
        public long GetHashCode64(ref OrdersBinKey key) => Utility.GetHashCode(this.Bin + 1000);

        public bool Equals(ref OrdersBinKey k1, ref OrdersBinKey k2) => k1.Bin == k2.Bin;
    }
}
