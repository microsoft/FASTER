// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;

namespace FasterPSFSample
{
    public struct CountBinKey : IFasterEqualityComparer<CountBinKey>
    {
        internal const int BinSize = 100;
        internal const int MaxOrders = BinSize * 10;
        internal const int LastBin = 9; // 0-based
        internal static bool WantLastBin;

        public int Bin;

        public CountBinKey(int bin) => this.Bin = bin;

        internal static bool GetBin(int numOrders, out int bin)
        {
            // Skip the last bin during initial inserts to illustrate not matching the PSF (returning null)
            bin = numOrders / BinSize;
            return WantLastBin || bin < LastBin;
        }

        // Make the hashcode for this distinct from size enum values
        public long GetHashCode64(ref CountBinKey key) => Utility.GetHashCode(key.Bin + 1000);

        public bool Equals(ref CountBinKey k1, ref CountBinKey k2) => k1.Bin == k2.Bin;
    }
}
