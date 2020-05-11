// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;

namespace FasterPSFSample
{
    public struct SizeKey : IFasterEqualityComparer<SizeKey>
    {
        public int Size;

        public SizeKey(Constants.Size size) => this.Size = (int)size;

        public override string ToString() => ((Constants.Size)this.Size).ToString();

        public long GetHashCode64(ref SizeKey key) => Utility.GetHashCode(key.Size);

        public bool Equals(ref SizeKey k1, ref SizeKey k2) => k1.Size == k2.Size;
    }
}
