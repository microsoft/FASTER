// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;

namespace FasterPSFSample
{
    public struct SizeKey : IFasterEqualityComparer<SizeKey>
    {
        public int SizeInt;

        public SizeKey(Constants.Size size) => this.SizeInt = (int)size;

        public override string ToString() => ((Constants.Size)this.SizeInt).ToString();

        public long GetHashCode64(ref SizeKey key) => Utility.GetHashCode(key.SizeInt);

        public bool Equals(ref SizeKey k1, ref SizeKey k2) => k1.SizeInt == k2.SizeInt;
    }
}
