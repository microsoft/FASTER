// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System.Drawing;

namespace FasterPSFSample
{
    public struct ColorKey : IFasterEqualityComparer<ColorKey>
    {
        public int ColorArgb;

        public ColorKey(Color color) => this.ColorArgb = color.ToArgb();

        public override string ToString() => Constants.ColorDict[this.ColorArgb].Name;

        public long GetHashCode64(ref ColorKey key) => Utility.GetHashCode(key.ColorArgb);

        public bool Equals(ref ColorKey k1, ref ColorKey k2) => k1.ColorArgb == k2.ColorArgb;
    }
}
