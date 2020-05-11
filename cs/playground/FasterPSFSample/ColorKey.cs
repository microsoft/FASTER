// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System.Drawing;

namespace FasterPSFSample
{
    public struct ColorKey : IFasterEqualityComparer<ColorKey>
    {
        public int Color;

        public ColorKey(Color color) => this.Color = color.ToArgb();

        public override string ToString() => Constants.ColorDict[this.Color].Name;

        public long GetHashCode64(ref ColorKey key) => Utility.GetHashCode(key.Color);

        public bool Equals(ref ColorKey k1, ref ColorKey k2) => k1.Color == k2.Color;
    }
}
