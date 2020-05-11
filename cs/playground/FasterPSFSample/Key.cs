// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System.Drawing;

namespace FasterPSFSample
{
    public struct Key : IFasterEqualityComparer<Key>, IOrders
    {
        public int Size { get; set; }

        public int Color { get; set; }

        public int NumSold { get; set; }

        public Key(Constants.Size size, Color color, int numSold)
        {
            this.Size = (int)size;
            this.Color = color.ToArgb();
            this.NumSold = numSold;
        }

        public (int, int, int) MemberTuple => (this.Size, this.Color, this.NumSold);

        public override string ToString() => $"{(Constants.Size)this.Size}, {Constants.ColorDict[this.Color].Name}, {NumSold}";

        private long AsLong => (this.Size + this.Color) << 30 | this.NumSold;

        public long GetHashCode64(ref Key key) => Utility.GetHashCode(key.AsLong);

        public bool Equals(ref Key k1, ref Key k2) => k1.MemberTuple == k2.MemberTuple;
    }
}
