// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System.Drawing;

namespace FasterPSFSample
{
    public struct CombinedKey : IFasterEqualityComparer<CombinedKey>
    {
        public int ValueType;
        public int ValueInt;

        public CombinedKey(Constants.Size size)
        {
            this.ValueType = (int)Constants.ValueType.Size;
            this.ValueInt = (int)size;
        }

        public CombinedKey(Color color)
        {
            this.ValueType = (int)Constants.ValueType.Color;
            this.ValueInt = color.ToArgb();
        }

        public CombinedKey(int countBin)
        {
            this.ValueType = (int)Constants.ValueType.Count;
            this.ValueInt = countBin;
        }

        public override string ToString()
        {
            var valueType = (Constants.ValueType)this.ValueType;
            var valueTypeString = $"{valueType}";
            return valueType switch
            {
                Constants.ValueType.Size => $"{valueTypeString}: {(Constants.Size)this.ValueInt}",
                Constants.ValueType.Color => $"{valueTypeString}: {Constants.ColorDict[this.ValueInt]}",
                Constants.ValueType.Count => $"{valueTypeString}: {this.ValueInt}",
                _ => throw new System.NotImplementedException("Unknown ValueType")
            };
        }

        public long GetHashCode64(ref CombinedKey key) 
            => (long)key.ValueType << 32 | (uint)key.ValueInt.GetHashCode();

        public bool Equals(ref CombinedKey k1, ref CombinedKey k2) 
            => k1.ValueType == k2.ValueType && k1.ValueInt == k2.ValueInt;
    }
}
