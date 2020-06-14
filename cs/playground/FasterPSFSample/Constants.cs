// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Drawing;

namespace FasterPSFSample
{
    public static class Constants
    {
        // Colors, strings, and enums are not blittable so we store int
        public enum Size
        {
            Small,
            Medium,
            Large,
            XLarge,
            XXLarge,
            NumSizes
        }

        static internal Dictionary<int, Color> ColorDict = new Dictionary<int, Color>
        {
            [Color.Black.ToArgb()] = Color.Black,
            [Color.Red.ToArgb()] = Color.Red,
            [Color.Green.ToArgb()] = Color.Green,
            [Color.Blue.ToArgb()] = Color.Blue,
            [Color.Purple.ToArgb()] = Color.Purple
        };

        static internal Color[] Colors = { Color.Black, Color.Red, Color.Green, Color.Blue, Color.Purple };

        public enum ValueType
        {
            Size,
            Color,
            Count
        }
    }
}
