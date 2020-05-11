// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

namespace FasterPSFSample
{
    public interface IOrders
    {
        // Colors, strings, and enums are not blittable so we use int
        int Size { get; set; }

        int Color { get; set; }

        int NumSold { get; set; }

        (int, int, int) MemberTuple => (this.Size, this.Color, this.NumSold);
    }
}
