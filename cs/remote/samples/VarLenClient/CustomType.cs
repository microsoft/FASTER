// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Runtime.InteropServices;

namespace VarLenClient
{
    [StructLayout(LayoutKind.Explicit, Size = 12)]
    public struct CustomType
    {
        [FieldOffset(0)]
        public int length;
        [FieldOffset(4)]
        public long payload;

        public CustomType(long payload)
        {
            length = 8;
            this.payload = payload;
        }
    }
}
