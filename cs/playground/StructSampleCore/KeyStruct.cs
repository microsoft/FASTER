// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;
using System.IO;
using System.Runtime.CompilerServices;

namespace StructSampleCore
{
    public struct KeyStruct : IKey<KeyStruct>
    {
        public long kfield1;
        public long kfield2;

        public long GetHashCode64()
        {
            return Utility.GetHashCode(kfield1);
        }
        public bool Equals(ref KeyStruct k2)
        {
            return kfield1 == k2.kfield1 && kfield2 == k2.kfield2;
        }
    }
}
