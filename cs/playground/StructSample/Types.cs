// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;
using System.IO;
using System.Runtime.CompilerServices;

namespace StructSampleCore
{
    public struct Key : IFasterEqualityComparer<Key>
    {
        public long kfield1;
        public long kfield2;

        public long GetHashCode64(ref Key key)
        {
            return Utility.GetHashCode(key.kfield1);
        }
        public bool Equals(ref Key k1, ref Key k2)
        {
            return k1.kfield1 == k2.kfield1 && k1.kfield2 == k2.kfield2;
        }
    }

    public struct Value
    {
        public long vfield1;
        public long vfield2;
    }

    public struct Input
    {
        public long ifield1;
        public long ifield2;
    }

    public struct Output
    {
        public Value value;
    }
}
