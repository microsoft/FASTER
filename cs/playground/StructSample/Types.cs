// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;
using System.IO;
using System.Runtime.CompilerServices;

namespace StructSample
{
    public struct LongComparer : IFasterEqualityComparer<long>
    {
        public bool Equals(ref long k1, ref long k2) => k1 == k2;
        public long GetHashCode64(ref long k) => Utility.GetHashCode(k);
    }

    public struct Key : IFasterEqualityComparer<Key>
    {
        public long kfield1;
        public long kfield2;

        public long GetHashCode64(ref Key key)
        {
            return Utility.GetHashCode(key.kfield1) ^ Utility.GetHashCode(key.kfield2);
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
