// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System.Threading;

namespace ReadAddress
{
    public struct Key
    {
        public long key;

        public Key(long first) => key = first;

        public override string ToString() => key.ToString();

        internal class Comparer : IFasterEqualityComparer<Key>
        {
            public long GetHashCode64(ref Key key) => Utility.GetHashCode(key.key);

            public bool Equals(ref Key k1, ref Key k2) => k1.key == k2.key;
        }
    }

    public struct Value
    {
        public long value;

        public Value(long first) => value = first;

        public override string ToString() => value.ToString();
    }

    /// <summary>
    /// Callback for FASTER operations
    /// </summary>
    public class Functions : SimpleFunctions<Key, Value>
    {
        // Return false to force a chain of values.
        public override bool ConcurrentWriter(ref Key key, ref Value input, ref Value src, ref Value dst, ref Value output, ref UpsertInfo upsertInfo) => false;

        public override bool InPlaceUpdater(ref Key key, ref Value input, ref Value value, ref Value output, ref RMWInfo rmwInfo) => false;
    }
}
