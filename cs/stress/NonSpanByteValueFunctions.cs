// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;

namespace FASTER.stress
{
    internal class NonSpanByteValueFunctions<TKey, TValue> : SimpleFunctions<TKey, TValue, Empty>
    {
        public override bool SingleWriter(ref TKey key, ref TValue input, ref TValue src, ref TValue dst, ref TValue output, ref UpsertInfo upsertInfo, WriteReason reason)
            => ConcurrentWriter(ref key, ref input, ref src, ref dst, ref output, ref upsertInfo);

        public override bool ConcurrentWriter(ref TKey key, ref TValue input, ref TValue src, ref TValue dst, ref TValue output, ref UpsertInfo upsertInfo)
        {
            output = dst = src;
            return true;
        }

        public override bool InitialUpdater(ref TKey key, ref TValue input, ref TValue value, ref TValue output, ref RMWInfo rmwInfo)
        {
            output = value = input;
            return true;
        }

        public override bool CopyUpdater(ref TKey key, ref TValue input, ref TValue oldValue, ref TValue newValue, ref TValue output, ref RMWInfo rmwInfo)
        {
            output = newValue = input;
            return true;
        }

        public override bool InPlaceUpdater(ref TKey key, ref TValue input, ref TValue value, ref TValue output, ref RMWInfo rmwInfo)
        {
            output = value = input;
            return true;
        }
    }
}
