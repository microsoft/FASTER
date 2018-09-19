// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;
using System.IO;
using System.Runtime.CompilerServices;

namespace SumStore
{
    public unsafe struct AdId
    {
        public const int physicalSize = sizeof(long);
        public long adId;

        public static long GetHashCode(AdId* key)
        {
            return Utility.GetHashCode(*((long*)key));
        }
        public static bool Equals(AdId* k1, AdId* k2)
        {
            return k1->adId == k2->adId;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int GetLength(AdId* key)
        {
            return physicalSize;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void Copy(AdId* src, AdId* dst)
        {
            dst->adId = src->adId;
        }

        public static AdId* MoveToContext(AdId* value)
        {
            return value;
        }
        #region Serialization
        public static bool HasObjectsToSerialize()
        {
            return false;
        }

        public static void Serialize(AdId* key, Stream toStream)
        {
            throw new InvalidOperationException();
        }

        public static void Deserialize(AdId* key, Stream fromStream)
        {
            throw new InvalidOperationException();
        }
        public static void Free(AdId* key)
        {
            throw new InvalidOperationException();
        }
        #endregion
    }
}
