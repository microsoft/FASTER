// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Runtime.CompilerServices;

namespace SumStore
{
    public unsafe struct NumClicks
    {
        public const int physicalSize = sizeof(long);
        public long numClicks;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int GetLength(NumClicks* input)
        {
            return physicalSize;
        }

        public static void Copy(NumClicks* src, NumClicks* dst)
        {
            dst->numClicks = src->numClicks;
        }

        // Shared read/write capabilities on value
        public static void AcquireReadLock(NumClicks* value)
        {
        }

        public static void ReleaseReadLock(NumClicks* value)
        {
        }

        public static void AcquireWriteLock(NumClicks* value)
        {
        }

        public static void ReleaseWriteLock(NumClicks* value)
        {
        }

        public static NumClicks* MoveToContext(NumClicks* value)
        {
            return value;
        }

        #region Serialization
        public static bool HasObjectsToSerialize()
        {
            return false;
        }

        public static void Serialize(NumClicks* key, Stream toStream)
        {
            throw new NotImplementedException();
        }

        public static void Deserialize(NumClicks* key, Stream fromStream)
        {
            throw new NotImplementedException();
        }
        public static void Free(NumClicks* key)
        {
            throw new NotImplementedException();
        }
        #endregion
    }
}
