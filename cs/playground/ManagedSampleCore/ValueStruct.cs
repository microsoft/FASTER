// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Runtime.CompilerServices;

namespace StructSample
{
    public unsafe struct ValueStruct
    {
        public const int physicalSize = sizeof(long) + sizeof(long);
        public long vfield1;
        public long vfield2;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int GetLength(ValueStruct* input)
        {
            return physicalSize;
        }

        public static void Copy(ValueStruct* src, ValueStruct* dst)
        {
            dst->vfield1 = src->vfield1;
            dst->vfield2 = src->vfield2;
        }

        // Shared read/write capabilities on value
        public static void AcquireReadLock(ValueStruct* value)
        {
        }

        public static void ReleaseReadLock(ValueStruct* value)
        {
        }

        public static void AcquireWriteLock(ValueStruct* value)
        {
        }

        public static void ReleaseWriteLock(ValueStruct* value)
        {
        }

        #region Serialization
        public static bool HasObjectsToSerialize()
        {
            return false;
        }

        public static void Serialize(ValueStruct* key, Stream toStream)
        {
            throw new InvalidOperationException();
        }

        public static void Deserialize(ValueStruct* key, Stream fromStream)
        {
            throw new InvalidOperationException();
        }

        public static void Free(ValueStruct* key)
        {
            throw new InvalidOperationException();
        }
        #endregion

        public static ValueStruct* MoveToContext(ValueStruct* value)
        {
            return value;
        }
    }
}
