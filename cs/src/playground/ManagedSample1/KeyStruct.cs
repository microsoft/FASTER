// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;
using System.IO;
using System.Runtime.CompilerServices;

namespace ManagedSample1
{
    public unsafe struct KeyStruct
    {
        public const int physicalSize = sizeof(long) + sizeof(long);
        public long kfield1;
        public long kfield2;

        public static long GetHashCode(KeyStruct* key)
        {
            return Utility.GetHashCode(*((long*)key));
        }
        public static bool Equals(KeyStruct* k1, KeyStruct* k2)
        {
            return k1->kfield1 == k2->kfield1 && k1->kfield2 == k2->kfield2;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int GetLength(KeyStruct* key)
        {
            return physicalSize;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void Copy(KeyStruct* src, KeyStruct* dst)
        {
            dst->kfield1 = src->kfield1;
            dst->kfield2 = src->kfield2;
        }

        #region Serialization
        public static bool HasObjectsToSerialize()
        {
            return false;
        }

        public static void Serialize(KeyStruct* key, Stream toStream)
        {
            throw new NotImplementedException();
        }

        public static void Deserialize(KeyStruct* key, Stream fromStream)
        {
            throw new NotImplementedException();
        }

        public static void Free(KeyStruct* key)
        {
            throw new NotImplementedException();
        }
        #endregion

        public static KeyStruct* MoveToContext(KeyStruct* key)
        {
            return key;
        }
    }
}
