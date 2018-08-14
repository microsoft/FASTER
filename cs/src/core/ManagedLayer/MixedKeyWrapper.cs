// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

//#define BLIT_KEY
using System;
using System.IO;
using System.Runtime.CompilerServices;



namespace FASTER.core
{
    [FASTER.core.Roslyn.TypeKind("internal")]
#if BLIT_KEY
    public unsafe struct MixedKeyWrapper
    {
        public MixedKey key;

        public static long GetHashCode(MixedKeyWrapper* key)
        {
            return UserType.Convert(key).GetHashCode();
        }

        public static bool Equals(MixedKeyWrapper* k1, MixedKeyWrapper* k2)
        {
            return UserType.Convert(k1).Equals(UserType.Convert(k2));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int GetLength(MixedKeyWrapper* key)
        {
            return sizeof(MixedKeyWrapper);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void Copy(MixedKeyWrapper* src, MixedKeyWrapper* dst)
        {
            dst->key = src->key;
        }

    #region Serialization
        public static bool HasObjectsToSerialize()
        {
            return false;
        }

        public static void Serialize(MixedKeyWrapper* key, Stream toStream)
        {
            throw new NotImplementedException();
        }

        public static void Deserialize(MixedKeyWrapper* key, Stream fromStream)
        {
            throw new NotImplementedException();
        }

        public static void Free(MixedKeyWrapper* key)
        {
            throw new NotImplementedException();
        }

    #endregion

        public static MixedKeyWrapper* MoveToContext(MixedKeyWrapper* key)
        {
            var addr = (MixedKeyWrapper*)
                MallocFixedPageSize<MixedKeyWrapper>.PhysicalInstance.Allocate();
            Copy(key, addr);
            return addr;
        }
    }
#else
    public unsafe struct MixedKeyWrapper
    {
        public BlittableTypeWrapper key;

        public static long GetHashCode(MixedKeyWrapper* key)
        {
            return key->key.GetObject<MixedKey>().GetHashCode64();
        }

        public static bool Equals(MixedKeyWrapper* k1, MixedKeyWrapper* k2)
        {
            return k1->key.GetObject<MixedKey>().Equals(k2->key.GetObject<MixedKey>());
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int GetLength(MixedKeyWrapper* key)
        {
            return sizeof(void*);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void Copy(MixedKeyWrapper* src, MixedKeyWrapper* dst)
        {
            dst->key = BlittableTypeWrapper.Create(src->key.GetObject<MixedKey>().Clone());
        }

#region Serialization
        public static bool HasObjectsToSerialize()
        {
            return true;
        }

        public static void Serialize(MixedKeyWrapper* key, Stream toStream)
        {
            key->key.GetObject<MixedKey>().Serialize(toStream);
        }

        public static void Deserialize(MixedKeyWrapper* key, Stream fromStream)
        {
            MixedKey k = new MixedKey();
            k.Deserialize(fromStream);
            key->key = BlittableTypeWrapper.Create(k);
        }

        public static void Free(MixedKeyWrapper* key)
        {
            key->key.Free<MixedKey>();
        }
#endregion

        public static MixedKeyWrapper* MoveToContext(MixedKeyWrapper* key)
        {
            var keyPtr = (MixedKeyWrapper*)
                MallocFixedPageSize<long>.PhysicalInstance.Allocate();
            *keyPtr = *key;
            return keyPtr;
        }
    }
#endif

}
