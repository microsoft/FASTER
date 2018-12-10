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
        public const int physicalSize = sizeof(long) + sizeof(long);
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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetLength()
        {
            return physicalSize;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void ShallowCopy(ref KeyStruct dst)
        {
            dst.kfield1 = kfield1;
            dst.kfield2 = kfield2;
        }

        #region Serialization
        public bool HasObjectsToSerialize()
        {
            return false;
        }

        public void Serialize(Stream toStream)
        {
            throw new InvalidOperationException();
        }

        public void Deserialize(Stream fromStream)
        {
            throw new InvalidOperationException();
        }
        #endregion
    }
}
