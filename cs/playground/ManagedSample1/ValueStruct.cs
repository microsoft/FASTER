// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;
using System.IO;
using System.Runtime.CompilerServices;

namespace ManagedSampleCore
{
    public struct ValueStruct : IValue<ValueStruct>
    {
        public const int physicalSize = sizeof(long) + sizeof(long);
        public long vfield1;
        public long vfield2;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int GetLength()
        {
            return physicalSize;
        }

        public void ShallowCopy(ref ValueStruct dst)
        {
            dst.vfield1 = vfield1;
            dst.vfield2 = vfield2;
        }

        // Shared read/write capabilities on value
        public void AcquireReadLock()
        {
        }

        public void ReleaseReadLock()
        {
        }

        public void AcquireWriteLock()
        {
        }

        public void ReleaseWriteLock()
        {
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

        public void Free()
        {
            throw new InvalidOperationException();
        }
        #endregion

        public ref ValueStruct MoveToContext(ref ValueStruct value)
        {
            return ref value;
        }
    }
}
