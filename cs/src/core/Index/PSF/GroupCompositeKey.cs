// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;

namespace FASTER.core
{
    internal struct GroupCompositeKey : IDisposable
    {
        // This cannot be typed to a TPSFKey because there may be different TPSFKeys across groups.
        private SectorAlignedMemory KeyPointerMem;

        internal void Set(SectorAlignedMemory keyMem) => this.KeyPointerMem = keyMem;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal unsafe ref TCompositeOrIndividualKey CastToKeyRef<TCompositeOrIndividualKey>()
            => ref Unsafe.AsRef<TCompositeOrIndividualKey>(this.KeyPointerMem.GetValidPointer());

        public void Dispose() => this.KeyPointerMem?.Return();
    }

    internal struct GroupCompositeKeyPair : IDisposable
    {
        internal long GroupId;

        // If the PSFGroup found the RecordId in its IPUCache, we carry it here.
        internal long LogicalAddress;

        internal GroupCompositeKey Before;
        internal GroupCompositeKey After;

        internal GroupCompositeKeyPair(long id)
        {
            this.GroupId = id;
            this.LogicalAddress = Constants.kInvalidAddress;
            this.Before = default;
            this.After = default;
            this.HasChanges = false;
        }

        internal bool HasAddress => this.LogicalAddress != Constants.kInvalidAddress;

        internal ref TCompositeKey GetBeforeKey<TCompositeKey>() => ref this.Before.CastToKeyRef<TCompositeKey>();

        internal ref TCompositeKey GetAfterKey<TCompositeKey>() => ref this.After.CastToKeyRef<TCompositeKey>();

        internal bool HasChanges;

        public void Dispose()
        {
            this.Before.Dispose();
            this.After.Dispose();
        }
    }
}
