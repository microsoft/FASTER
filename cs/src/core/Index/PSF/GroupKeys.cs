// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;

namespace FASTER.core
{
    internal struct GroupKeys : IDisposable
    {
        // This cannot be typed to a TPSFKey because there may be different TPSFKeys across groups.
        private SectorAlignedMemory compositeKeyMem;
        private SectorAlignedMemory flagsMem;

        internal void Set(SectorAlignedMemory keyMem, SectorAlignedMemory flagsMem)
        {
            this.compositeKeyMem = keyMem;
            this.flagsMem = flagsMem;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal unsafe ref TCompositeOrIndividualKey CastToKeyRef<TCompositeOrIndividualKey>()
            => ref Unsafe.AsRef<TCompositeOrIndividualKey>(this.compositeKeyMem.GetValidPointer());

        internal unsafe PSFResultFlags* ResultFlags => (PSFResultFlags*)this.flagsMem.GetValidPointer();

        internal unsafe bool IsNullAt(int psfOrdinal) => (this.ResultFlags + psfOrdinal)->HasFlag(PSFResultFlags.IsNull);
        public unsafe bool IsUnlinkOldAt(int psfOrdinal) => (this.ResultFlags + psfOrdinal)->HasFlag(PSFResultFlags.UnlinkOld);
        public unsafe bool IsLinkNewAt(int psfOrdinal) => (this.ResultFlags + psfOrdinal)->HasFlag(PSFResultFlags.LinkNew);

        public unsafe bool HasChanges => this.ResultFlags->HasFlag(PSFResultFlags.UnlinkOld) || this.ResultFlags->HasFlag(PSFResultFlags.LinkNew);

        public void Dispose()
        {
            this.compositeKeyMem?.Return();
            this.flagsMem?.Return();
        }
    }

    internal struct GroupKeysPair : IDisposable
    {
        internal long GroupId;

        // If the PSFGroup found the RecordId in its IPUCache, we carry it here.
        internal long LogicalAddress;

        internal GroupKeys Before;
        internal GroupKeys After;

        internal GroupKeysPair(long id)
        {
            this.GroupId = id;
            this.LogicalAddress = Constants.kInvalidAddress;
            this.Before = default;
            this.After = default;
        }

        internal bool HasAddress => this.LogicalAddress != Constants.kInvalidAddress;

        internal ref TCompositeKey GetBeforeKey<TCompositeKey>() => ref this.Before.CastToKeyRef<TCompositeKey>();

        internal ref TCompositeKey GetAfterKey<TCompositeKey>() => ref this.After.CastToKeyRef<TCompositeKey>();

        internal bool HasChanges => this.After.HasChanges;

        public void Dispose()
        {
            this.Before.Dispose();
            this.After.Dispose();
        }
    }
}
