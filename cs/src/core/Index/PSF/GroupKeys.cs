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

        internal unsafe ref TCompositeKey GetCompositeKeyRef<TCompositeKey>()
            => ref Unsafe.AsRef<TCompositeKey>(this.compositeKeyMem.GetValidPointer());

        internal unsafe PSFResultFlags* ResultFlags => (PSFResultFlags*)this.flagsMem.GetValidPointer();

        internal unsafe bool IsNullAt(int ordinal) => (this.ResultFlags + ordinal)->HasFlag(PSFResultFlags.IsNull);
        public unsafe bool IsUnlinkOldAt(int ordinal) => (this.ResultFlags + ordinal)->HasFlag(PSFResultFlags.UnlinkOld);
        public unsafe bool IsLinkNewAt(int ordinal) => (this.ResultFlags + ordinal)->HasFlag(PSFResultFlags.LinkNew);

        public unsafe bool HasChanges => this.ResultFlags->HasFlag(PSFResultFlags.UnlinkOld)
                                      || this.ResultFlags->HasFlag(PSFResultFlags.LinkNew);

        public void Dispose()
        {
            this.compositeKeyMem.Return();
            this.flagsMem.Return();
        }
    }

    internal struct GroupKeysPair : IDisposable
    {
        internal long GroupId;
        internal int KeySize;

        // If the PSFGroup found the RecordId in its IPUCache, we carry it here.
        internal long LogicalAddress;

        internal GroupKeys Before;
        internal GroupKeys After;

        internal bool HasAddress => this.LogicalAddress != Constants.kInvalidAddress;

        internal ref TCompositeKey GetBeforeKey<TCompositeKey>()
            => ref this.Before.GetCompositeKeyRef<TCompositeKey>();

        internal ref TCompositeKey GetAfterKey<TCompositeKey>()
            => ref this.After.GetCompositeKeyRef<TCompositeKey>();

        internal bool HasChanges => this.After.HasChanges;

        public void Dispose()
        {
            this.Before.Dispose();
            this.After.Dispose();
        }
    }
}
