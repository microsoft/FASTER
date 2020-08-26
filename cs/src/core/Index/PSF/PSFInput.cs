// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

namespace FASTER.core
{
    /// <summary>
    /// Input to PsfInternalReadAddress on the primary (stores user values) FasterKV to retrieve the Key and Value
    /// for a logicalAddress returned from the secondary FasterKV instances.  This class is FasterKV-provider-specific.
    /// </summary>
    /// <typeparam name="TKey">The type of the key for user values</typeparam>
    public unsafe struct PSFInputPrimaryReadAddress<TKey>
        where TKey : new()
    {
        internal PSFInputPrimaryReadAddress(long readLA)
        {
            this.ReadLogicalAddress = readLA;
        }

        public long ReadLogicalAddress { get; set; }
    }

    /// <summary>
    /// Input to operations on the secondary FasterKV instance (stores PSF chains) for everything
    /// except reading based on a LogicalAddress.
    /// </summary>
    public unsafe struct PSFInputSecondary<TPSFKey> : IDisposable
        where TPSFKey : new()
    {
        private SectorAlignedMemory keyPointerMem;

        internal PSFInputSecondary(long groupId, int psfOrdinal)
        {
            this.keyPointerMem = null;
            this.GroupId = groupId;
            this.PsfOrdinal = psfOrdinal;
            this.IsDelete = false;
            this.ReadLogicalAddress = Constants.kInvalidAddress;
        }

        internal void SetQueryKey(SectorAlignedBufferPool pool, KeyAccessor<TPSFKey> keyAccessor, ref TPSFKey key)
        {
            // Create a varlen CompositeKey with just one item. This is ONLY used as the query key to QueryPSF.
            this.keyPointerMem = pool.Get(keyAccessor.KeyPointerSize);
            ref KeyPointer<TPSFKey> keyPointer = ref Unsafe.AsRef<KeyPointer<TPSFKey>>(keyPointerMem.GetValidPointer());
            keyPointer.Initialize(this.PsfOrdinal, ref key);
        }

        /// <summary>
        /// The ID of the <see cref="PSFGroup{TProviderData, TPSFKey, TRecordId}"/> for this operation.
        /// </summary>
        public long GroupId { get; }

        /// <summary>
        /// The ordinal of the <see cref="PSF{TPSFKey, TRecordId}"/> in the group <see cref="GroupId"/>for this operation.
        /// </summary>
        public int PsfOrdinal { get; set; }

        /// <summary>
        /// Whether this is a Delete (or the Delete part of an RCU)
        /// </summary>
        public bool IsDelete { get; set; }

        /// <summary>
        /// The logical address to read in one of the PsfRead*Address methods
        /// </summary>
        public long ReadLogicalAddress { get; set; }

        /// <summary>
        /// The query key for a QueryPSF method
        /// </summary>
        public ref TPSFKey QueryKeyRef => ref Unsafe.AsRef<TPSFKey>(this.keyPointerMem.GetValidPointer());

        public void Dispose()
        {
            if (!(this.keyPointerMem is null))
                this.keyPointerMem.Return();
        }
    }
}
