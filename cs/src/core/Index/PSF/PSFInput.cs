// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

namespace FASTER.core
{
    /// <summary>
    /// The additional input interface passed to the PSF functions for internal Insert, Read, etc. operations.
    /// </summary>
    /// <typeparam name="TKey">The type of the Key, either a <see cref="CompositeKey{TPSFKey}"/> for the 
    ///     secondary FasterKV instances, or the user's TKVKey for the primary FasterKV instance.</typeparam>
    /// <remarks>The interface separation is needed for the PendingContext, and for the "TPSFKey : struct"
    ///     constraint in PSFInputSecondary</remarks>
    public interface IPSFInput<TKey>
    {
        unsafe void SetFlags(PSFResultFlags* resultFlags);

        /// <summary>
        /// The ID of the <see cref="PSFGroup{TProviderData, TPSFKey, TRecordId}"/> for this operation.
        /// </summary>
        long GroupId { get; }

        /// <summary>
        /// The ordinal of the <see cref="PSF{TPSFKey, TRecordId}"/> being queried; writable only for Insert.
        /// </summary>
        int PsfOrdinal { get; set; }

        /// <summary>
        /// For Insert(), determine if the result of the <see cref="PSF{TPSFKey, TRecordId}"/> at <see cref="PsfOrdinal"/> 
        /// was null; if so the value did not match and should not be stored in the key's chain.
        /// </summary>
        bool IsNullAt { get; }

        /// <summary>
        /// For Delete() or Insert() done as part of RCU, this indicates if it the tombstone should be set for this record.
        /// </summary>
        bool IsDelete { get; set; }

        /// <summary>
        /// For tracing back the chain, this is the next logicalAddress to get.
        /// </summary>
        long ReadLogicalAddress { get; set; }

        ref TKey QueryKeyRef { get; }
    }

    // TODO: Trim IPSFInput to only what PSFInputPrimaryReadAddress needs

    /// <summary>
    /// Input to PsfInternalReadAddress on the primary (stores user values) FasterKV to retrieve the Key and Value
    /// for a logicalAddress returned from the secondary FasterKV instances.  This class is FasterKV-provider-specific.
    /// </summary>
    /// <typeparam name="TKey">The type of the key for user values</typeparam>
    public unsafe class PSFInputPrimaryReadAddress<TKey> : IPSFInput<TKey>
    {
        internal PSFInputPrimaryReadAddress(long readLA)
        {
            this.ReadLogicalAddress = readLA;
        }

        /// <inheritdoc/>
        public long GroupId => throw new PSFInvalidOperationException("Not valid for Primary FasterKV");

        /// <inheritdoc/>
        public int PsfOrdinal
        { 
            get => Constants.kInvalidPsfOrdinal;
            set => throw new PSFInvalidOperationException("Not valid for Primary FasterFKV");
        }

        public void SetFlags(PSFResultFlags* resultFlags)
            => throw new PSFInvalidOperationException("Not valid for Primary FasterKV");

        public bool IsNullAt => throw new PSFInvalidOperationException("Not valid for Primary FasterKV");

        public bool IsDelete
        {
            get => throw new PSFInvalidOperationException("Not valid for Primary FasterKV");
            set => throw new PSFInvalidOperationException("Not valid for Primary FasterKV");
        }

        public long ReadLogicalAddress { get; set; }

        public ref TKey QueryKeyRef => throw new PSFInvalidOperationException("Not valid for Primary FasterKV");
    }

    /// <summary>
    /// Input to operations on the secondary FasterKV instance (stores PSF chains) for everything
    /// except reading based on a LogicalAddress.
    /// </summary>
    public unsafe class PSFInputSecondary<TPSFKey> : IPSFInput<CompositeKey<TPSFKey>>, IDisposable
        where TPSFKey : struct
    {
        internal readonly KeyAccessor<TPSFKey> keyAccessor;
        internal PSFResultFlags* resultFlags;
        private SectorAlignedMemory keyPointerMem;

        internal PSFInputSecondary(int psfOrdinal, KeyAccessor<TPSFKey> keyAcc, long groupId, PSFResultFlags* flags = null)
        {
            this.PsfOrdinal = psfOrdinal;
            this.keyAccessor = keyAcc;
            this.GroupId = groupId;
            this.resultFlags = flags;
            this.ReadLogicalAddress = Constants.kInvalidAddress;
        }

        internal void SetQueryKey(SectorAlignedBufferPool pool, ref TPSFKey key)
        {
            // Create a varlen CompositeKey with just one item. This is ONLY used as the query key to QueryPSF.
            this.keyPointerMem = pool.Get(this.keyAccessor.KeyPointerSize);
            ref KeyPointer<TPSFKey> keyPointer = ref Unsafe.AsRef<KeyPointer<TPSFKey>>(keyPointerMem.GetValidPointer());
            keyPointer.PrevAddress = Constants.kInvalidAddress;
            keyPointer.PsfOrdinal = (ushort)this.PsfOrdinal;
            keyPointer.Key = key;
        }

        public long GroupId { get; }

        public int PsfOrdinal { get; set; }

        public void SetFlags(PSFResultFlags* resultFlags) => this.resultFlags = resultFlags;

        public bool IsNullAt => this.resultFlags[this.PsfOrdinal].HasFlag(PSFResultFlags.IsNull);

        public bool IsDelete { get; set; }

        public long ReadLogicalAddress { get; set; }

        public ref CompositeKey<TPSFKey> QueryKeyRef
            => ref Unsafe.AsRef<CompositeKey<TPSFKey>>(this.keyPointerMem.GetValidPointer());

        public void Dispose()
        {
            if (!(this.keyPointerMem is null))
                this.keyPointerMem.Return();
        }
    }
}
