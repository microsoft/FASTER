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
    /// <typeparam name="TKey">The type of the Key, either a <see cref="PSFCompositeKey{TPSFKey}"/> for the 
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

        /// <summary>
        /// Get the hashcode of the key of the <see cref="PSF{TPSFKey, TRecordId}"/> at <see cref="PsfOrdinal"/>
        /// </summary>
        long GetHashCode64At(ref TKey cKey);

        /// <summary>
        /// Determine if the keys match for the <see cref="PSF{TPSFKey, TRecordId}"/>s
        /// at <see cref="PsfOrdinal"/>. For query, the queryKey is a composite consisting of only one key, and
        /// its ordinal is always zero.
        /// </summary>
        /// <param name="queryKey">The composite key whose value is being matched with the store. If the
        ///     operation is Query, this is a composite consisting of only one key, the query key</param>
        /// <param name="storedKey">The composite key in storage being compared to</param>
        /// <returns></returns>
        bool EqualsAt(ref TKey queryKey, ref TKey storedKey);
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

        public long GetHashCode64At(ref TKey key)
            => throw new PSFInvalidOperationException("Not valid for Primary FasterKV");

        public bool EqualsAt(ref TKey queryKey, ref TKey storedKey)
            => throw new PSFInvalidOperationException("Not valid for Primary FasterKV");
    }

    /// <summary>
    /// Input to operations on the secondary FasterKV instance (stores PSF chains) for everything
    /// except reading based on a LogicalAddress.
    /// </summary>
    public unsafe class PSFInputSecondary<TPSFKey> : IPSFInput<PSFCompositeKey<TPSFKey>>
        where TPSFKey : struct
    {
        internal readonly ICompositeKeyComparer<PSFCompositeKey<TPSFKey>> comparer;
        internal PSFResultFlags* resultFlags;

        internal PSFInputSecondary(int psfOrd, ICompositeKeyComparer<PSFCompositeKey<TPSFKey>> keyCmp,
                                   long groupId, PSFResultFlags* flags = null)
        {
            this.PsfOrdinal = psfOrd;
            this.comparer = keyCmp;
            this.GroupId = groupId;
            this.resultFlags = flags;
            this.ReadLogicalAddress = Constants.kInvalidAddress;
        }

        public long GroupId { get; }

        public int PsfOrdinal { get; set; }

        public void SetFlags(PSFResultFlags* resultFlags) => this.resultFlags = resultFlags;

        public bool IsNullAt => this.resultFlags[this.PsfOrdinal].HasFlag(PSFResultFlags.IsNull);

        public bool IsDelete { get; set; }

        public long ReadLogicalAddress { get; set; }

        private bool IsQuery => this.resultFlags is null;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetHashCode64At(ref PSFCompositeKey<TPSFKey> cKey)
            => this.comparer.GetHashCode64(this.IsQuery ? 0 : this.PsfOrdinal, ref cKey);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool EqualsAt(ref PSFCompositeKey<TPSFKey> queryKey, ref PSFCompositeKey<TPSFKey> storedKey)
            => this.comparer.Equals(this.IsQuery, this.PsfOrdinal, ref queryKey, ref storedKey);
    }
}
