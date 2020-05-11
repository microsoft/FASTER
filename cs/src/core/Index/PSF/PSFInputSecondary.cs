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
    public interface IPSFInput<TKey>
    {
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
        public int PsfOrdinal
        { 
            get => Constants.kInvalidPsfOrdinal;
            set => throw new InvalidOperationException("Not valid for reading from Primary Address");
        }

        public bool IsNullAt => throw new InvalidOperationException("Not valid for reading from Primary Address");

        public long ReadLogicalAddress { get; set; }

        public long GetHashCode64At(ref TKey key)
            => throw new InvalidOperationException("Not valid for reading from Primary Address");

        public bool EqualsAt(ref TKey queryKey, ref TKey storedKey)
            => throw new InvalidOperationException("Not valid for reading from Primary Address");
    }

    /// <summary>
    /// Input to operations on the secondary FasterKV instance (stores PSF chains) for everything
    /// except reading based on a LogicalAddress.
    /// </summary>
    public unsafe class PSFInputSecondary<TPSFKey> : IPSFInput<PSFCompositeKey<TPSFKey>>
        where TPSFKey : struct
    {
        internal readonly ICompositeKeyComparer<PSFCompositeKey<TPSFKey>> comparer;
        internal readonly bool* nullIndicators;

        internal PSFInputSecondary(int psfOrd, ICompositeKeyComparer<PSFCompositeKey<TPSFKey>> keyCmp,
                          bool* nullIndicators = null)
        {
            this.PsfOrdinal = psfOrd;
            this.comparer = keyCmp;
            this.nullIndicators = nullIndicators;
            this.ReadLogicalAddress = Constants.kInvalidAddress;
        }

        public int PsfOrdinal { get; set; }

        public bool IsNullAt => this.nullIndicators[this.PsfOrdinal];

        public long ReadLogicalAddress { get; set; }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetHashCode64At(ref PSFCompositeKey<TPSFKey> cKey)
            => this.comparer.GetHashCode64(this.PsfOrdinal, ref cKey);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool EqualsAt(ref PSFCompositeKey<TPSFKey> queryKey, ref PSFCompositeKey<TPSFKey> storedKey)
            => this.comparer.Equals(this.nullIndicators is null, this.PsfOrdinal, ref queryKey, ref storedKey);
    }
}
