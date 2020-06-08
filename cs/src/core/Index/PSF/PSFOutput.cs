// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

using System.Collections.Concurrent;
using System.Runtime.CompilerServices;

namespace FASTER.core
{
    /// <summary>
    /// The additional output interface passed to the PSF functions for internal Insert, Read, etc. operations.
    /// </summary>
    /// <typeparam name="TKey">The type of the Key, either a <see cref="PSFCompositeKey{TPSFKey}"/> for the 
    ///     secondary FasterKV instances, or the user's TKVKey for the primary FasterKV instance.</typeparam>
    /// <typeparam name="TValue">The type of the Key, either a <see cref="PSFValue{TRecordId}"/> for the 
    ///     secondary FasterKV instances, or the user's TKVValue for the primary FasterKV instance.</typeparam>
    public interface IPSFOutput<TKey, TValue>
    {
        PSFOperationStatus Visit(int psfOrdinal, ref TKey key, ref TValue value, bool tombstone, bool isConcurrent);
    }

    /// <summary>
    /// Output from ReadInternal on the primary (stores user values) FasterKV instance when reading
    /// based on a LogicalAddress rather than a key. This class is FasterKV-provider-specific.
    /// </summary>
    /// <typeparam name="TKVKey">The type of the key for user values</typeparam>
    /// <typeparam name="TKVValue">The type of the user values</typeparam>
    public unsafe class PSFOutputPrimaryReadAddress<TKVKey, TKVValue> : IPSFOutput<TKVKey, TKVValue>
        where TKVKey : new()
        where TKVValue : new()
    {
        private readonly AllocatorBase<TKVKey, TKVValue> allocator;

        internal ConcurrentQueue<FasterKVProviderData<TKVKey, TKVValue>> ProviderDatas { get; private set; }

        internal PSFOutputPrimaryReadAddress(AllocatorBase<TKVKey, TKVValue> alloc,
                                             ConcurrentQueue<FasterKVProviderData<TKVKey, TKVValue>> provDatas)
        {
            this.allocator = alloc;
            this.ProviderDatas = provDatas;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public PSFOperationStatus Visit(int psfOrdinal, ref TKVKey key, ref TKVValue value, bool tombstone, bool isConcurrent)
        {
            // tombstone is not used here; it is only needed for the chains in the secondary FKV.
            this.ProviderDatas.Enqueue(new FasterKVProviderData<TKVKey, TKVValue>(this.allocator, ref key, ref value));
            return new PSFOperationStatus(OperationStatus.SUCCESS);
        }
    }

    /// <summary>
    /// Output from operations on the secondary FasterKV instance (stores PSF chains).
    /// </summary>
    /// <typeparam name="TPSFKey">The type of the key returned from a <see cref="PSF{TPSFKey, TRecordId}"/></typeparam>
    /// <typeparam name="TRecordId">The type of the provider's record identifier</typeparam>
    public unsafe class PSFOutputSecondary<TPSFKey, TRecordId> : IPSFOutput<PSFCompositeKey<TPSFKey>, PSFValue<TRecordId>>
        where TPSFKey : struct
        where TRecordId : struct
    {
        readonly IPSFValueAccessor<PSFValue<TRecordId>> psfValueAccessor;

        internal TRecordId RecordId { get; private set; }

        internal bool Tombstone { get; private set; }

        internal long PreviousLogicalAddress { get; private set; }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal PSFOutputSecondary(IPSFValueAccessor<PSFValue<TRecordId>> accessor)
        {
            this.psfValueAccessor = accessor;
            this.RecordId = default;
            this.PreviousLogicalAddress = Constants.kInvalidAddress;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public PSFOperationStatus Visit(int psfOrdinal, ref PSFCompositeKey<TPSFKey> key,
                               ref PSFValue<TRecordId> value, bool tombstone, bool isConcurrent)
        {
            // This is the secondary FKV so we wait to create provider data until QueryPSF returns.
            this.RecordId = value.RecordId;
            this.Tombstone = tombstone;
            this.PreviousLogicalAddress = *(this.psfValueAccessor.GetChainLinkPtrs(ref value) + psfOrdinal);
            return new PSFOperationStatus(OperationStatus.SUCCESS);
        }
    }
}
