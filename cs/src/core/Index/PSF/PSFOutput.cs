// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace FASTER.core
{
    /// <summary>
    /// The additional output interface passed to the PSF functions for internal Insert, Read, etc. operations.
    /// </summary>
    /// <typeparam name="TKey">The type of the Key, either a <see cref="CompositeKey{TPSFKey}"/> for the 
    ///     secondary FasterKV instances, or the user's TKVKey for the primary FasterKV instance.</typeparam>
    /// <typeparam name="TValue">The type of the Value, either a TRecordId for the 
    ///     secondary FasterKV instances, or the user's TKVValue for the primary FasterKV instance.</typeparam>
    public interface IPSFOutput<TKey, TValue>
    {
        PSFOperationStatus Visit(int psfOrdinal, ref TKey key, ref TValue value, bool tombstone, bool isConcurrent);

        PSFOperationStatus Visit(int psfOrdinal, long physicalAddress, ref TValue value, bool tombstone, bool isConcurrent);
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

        public PSFOperationStatus Visit(int psfOrdinal, long physicalAddress, ref TKVValue value, bool tombstone, bool isConcurrent)
            => throw new PSFInternalErrorException("Cannot call this form of Visit() on the primary FKV");  // TODO review error messages
    }

    /// <summary>
    /// Output from operations on the secondary FasterKV instance (stores PSF chains).
    /// </summary>
    /// <typeparam name="TPSFKey">The type of the key returned from a <see cref="PSF{TPSFKey, TRecordId}"/></typeparam>
    /// <typeparam name="TRecordId">The type of the provider's record identifier</typeparam>
    public unsafe class PSFOutputSecondary<TPSFKey, TRecordId> : IPSFOutput<TPSFKey, TRecordId>
        where TPSFKey : struct
        where TRecordId : struct
    {
        private readonly KeyAccessor<TPSFKey> keyAccessor;

        internal TRecordId RecordId { get; private set; }

        internal bool Tombstone { get; private set; }

        internal long PreviousLogicalAddress { get; private set; }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal PSFOutputSecondary(KeyAccessor<TPSFKey> keyAcc)
        {
            this.keyAccessor = keyAcc;
            this.RecordId = default;
            this.PreviousLogicalAddress = Constants.kInvalidAddress;
        }

        public PSFOperationStatus Visit(int psfOrdinal, ref TPSFKey key,
                                        ref TRecordId value, bool tombstone, bool isConcurrent)
        {
            // This is the secondary FKV; we hold onto the RecordId and create the provider data when QueryPSF returns.
            this.RecordId = value;
            this.Tombstone = tombstone;
            ref CompositeKey<TPSFKey> compositeKey = ref CompositeKey<TPSFKey>.CastFromFirstKeyPointerRefAsKeyRef(ref key);
            ref KeyPointer<TPSFKey> keyPointer = ref this.keyAccessor.GetKeyPointerRef(ref compositeKey, psfOrdinal);
            Debug.Assert(keyPointer.PsfOrdinal == (ushort)psfOrdinal, "Visit found mismatched PSF ordinal");
            this.PreviousLogicalAddress = keyPointer.PreviousAddress;
            return new PSFOperationStatus(OperationStatus.SUCCESS);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public PSFOperationStatus Visit(int psfOrdinal, long physicalAddress,
                                        ref TRecordId value, bool tombstone, bool isConcurrent)
        {
            // This is the secondary FKV; we hold onto the RecordId and create the provider data when QueryPSF returns.
            this.RecordId = value;
            this.Tombstone = tombstone;
            ref KeyPointer<TPSFKey> keyPointer = ref this.keyAccessor.GetKeyPointerRef(physicalAddress);
            Debug.Assert(keyPointer.PsfOrdinal == (ushort)psfOrdinal, "Visit found mismatched PSF ordinal");
            this.PreviousLogicalAddress = keyPointer.PreviousAddress;
            return new PSFOperationStatus(OperationStatus.SUCCESS);
        }
    }
}
