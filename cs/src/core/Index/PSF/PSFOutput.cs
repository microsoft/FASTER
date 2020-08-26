// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

using System.Collections.Concurrent;
using System.Runtime.CompilerServices;

namespace FASTER.core
{
    /// <summary>
    /// Output from ReadInternal on the primary (stores user values) FasterKV instance when reading
    /// based on a LogicalAddress rather than a key. This class is FasterKV-provider-specific.
    /// </summary>
    /// <typeparam name="TKVKey">The type of the key for user values</typeparam>
    /// <typeparam name="TKVValue">The type of the user values</typeparam>
    public unsafe struct PSFOutputPrimaryReadAddress<TKVKey, TKVValue>
        where TKVKey : new()
        where TKVValue : new()
    {
        internal readonly AllocatorBase<TKVKey, TKVValue> allocator;

        internal ConcurrentQueue<FasterKVProviderData<TKVKey, TKVValue>> ProviderDatas { get; private set; }

        internal PSFOutputPrimaryReadAddress(AllocatorBase<TKVKey, TKVValue> alloc,
                                             ConcurrentQueue<FasterKVProviderData<TKVKey, TKVValue>> provDatas)
        {
            this.allocator = alloc;
            this.ProviderDatas = provDatas;
        }
#if false
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public PSFOperationStatus Visit(int psfOrdinal, ref TKVKey key, ref TKVValue value, bool tombstone, bool isConcurrent)
        {
            // tombstone is not used here; it is only needed for the chains in the secondary FKV.
            this.ProviderDatas.Enqueue(new FasterKVProviderData<TKVKey, TKVValue>(this.allocator, ref key, ref value));
            return new PSFOperationStatus(OperationStatus.SUCCESS);
        }

        public PSFOperationStatus Visit(int psfOrdinal, long physicalAddress, ref TKVValue value, bool tombstone, bool isConcurrent)
            => throw new PSFInternalErrorException("Cannot call this form of Visit() on the primary FKV");  // TODO review error messages
#endif
    }

    /// <summary>
    /// Output from operations on the secondary FasterKV instance (stores PSF chains).
    /// </summary>
    /// <typeparam name="TPSFKey">The type of the key returned from a <see cref="PSF{TPSFKey, TRecordId}"/></typeparam>
    /// <typeparam name="TRecordId">The type of the provider's record identifier</typeparam>
    public unsafe struct PSFOutputSecondary<TPSFKey, TRecordId>
        where TPSFKey : new()
        where TRecordId : new()
    {
        internal readonly KeyAccessor<TPSFKey> keyAccessor;

        internal TRecordId RecordId { get; set; }

        internal bool Tombstone { get; set; }

        internal long PreviousLogicalAddress { get; set; }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal PSFOutputSecondary(KeyAccessor<TPSFKey> keyAcc)
        {
            this.keyAccessor = keyAcc;
            this.RecordId = default;
            this.Tombstone = false;
            this.PreviousLogicalAddress = Constants.kInvalidAddress;
        }

#if false
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
#endif
    }
}
