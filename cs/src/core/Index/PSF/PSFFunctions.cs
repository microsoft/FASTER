// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace FASTER.core
{
    // Non-generic interface
    internal interface IPSFFunctions { }

    internal interface IPSFFunctions<TKey, TValue, TInput, TOutput> : IPSFFunctions
    {
        #region Input accessors

        long GroupId(ref TInput input);

        bool IsDelete(ref TInput input);
        bool SetDelete(ref TInput input, bool value);

        public long ReadLogicalAddress(ref TInput input);

        public ref TKey QueryKeyRef(ref TInput input);
        #endregion Input accessors

        #region Output visitors
        PSFOperationStatus VisitPrimaryReadAddress(ref TKey key, ref TValue value, ref TOutput output, bool isConcurrent);

        PSFOperationStatus VisitSecondaryRead(ref TValue value, ref TInput input, ref TOutput output, long physicalAddress, bool tombstone, bool isConcurrent);

        PSFOperationStatus VisitSecondaryRead(ref TKey key, ref TValue value, ref TInput input, ref TOutput output, bool tombstone, bool isConcurrent);
        #endregion Output visitors
    }

    /// <summary>
    /// The Functions for the TRecordId (which is the Value param to the secondary FasterKV); mostly pass-through
    /// </summary>
    /// <typeparam name="TKVKey">The type of the user key in the primary Faster KV</typeparam>
    /// <typeparam name="TKVValue">The type of the user value in the primary Faster KV</typeparam>
    internal class PSFPrimaryFunctions<TKVKey, TKVValue> : StubbedFunctions<TKVKey, TKVValue, PSFInputPrimaryReadAddress<TKVKey>, PSFOutputPrimaryReadAddress<TKVKey, TKVValue>>,
                                                         IPSFFunctions<TKVKey, TKVValue, PSFInputPrimaryReadAddress<TKVKey>, PSFOutputPrimaryReadAddress<TKVKey, TKVValue>>
        where TKVKey : new()
        where TKVValue : new()
    {
        public long GroupId(ref PSFInputPrimaryReadAddress<TKVKey> input) => throw new PSFInternalErrorException("Cannot call this accessor on the primary FKV");

        public bool IsDelete(ref PSFInputPrimaryReadAddress<TKVKey> input) => throw new PSFInternalErrorException("Cannot call this accessor on the primary FKV");
        public bool SetDelete(ref PSFInputPrimaryReadAddress<TKVKey> input, bool value) => throw new PSFInternalErrorException("Cannot call this accessor on the primary FKV");

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long ReadLogicalAddress(ref PSFInputPrimaryReadAddress<TKVKey> input) => input.ReadLogicalAddress;

        public ref TKVKey QueryKeyRef(ref PSFInputPrimaryReadAddress<TKVKey> input) => throw new PSFInternalErrorException("Cannot call this accessor on the primary FKV");

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public PSFOperationStatus VisitPrimaryReadAddress(ref TKVKey key, ref TKVValue value, ref PSFOutputPrimaryReadAddress<TKVKey, TKVValue> output, bool isConcurrent)
        {
            // Tombstone is not needed here; it is only needed for the chains in the secondary FKV.
            output.ProviderDatas.Enqueue(new FasterKVProviderData<TKVKey, TKVValue>(output.allocator, ref key, ref value));
            return new PSFOperationStatus(OperationStatus.SUCCESS);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public PSFOperationStatus VisitSecondaryRead(ref TKVValue value, ref PSFInputPrimaryReadAddress<TKVKey> input, ref PSFOutputPrimaryReadAddress<TKVKey, TKVValue> output,
                                                     long physicalAddress, bool tombstone, bool isConcurrent)
            => throw new PSFInternalErrorException("Cannot call this form of Visit() on the primary FKV");  // TODO review error messages

        public PSFOperationStatus VisitSecondaryRead(ref TKVKey key, ref TKVValue value, ref PSFInputPrimaryReadAddress<TKVKey> input, ref PSFOutputPrimaryReadAddress<TKVKey, TKVValue> output,
                                                          bool tombstone, bool isConcurrent)
            => throw new PSFInternalErrorException("Cannot call this form of Visit() on the primary FKV");  // TODO review error messages
    }

    /// <summary>
    /// The Functions for the TRecordId (which is the Value param to the secondary FasterKV); mostly pass-through
    /// </summary>
    /// <typeparam name="TPSFKey">The type of the <see cref="PSF{TPSFKey, TRecordId}"/> result key</typeparam>
    /// <typeparam name="TRecordId">The type of the <see cref="PSF{TPSFKey, TRecordId}"/> value</typeparam>
    public class PSFSecondaryFunctions<TPSFKey, TRecordId> : StubbedFunctions<TPSFKey, TRecordId, PSFInputSecondary<TPSFKey>, PSFOutputSecondary<TPSFKey, TRecordId>>,
                                                             IPSFFunctions<TPSFKey, TRecordId, PSFInputSecondary<TPSFKey>, PSFOutputSecondary<TPSFKey, TRecordId>>
        where TPSFKey : new()
        where TRecordId : new()
    {
        public long GroupId(ref PSFInputSecondary<TPSFKey> input) => input.GroupId;

        public bool IsDelete(ref PSFInputSecondary<TPSFKey> input) => input.IsDelete;
        public bool SetDelete(ref PSFInputSecondary<TPSFKey> input, bool value) => input.IsDelete = value;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long ReadLogicalAddress(ref PSFInputSecondary<TPSFKey> input) => input.ReadLogicalAddress;

        public ref TPSFKey QueryKeyRef(ref PSFInputSecondary<TPSFKey> input) => ref input.QueryKeyRef;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public PSFOperationStatus VisitPrimaryReadAddress(ref TPSFKey key, ref TRecordId value, ref PSFOutputSecondary<TPSFKey, TRecordId> output, bool isConcurrent)
            => throw new PSFInternalErrorException("Cannot call this form of Visit() on the secondary FKV");

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public PSFOperationStatus VisitSecondaryRead(ref TRecordId value, ref PSFInputSecondary<TPSFKey> input, ref PSFOutputSecondary<TPSFKey, TRecordId> output,
                                                     long physicalAddress, bool tombstone, bool isConcurrent)
        {
            // This is the secondary FKV; we hold onto the RecordId and create the provider data when QueryPSF returns.
            output.RecordId = value;
            output.Tombstone = tombstone;
            ref KeyPointer<TPSFKey> keyPointer = ref output.keyAccessor.GetKeyPointerRef(physicalAddress);
            Debug.Assert(keyPointer.PsfOrdinal == input.PsfOrdinal, "Visit found mismatched PSF ordinal");
            output.PreviousLogicalAddress = keyPointer.PreviousAddress;
            return new PSFOperationStatus(OperationStatus.SUCCESS);
        }

        public PSFOperationStatus VisitSecondaryRead(ref TPSFKey key, ref TRecordId value, ref PSFInputSecondary<TPSFKey> input, ref PSFOutputSecondary<TPSFKey, TRecordId> output,
                                                     bool tombstone, bool isConcurrent)
        {
            // This is the secondary FKV; we hold onto the RecordId and create the provider data when QueryPSF returns.
            output.RecordId = value;
            output.Tombstone = tombstone;
            ref CompositeKey<TPSFKey> compositeKey = ref CompositeKey<TPSFKey>.CastFromFirstKeyPointerRefAsKeyRef(ref key);
            ref KeyPointer<TPSFKey> keyPointer = ref output.keyAccessor.GetKeyPointerRef(ref compositeKey, input.PsfOrdinal);
            Debug.Assert(keyPointer.PsfOrdinal == input.PsfOrdinal, "Visit found mismatched PSF ordinal");
            output.PreviousLogicalAddress = keyPointer.PreviousAddress;
            return new PSFOperationStatus(OperationStatus.SUCCESS);
        }
    }

    public class StubbedFunctions<TKey, TValue, TInput, TOutput> : IFunctions<TKey, TValue, TInput, TOutput, PSFContext>
    {
        // All IFunctions methods are unused; the IFunctions "implementation" is to satisfy the ClientSession requirement. We use only the Visit methods.
        #region IFunctions stubs
        private const string MustUseVisitMethod = "PSF-implementing FasterKVs must use one of the Visit* methods";

        #region Upserts
        public bool ConcurrentWriter(ref TKey _, ref TValue src, ref TValue dst) => throw new PSFInternalErrorException(MustUseVisitMethod);

        public void SingleWriter(ref TKey _, ref TValue src, ref TValue dst) => throw new PSFInternalErrorException(MustUseVisitMethod);

        public void UpsertCompletionCallback(ref TKey _, ref TValue value, PSFContext ctx) => throw new PSFInternalErrorException(MustUseVisitMethod);
        #endregion Upserts

        #region Reads
        public void ConcurrentReader(ref TKey key, ref TInput input, ref TValue value, ref TOutput dst)
            => throw new PSFInternalErrorException(MustUseVisitMethod);

        public unsafe void SingleReader(ref TKey _, ref TInput input, ref TValue value, ref TOutput dst)
            => throw new PSFInternalErrorException(MustUseVisitMethod);

        public void ReadCompletionCallback(ref TKey _, ref TInput input, ref TOutput output, PSFContext ctx, Status status)
            => throw new PSFInternalErrorException(MustUseVisitMethod);
        #endregion Reads

        #region RMWs
        public void CopyUpdater(ref TKey _, ref TInput input, ref TValue oldValue, ref TValue newValue)
            => throw new PSFInternalErrorException(MustUseVisitMethod);

        public void InitialUpdater(ref TKey _, ref TInput input, ref TValue value)
            => throw new PSFInternalErrorException(MustUseVisitMethod);

        public bool InPlaceUpdater(ref TKey _, ref TInput input, ref TValue value)
            => throw new PSFInternalErrorException(MustUseVisitMethod);

        public void RMWCompletionCallback(ref TKey _, ref TInput input, PSFContext ctx, Status status)
            => throw new PSFInternalErrorException(MustUseVisitMethod);
        #endregion RMWs

        public void DeleteCompletionCallback(ref TKey _, PSFContext ctx)
            => throw new PSFInternalErrorException(MustUseVisitMethod);

        public void CheckpointCompletionCallback(string sessionId, CommitPoint commitPoint)
            => throw new PSFInternalErrorException(MustUseVisitMethod);
        #endregion IFunctions stubs
    }
}
