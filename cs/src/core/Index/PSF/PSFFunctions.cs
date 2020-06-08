// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

using System;

namespace FASTER.core
{
    /// <summary>
    /// The Functions for the PSFValue-wrapped Value; mostly pass-through
    /// </summary>
    /// <typeparam name="TPSFKey">The type of the <see cref="PSF{TPSFKey, TRecordId}"/> result key</typeparam>
    /// <typeparam name="TRecordId">The type of the <see cref="PSF{TPSFKey, TRecordId}"/> value</typeparam>
    public class PSFFunctions<TPSFKey, TRecordId> : IFunctions<PSFCompositeKey<TPSFKey>, PSFValue<TRecordId>, PSFInputSecondary<TPSFKey>,
                                                               PSFOutputSecondary<TPSFKey, TRecordId>, PSFContext>
        where TPSFKey: struct
        where TRecordId: struct
    {
        IPSFValueAccessor<PSFValue<TRecordId>> psfValueAccessor;

        // TODO: remove stuff that has been moved to PSFOutput.Visit, etc.

        internal PSFFunctions(IPSFValueAccessor<PSFValue<TRecordId>> accessor)
            => this.psfValueAccessor = accessor;

        #region Upserts
        public bool ConcurrentWriter(ref PSFCompositeKey<TPSFKey> _, ref PSFValue<TRecordId> src, ref PSFValue<TRecordId> dst)
        {
            src.CopyTo(ref dst, this.psfValueAccessor.RecordIdSize, this.psfValueAccessor.PSFCount);
            return true;
        }

        public void SingleWriter(ref PSFCompositeKey<TPSFKey> _, ref PSFValue<TRecordId> src, ref PSFValue<TRecordId> dst)
            => src.CopyTo(ref dst, this.psfValueAccessor.RecordIdSize, this.psfValueAccessor.PSFCount);

        public void UpsertCompletionCallback(ref PSFCompositeKey<TPSFKey> _, ref PSFValue<TRecordId> value, PSFContext ctx)
        { /* TODO: UpsertCompletionCallback */ }
        #endregion Upserts

        #region Reads
        public void ConcurrentReader(ref PSFCompositeKey<TPSFKey> key, ref PSFInputSecondary<TPSFKey> input, ref PSFValue<TRecordId> value, ref PSFOutputSecondary<TPSFKey, TRecordId> dst)
            => throw new PSFInternalErrorException("PSFOutput.Visit instead of ConcurrentReader should be called on PSF-implementing FasterKVs");

        public unsafe void SingleReader(ref PSFCompositeKey<TPSFKey> _, ref PSFInputSecondary<TPSFKey> input, ref PSFValue<TRecordId> value, ref PSFOutputSecondary<TPSFKey, TRecordId> dst)
            => throw new PSFInternalErrorException("PSFOutput.Visit instead of SingleReader should be called on PSF-implementing FasterKVs");

        public void ReadCompletionCallback(ref PSFCompositeKey<TPSFKey> _, ref PSFInputSecondary<TPSFKey> input, ref PSFOutputSecondary<TPSFKey, TRecordId> output, PSFContext ctx, Status status)
        { /* TODO: ReadCompletionCallback */ }
        #endregion Reads

        #region RMWs
        public void CopyUpdater(ref PSFCompositeKey<TPSFKey> _, ref PSFInputSecondary<TPSFKey> input, ref PSFValue<TRecordId> oldValue, ref PSFValue<TRecordId> newValue)
            => throw new PSFInternalErrorException("RMW should not be done on PSF-implementing FasterKVs");

        public void InitialUpdater(ref PSFCompositeKey<TPSFKey> _, ref PSFInputSecondary<TPSFKey> input, ref PSFValue<TRecordId> value)
            => throw new PSFInternalErrorException("RMW should not be done on PSF-implementing FasterKVs");

        public bool InPlaceUpdater(ref PSFCompositeKey<TPSFKey> _, ref PSFInputSecondary<TPSFKey> input, ref PSFValue<TRecordId> value)
            => throw new PSFInternalErrorException("RMW should not be done on PSF-implementing FasterKVs");

        public void RMWCompletionCallback(ref PSFCompositeKey<TPSFKey> _, ref PSFInputSecondary<TPSFKey> input, PSFContext ctx, Status status)
            => throw new PSFInternalErrorException("RMW should not be done on PSF-implementing FasterKVs");
        #endregion RMWs

        public void DeleteCompletionCallback(ref PSFCompositeKey<TPSFKey> _, PSFContext ctx)
        { /* TODO: DeleteCompletionCallback */ }

        public void CheckpointCompletionCallback(string sessionId, CommitPoint commitPoint)
        { /* TODO: CheckpointCompletionCallback */ }
    }
}
