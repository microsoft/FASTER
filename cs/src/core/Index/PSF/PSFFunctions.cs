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
        IChainPost<PSFValue<TRecordId>> chainPost;

        // TODO: remove stuff that has been moved to PSFOutput, etc.

        internal PSFFunctions(IChainPost<PSFValue<TRecordId>> chainPost)
            => this.chainPost = chainPost;

        #region Upserts
        public bool ConcurrentWriter(ref PSFCompositeKey<TPSFKey> _, ref PSFValue<TRecordId> src, ref PSFValue<TRecordId> dst)
        {
            src.CopyTo(ref dst, this.chainPost.RecordIdSize, this.chainPost.ChainHeight);
            return true;
        }

        public void SingleWriter(ref PSFCompositeKey<TPSFKey> _, ref PSFValue<TRecordId> src, ref PSFValue<TRecordId> dst)
            => src.CopyTo(ref dst, this.chainPost.RecordIdSize, this.chainPost.ChainHeight);

        public void UpsertCompletionCallback(ref PSFCompositeKey<TPSFKey> _, ref PSFValue<TRecordId> value, PSFContext ctx)
        { /* TODO */ }
        #endregion Upserts

        #region Reads
        public void ConcurrentReader(ref PSFCompositeKey<TPSFKey> key, ref PSFInputSecondary<TPSFKey> input, ref PSFValue<TRecordId> value, ref PSFOutputSecondary<TPSFKey, TRecordId> dst)
            => this.SingleReader(ref key, ref input, ref value, ref dst);

        public unsafe void SingleReader(ref PSFCompositeKey<TPSFKey> _, ref PSFInputSecondary<TPSFKey> input, ref PSFValue<TRecordId> value, ref PSFOutputSecondary<TPSFKey, TRecordId> dst)
        {
            /* TODO is SingleReader still used?
            dst.RecordId = value.RecordId;
            dst.previousChainLinkLogicalAddress = *(this.chainPost.GetChainLinkPtrs(ref value) + input.PsfOrdinal);
            */
        }

        public void ReadCompletionCallback(ref PSFCompositeKey<TPSFKey> _, ref PSFInputSecondary<TPSFKey> input, ref PSFOutputSecondary<TPSFKey, TRecordId> output, PSFContext ctx, Status status)
        { /* TODO */ }
        #endregion Reads

        #region RMWs
        public void CopyUpdater(ref PSFCompositeKey<TPSFKey> _, ref PSFInputSecondary<TPSFKey> input, ref PSFValue<TRecordId> oldValue, ref PSFValue<TRecordId> newValue)
            => newValue = oldValue; // TODO ensure no deepcopy needed

        public void InitialUpdater(ref PSFCompositeKey<TPSFKey> _, ref PSFInputSecondary<TPSFKey> input, ref PSFValue<TRecordId> value)
        { /* TODO */ }

        public bool InPlaceUpdater(ref PSFCompositeKey<TPSFKey> _, ref PSFInputSecondary<TPSFKey> input, ref PSFValue<TRecordId> value)
        { return true; /* TODO */ }

        public void RMWCompletionCallback(ref PSFCompositeKey<TPSFKey> _, ref PSFInputSecondary<TPSFKey> input, PSFContext ctx, Status status)
        { /* TODO */ }
        #endregion RMWs

        public void DeleteCompletionCallback(ref PSFCompositeKey<TPSFKey> _, PSFContext ctx)
        { /* TODO */ }

        public void CheckpointCompletionCallback(string sessionId, CommitPoint commitPoint)
        { /* TODO */ }
    }
}
