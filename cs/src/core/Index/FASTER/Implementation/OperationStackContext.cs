// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;

namespace FASTER.core
{
    internal struct OperationStackContext<Key, Value>
    {
        // Note: Cannot use ref fields because they are not supported before net7.0.
        internal HashEntryInfo hei;
        internal RecordSource<Key, Value> recSrc;

        internal OperationStackContext(long keyHash) => this.hei = new(keyHash);

        /// <summary>
        /// Sets <see cref="recSrc"/> to the current <see cref="hei"/>.<see cref="HashEntryInfo.Address"/>, which is the address it had
        /// at the time of last retrieval from the hash table.
        /// </summary>
        /// <param name="srcLog">The FasterKV's hlog</param>
        internal void SetRecordSourceToHashEntry(AllocatorBase<Key, Value> srcLog) => this.recSrc.Set(hei.Address, srcLog);

        /// <summary>
        /// Sets <see cref="recSrc"/> to the current <see cref="hei"/>.<see cref="HashEntryInfo.CurrentAddress"/>, which is the current address
        /// in the hash table. This is the same effect as calling <see cref="FasterKV{Key, Value}.FindTag(ref HashEntryInfo)"/>.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void UpdateRecordSourceToCurrentHashEntry()
        {
            this.hei.SetToCurrent();
            this.SetRecordSourceToHashEntry(this.recSrc.Log);
        }

        /// <summary>
        /// If this is not <see cref="Constants.kInvalidAddress"/>, it is the logical Address allocated by CreateNewRecord*; if an exception
        /// occurs, this needs to be set invalid and non-tentative by the caller's 'finally' (to avoid another try/finally overhead).
        /// </summary>
        internal long newLogicalAddress;

        /// <summary>
        /// Called during normal operations when a record insertion fails, to set the new record invalid and non-tentative.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void SetNewRecordInvalid(ref RecordInfo newRecordInfo)
        {
            newRecordInfo.SetInvalid();
            this.newLogicalAddress = Constants.kInvalidAddress;
        }

        /// <summary>
        /// Called during normal operations when a record insertion fails, to set the new record invalid and non-tentative.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void SetNewRecordInvalidAtomic(ref RecordInfo newRecordInfo)
        {
            newRecordInfo.SetInvalidAtomic();
            this.newLogicalAddress = Constants.kInvalidAddress;
        }

        /// <summary>
        /// Called during normal operations when a record insertion succeeds, to set the new record non-tentative (permanent).
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void ClearNewRecordTentativeBitAtomic(ref RecordInfo newRecordInfo)
        {
            newRecordInfo.ClearTentativeBitAtomic();
            this.newLogicalAddress = Constants.kInvalidAddress;
        }

        /// <summary>
        /// Called during InternalXxx 'finally' handler, to set the new record invalid and non-tentative if an exception or other error occurred.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void HandleNewRecordOnError(FasterKV<Key, Value> fkv)
        {
            if (this.newLogicalAddress != Constants.kInvalidAddress)
            {
                fkv.SetRecordInvalid(newLogicalAddress);
                this.newLogicalAddress = Constants.kInvalidAddress;
            }
        }
    }
}
