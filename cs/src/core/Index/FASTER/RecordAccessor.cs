// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace FASTER.core
{
    /// <summary>
    /// Provides an accessor to the record at a given logical address, for use in IAdvancedFunctions callbacks.
    /// </summary>
    /// <typeparam name="Key"></typeparam>
    /// <typeparam name="Value"></typeparam>
    public class RecordAccessor<Key, Value>
    {
        private readonly FasterKV<Key, Value> fkv;

        internal RecordAccessor(FasterKV<Key, Value> fkv) => this.fkv = fkv;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void VerifyAddress(long logicalAddress)
        {
            if (!IsValid(logicalAddress))
                throw new FasterException("Invalid logical address");
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void VerifyIsInMemoryAddress(long logicalAddress)
        {
            VerifyAddress(logicalAddress);
            if (!IsInMemory(logicalAddress))
                throw new FasterException("Address is not in the in-memory portion of the log");
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private unsafe ref RecordInfo GetRecordInfo(long logicalAddress)
        {
            VerifyIsInMemoryAddress(logicalAddress);
            return ref this.fkv.hlog.GetInfo(this.fkv.hlog.GetPhysicalAddress(logicalAddress));
        }

        /// <summary>
        /// Indicates whether the address is within the FasterKV HybridLog
        /// </summary>
        /// <param name="logicalAddress">The address to verify</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool IsReadCacheAddress(long logicalAddress)
            => this.fkv.UseReadCache && ((logicalAddress & Constants.kReadCacheBitMask) != 0);

        #region public interface

        /// <summary>
        /// Indicates whether the address is within the FasterKV HybridLog logical address space (does not verify alignment)
        /// </summary>
        /// <param name="logicalAddress">The address to verify</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool IsValid(long logicalAddress) => logicalAddress >= this.fkv.Log.BeginAddress && logicalAddress <= this.fkv.Log.TailAddress && !IsReadCacheAddress(logicalAddress);

        /// <summary>
        /// Indicates whether the address is in memory within the FasterKV HybridLog
        /// </summary>
        /// <param name="logicalAddress">The address to verify</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool IsInMemory(long logicalAddress) => IsValid(logicalAddress) && logicalAddress >= this.fkv.Log.HeadAddress;

        /// <summary>
        /// Returns the previous logical address in the hash collision chain that includes the given logical address
        /// </summary>
        /// <param name="logicalAddress">The address to examine</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetPreviousAddress(long logicalAddress) => GetRecordInfo(logicalAddress).PreviousAddress;

        /// <summary>
        /// Returns whether the record at the given logical address is deleted
        /// </summary>
        /// <param name="logicalAddress">The address to examine</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool IsTombstone(long logicalAddress) => GetRecordInfo(logicalAddress).Tombstone;

        /// <summary>
        /// Returns the version number of the record at the given logical address
        /// </summary>
        /// <param name="logicalAddress">The address to examine</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Version(long logicalAddress) => GetRecordInfo(logicalAddress).Version;

        /// <summary>
        /// Locks the RecordInfo at address
        /// </summary>
        /// <param name="logicalAddress">The address to examine</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SpinLock(long logicalAddress)
        {
            Debug.Assert(logicalAddress >= this.fkv.Log.ReadOnlyAddress);
            GetRecordInfo(logicalAddress).SpinLock();
        }

        /// <summary>
        /// Unlocks the RecordInfo at address
        /// </summary>
        /// <param name="logicalAddress">The address to examine</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Unlock(long logicalAddress) => GetRecordInfo(logicalAddress).Unlock();

        #endregion public interface
    }
}
