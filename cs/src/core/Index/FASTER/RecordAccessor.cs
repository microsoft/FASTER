// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;

namespace FASTER.core
{
    /// <summary>
    /// Provides an accessor to the record at a given logical address, for use in IFunctions callbacks.
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
            if (!IsLogAddress(logicalAddress))
                throw new FasterException(IsReadCacheAddress(logicalAddress) ? "Invalid use of ReadCache address" : "Invalid logical address");
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private unsafe ref RecordInfo GetRecordInfo(long logicalAddress)
        {
            VerifyAddress(logicalAddress);
            return ref this.fkv.hlog.GetInfo(this.fkv.hlog.GetPhysicalAddress(logicalAddress));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal IHeapContainer<Key> GetKeyContainer(ref Key key) => this.fkv.hlog.GetKeyContainer(ref key);

        #region public interface

        /// <summary>
        /// Indicates whether the address is within the FasterKV HybridLog
        /// </summary>
        /// <param name="logicalAddress">The address to verify</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool IsReadCacheAddress(long logicalAddress)
            => this.fkv.UseReadCache && ((logicalAddress & Constants.kReadCacheBitMask) != 0);

        /// <summary>
        /// Indicates whether the address is within the FasterKV HybridLog
        /// </summary>
        /// <param name="logicalAddress">The address to verify</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool IsLogAddress(long logicalAddress)
            => logicalAddress >= this.fkv.Log.BeginAddress && logicalAddress <= this.fkv.Log.TailAddress && !IsReadCacheAddress(logicalAddress);

        /// <summary>
        /// Indicates whether the address is in memory within the FasterKV HybridLog
        /// </summary>
        /// <param name="logicalAddress">The address to verify</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool IsInMemory(long logicalAddress)
            => IsLogAddress(logicalAddress) && logicalAddress >= this.fkv.Log.HeadAddress;

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
        /// Returns whether Value is an object type.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool ValueHasObjects() => this.fkv.hlog.ValueHasObjects();

        /// <summary>
        /// Returns the version number of the record at the given logical address
        /// </summary>
        /// <param name="logicalAddress">The address to examine</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ref Value GetValue(long logicalAddress) => ref this.fkv.hlog.GetValue(this.fkv.hlog.GetPhysicalAddress(logicalAddress));

        #endregion public interface
    }
}
