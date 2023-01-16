// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;
using System.Threading;

namespace FASTER.core
{
    /// <summary>Hash table entry information for a key</summary>
    internal unsafe struct HashEntryInfo
    {
        /// <summary>The hash bucket for this key</summary>
        internal HashBucket* bucket;

        /// <summary>The hash bucket entry slot for this key</summary>
        internal int slot;

        /// <summary>The hash bucket entry for this key</summary>
        internal HashBucketEntry entry;

        /// <summary>The hash code for this key</summary>
        internal readonly long hash;

        /// <summary>The hash tag for this key</summary>
        internal ushort tag;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal HashEntryInfo(long hash)
        {
            bucket = default;
            slot = default;
            entry = default;
            this.hash = hash;
            tag = (ushort)((ulong)this.hash >> Constants.kHashTagShift);
        }

        /// <summary>
        /// The original address of this hash entry (at the time of FindTag, etc.)
        /// </summary>
        internal long Address => entry.Address;
        internal long AbsoluteAddress => Utility.AbsoluteAddress(this.Address);

        /// <summary>
        /// The current address of this hash entry (which may have been updated (via CAS) in the bucket after FindTag, etc.)
        /// </summary>
        internal long CurrentAddress => new HashBucketEntry() { word = this.bucket->bucket_entries[this.slot] }.Address;
        internal long AbsoluteCurrentAddress => Utility.AbsoluteAddress(this.CurrentAddress);

        /// <summary>
        /// Return whether the <see cref="HashBucketEntry"/> has been updated
        /// </summary>
        internal bool IsNotCurrent => this.CurrentAddress != this.Address;

        /// <summary>
        /// Whether the original address for this hash entry (at the time of FindTag, etc.) is a readcache address.
        /// </summary>
        internal bool IsReadCache => entry.ReadCache;

        /// <summary>
        /// Whether the original address for this hash entry (at the time of FindTag, etc.) is a readcache address.
        /// </summary>
        internal bool IsCurrentReadCache => (this.bucket->bucket_entries[this.slot] & Constants.kReadCacheBitMask) != 0;

        /// <summary>
        /// Set members to the current entry (which may have been updated (via CAS) in the bucket after FindTag, etc.)
        /// </summary>
        internal void SetToCurrent() => this.entry = new HashBucketEntry() { word = this.bucket->bucket_entries[this.slot] };

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool TryCAS(long newLogicalAddress) => TryCAS(newLogicalAddress, this.tag);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool TryCAS(long newLogicalAddress, ushort tag)
        {
            // Insert as the first record in the hash chain.
            HashBucketEntry updatedEntry = new()
            {
                Tag = tag,
                Address = newLogicalAddress & Constants.kAddressMask,
                Pending = this.entry.Pending,
                Tentative = false
                // .ReadCache is included in newLogicalAddress
            };

            if (this.entry.word == Interlocked.CompareExchange(ref this.bucket->bucket_entries[this.slot], updatedEntry.word, this.entry.word))
            {
                this.entry.word = updatedEntry.word;
                this.tag = tag;
                return true;
            }
            return false;
        }

        public override string ToString()
        {
            if (bucket == null)
                return $"hash {this.hash} <no bucket>";

            var isRC = "(rc)";
            var addrRC = this.IsReadCache ? isRC : string.Empty;
            var currAddrRC = this.IsCurrentReadCache ? isRC : string.Empty;
            var isNotCurr = this.Address == this.CurrentAddress ? string.Empty : "*";

            // The debugger often can't call the Globalization NegativeSign property so ToString() would just display the class name
            var hashSign = hash < 0 ? "-" : string.Empty;
            var absHash = this.hash >= 0 ? this.hash : -this.hash;
            return $"addr {this.AbsoluteAddress}{addrRC}, currAddr {this.AbsoluteCurrentAddress}{currAddrRC}{isNotCurr}, hash {hashSign}{absHash}, tag {this.tag}, slot {this.slot}";
        }
    }

    public unsafe partial class FasterKV<Key, Value> : FasterBase, IFasterKV<Key, Value>
    {
        // Wrappers to call and populate.
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal unsafe bool FindTag(ref HashEntryInfo hei)
        {
            hei.bucket = default;
            hei.slot = default;
            hei.entry = default;
            return FindTag(hei.hash, hei.tag, ref hei.bucket, ref hei.slot, ref hei.entry);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal unsafe void FindOrCreateTag(ref HashEntryInfo hei)
        {
            hei.bucket = default;
            hei.slot = default;
            hei.entry = default;
            FindOrCreateTag(hei.hash, hei.tag, ref hei.bucket, ref hei.slot, ref hei.entry, hlog.BeginAddress);
        }
    }
}
