// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;
using System.Text;

namespace FASTER.core
{
    /// <summary>
    /// Provides access to the <see cref="CompositeKey{TPSFKey}"/> internals that are hidden behind
    /// the Key typeparam of the secondary FasterKV.
    /// </summary>
    /// <typeparam name="TPSFKey">The type of the Key returned by a PSF function</typeparam>
    internal unsafe class KeyAccessor<TPSFKey>
        where TPSFKey : new()
    {
        private readonly IFasterEqualityComparer<TPSFKey> userComparer;

        internal KeyAccessor(IFasterEqualityComparer<TPSFKey> userComparer, int keyCount, int keyPointerSize)
        {
            this.userComparer = userComparer;
            this.KeyCount = keyCount;
            this.KeyPointerSize = keyPointerSize;
        }

        public int KeyCount { get; }

        public int KeyPointerSize { get; }

        #region KeyPointer accessors
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetPreviousAddress(long physicalAddress)
            => this.GetKeyPointerRef(physicalAddress).PreviousAddress;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SetPreviousAddress(ref CompositeKey<TPSFKey> key, int psfOrdinal, long prevAddress)
            => this.GetKeyPointerRef(ref key, psfOrdinal).PreviousAddress = prevAddress;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SetOffsetToStartOfKeys(ref CompositeKey<TPSFKey> key, int psfOrdinal, int offset)
            => this.GetKeyPointerRef(ref key, psfOrdinal).OffsetToStartOfKeys = offset;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool IsNullAt(ref CompositeKey<TPSFKey> key, int psfOrdinal) => this.GetKeyPointerRef(ref key, psfOrdinal).IsNull;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool IsUnlinkOldAt(ref CompositeKey<TPSFKey> key, int psfOrdinal) => this.GetKeyPointerRef(ref key, psfOrdinal).IsUnlinkOld;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool IsLinkNewAt(ref CompositeKey<TPSFKey> key, int psfOrdinal) => this.GetKeyPointerRef(ref key, psfOrdinal).IsLinkNew;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetHashCode64(ref KeyPointer<TPSFKey> keyPointer)
            => Utility.GetHashCode(this.userComparer.GetHashCode64(ref keyPointer.Key)) ^ Utility.GetHashCode(keyPointer.PsfOrdinal + 1);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetHashCode64(ref CompositeKey<TPSFKey> key, int psfOrdinal)
        {
            ref KeyPointer<TPSFKey> keyPointer = ref key.GetKeyPointerRef(psfOrdinal, this.KeyPointerSize);
            return Utility.GetHashCode(this.userComparer.GetHashCode64(ref keyPointer.Key)) ^ Utility.GetHashCode(keyPointer.PsfOrdinal + 1);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool EqualsAtKeyAddress(ref KeyPointer<TPSFKey> queryKeyPointer, long physicalAddress)
            => KeysEqual(ref queryKeyPointer, ref GetKeyPointerRef(physicalAddress));

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool EqualsAtRecordAddress(ref KeyPointer<TPSFKey> queryKeyPointer, long physicalAddress)
            => KeysEqual(ref queryKeyPointer, ref GetKeyPointerRef(physicalAddress, queryKeyPointer.PsfOrdinal));

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool KeysEqual(ref KeyPointer<TPSFKey> queryKeyPointer, ref KeyPointer<TPSFKey> storedKeyPointer)
            => queryKeyPointer.PsfOrdinal == storedKeyPointer.PsfOrdinal && this.userComparer.Equals(ref queryKeyPointer.Key, ref storedKeyPointer.Key);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal ref KeyPointer<TPSFKey> GetKeyPointerRef(ref CompositeKey<TPSFKey> key, int psfOrdinal)
            => ref key.GetKeyPointerRef(psfOrdinal, this.KeyPointerSize);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal unsafe ref KeyPointer<TPSFKey> GetKeyPointerRef(long physicalAddress)
            => ref Unsafe.AsRef<KeyPointer<TPSFKey>>((byte*)physicalAddress);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal unsafe ref KeyPointer<TPSFKey> GetKeyPointerRef(long physicalAddress, int psfOrdinal)
            => ref Unsafe.AsRef<KeyPointer<TPSFKey>>((byte*)GetKeyAddressFromRecordPhysicalAddress(physicalAddress, psfOrdinal));
        #endregion KeyPointer accessors

        #region Address manipulation
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetRecordAddressFromKeyPhysicalAddress(long physicalAddress)
            => physicalAddress - this.GetKeyPointerRef(physicalAddress).OffsetToStartOfKeys - RecordInfo.GetLength();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetKeyAddressFromRecordPhysicalAddress(long physicalAddress, int psfOrdinal)
            // TODOperf: if we omit IsNull keys, then this will have to walk to the key with psfOrdinal. Fortunately it is only 
            // called during AsyncGetFromDiskCallback.
            => physicalAddress + RecordInfo.GetLength() + psfOrdinal * this.KeyPointerSize;
        #endregion Address manipulation

#if DEBUG
        public string GetString(ref CompositeKey<TPSFKey> compositeKey, int psfOrdinal = -1)
        {
            if (psfOrdinal == -1)
            {
                var sb = new StringBuilder("{");
                for (var ii = 0; ii < this.KeyCount; ++ii)
                {
                    if (ii > 0)
                        sb.Append(", ");
                    ref KeyPointer<TPSFKey> keyPointer = ref this.GetKeyPointerRef(ref compositeKey, ii);
                    sb.Append(keyPointer.IsNull ? "null" : keyPointer.Key.ToString());
                }
                sb.Append("}");
                return sb.ToString();
            }
            return this.GetString(ref this.GetKeyPointerRef(ref compositeKey, psfOrdinal));
        }

        public string GetString(ref KeyPointer<TPSFKey> keyPointer)
            => $"{{{(keyPointer.IsNull ? "null" : keyPointer.Key.ToString())}}}";
#endif
    }
}