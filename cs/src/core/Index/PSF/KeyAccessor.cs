// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;
using System.Text;

namespace FASTER.core
{
    /// <summary>
    /// Internal interface to bridge the generic definition for composite key type.
    /// </summary>
    /// <typeparam name="TCompositeKey"></typeparam>
    internal interface IKeyAccessor<TCompositeKey>
    {
        int KeyCount { get; }

        int KeyPointerSize { get; }

        long GetPrevAddress(long physicalAddress);

        void SetPrevAddress(ref TCompositeKey key, int psfOrdinal, long prevAddress);

        long GetRecordAddressFromKeyPhysicalAddress(long physicalAddress);

        long GetRecordAddressFromKeyLogicalAddress(long logicalAddress, int psfOrdinal);

        long GetKeyAddressFromRecordPhysicalAddress(long physicalAddress, int psfOrdinal);

        long GetHashCode64(ref TCompositeKey key, int psfOrdinal, bool isQuery);

        bool EqualsAtKeyAddress(ref TCompositeKey queryKey, long physicalAddress);

        bool EqualsAtRecordAddress(ref TCompositeKey queryKey, long physicalAddress);

        string GetString(ref TCompositeKey key, int psfOrdinal = -1);
    }

    /// <summary>
    /// Provides access to the <see cref="CompositeKey{TPSFKey}"/> internals that are hidden behind
    /// the Key typeparam of the secondary FasterKV.
    /// </summary>
    /// <typeparam name="TPSFKey">The type of the Key returned by a PSF function</typeparam>
    internal unsafe class KeyAccessor<TPSFKey> : IKeyAccessor<CompositeKey<TPSFKey>>
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

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetPrevAddress(long physicalAddress) 
            => GetKeyPointerRef(physicalAddress).PrevAddress;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void SetPrevAddress(ref CompositeKey<TPSFKey> key, int psfOrdinal, long prevAddress)
            => this.GetKeyPointerRef(ref key, psfOrdinal).PrevAddress = prevAddress;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetRecordAddressFromKeyPhysicalAddress(long physicalAddress)
            => physicalAddress - this.GetKeyPointerRef(physicalAddress).PsfOrdinal * this.KeyPointerSize - RecordInfo.GetLength(); // TODO: Assumes all PSFs are present

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetRecordAddressFromKeyLogicalAddress(long logicalAddress, int psfOrdinal)
            => logicalAddress - psfOrdinal * this.KeyPointerSize - RecordInfo.GetLength(); // TODO: Assumes all PSFs are present

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetKeyAddressFromRecordPhysicalAddress(long physicalAddress, int psfOrdinal)
            => physicalAddress + RecordInfo.GetLength() + psfOrdinal * this.KeyPointerSize; // TODO: Assumes all PSFs are present

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long GetHashCode64(ref CompositeKey<TPSFKey> key, int psfOrdinal, bool isQuery)
        {
            ref KeyPointer<TPSFKey> keyPointer = ref key.GetKeyPointerRef(isQuery ? 0 : psfOrdinal, this.KeyPointerSize);
            return Utility.GetHashCode(this.userComparer.GetHashCode64(ref keyPointer.Key)) ^ Utility.GetHashCode(keyPointer.PsfOrdinal + 1);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool EqualsAtKeyAddress(ref CompositeKey<TPSFKey> queryKey, long physicalAddress)
        {
            // The query key only has a single key in it--the one we're trying to match.
            ref KeyPointer<TPSFKey> queryKeyPointer = ref queryKey.GetKeyPointerRef(0, this.KeyPointerSize);
            ref KeyPointer<TPSFKey> storedKeyPointer = ref GetKeyPointerRef(physicalAddress);
            return KeysEqual(ref queryKeyPointer, ref storedKeyPointer);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool EqualsAtRecordAddress(ref CompositeKey<TPSFKey> queryKey, long physicalAddress)
        {
            // The query key only has a single key in it--the one we're trying to match.
            ref KeyPointer<TPSFKey> queryKeyPointer = ref queryKey.GetKeyPointerRef(0, this.KeyPointerSize);
            ref KeyPointer<TPSFKey> storedKeyPointer = ref GetKeyPointerRef(physicalAddress, queryKeyPointer.PsfOrdinal);
            return KeysEqual(ref queryKeyPointer, ref storedKeyPointer);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool KeysEqual(ref KeyPointer<TPSFKey> queryKeyPointer, ref KeyPointer<TPSFKey> storedKeyPointer)
        {
            return queryKeyPointer.PsfOrdinal == storedKeyPointer.PsfOrdinal &&
                    this.userComparer.Equals(ref queryKeyPointer.Key, ref storedKeyPointer.Key);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal ref KeyPointer<TPSFKey> GetKeyPointerRef(ref CompositeKey<TPSFKey> key, int psfOrdinal)
            => ref key.GetKeyPointerRef(psfOrdinal, this.KeyPointerSize);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal unsafe ref KeyPointer<TPSFKey> GetKeyPointerRef(long physicalAddress)
            => ref Unsafe.AsRef<KeyPointer<TPSFKey>>((byte*)physicalAddress);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal unsafe ref KeyPointer<TPSFKey> GetKeyPointerRef(long physicalAddress, int psfOrdinal)
            => ref Unsafe.AsRef<KeyPointer<TPSFKey>>((byte*)GetKeyAddressFromRecordPhysicalAddress(physicalAddress, psfOrdinal));

        public string GetString(ref CompositeKey<TPSFKey> key, int psfOrdinal = -1)
        {
            var sb = new StringBuilder("{");

            if (psfOrdinal == -1)
            {
                for (var ii = 0; ii < this.KeyCount; ++ii)
                {
                    if (ii > 0)
                        sb.Append(", ");
                    ref KeyPointer<TPSFKey> keyPointer = ref this.GetKeyPointerRef(ref key, ii);
                    sb.Append(keyPointer.IsNull ? "null" : keyPointer.Key.ToString());
                }
            }
            else
            {
                ref KeyPointer<TPSFKey> keyPointer = ref this.GetKeyPointerRef(ref key, psfOrdinal);
                sb.Append(keyPointer.IsNull ? "null" : keyPointer.Key.ToString());
            }
            sb.Append("}");
            return sb.ToString();
        }
    }
}
