// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;

namespace FASTER.core
{
    /// <summary>
    /// Wraps the set of TPSFKeys for a record in the secondary FasterKV instance.
    /// </summary>
    /// <typeparam name="TPSFKey"></typeparam>
    public unsafe struct CompositeKey<TPSFKey>
        where TPSFKey : new()
    {
        // This class is essentially a "reinterpret_cast<KeyPointer<TPSFKey>*>" implementation; there are no data members.

        /// <summary>
        /// Get a reference to the key for the PSF identified by psfOrdinal.
        /// </summary>
        /// <param name="psfOrdinal">The ordinal of the PSF in its parent PSFGroup</param>
        /// <param name="keyPointerSize">Size of the KeyPointer{TPSFKey} struct</param>
        /// <returns>A reference to the key for the PSF identified by psfOrdinal.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal ref KeyPointer<TPSFKey> GetKeyPointerRef(int psfOrdinal, int keyPointerSize) 
            => ref Unsafe.AsRef<KeyPointer<TPSFKey>>((byte*)Unsafe.AsPointer(ref this) + keyPointerSize * psfOrdinal);

        /// <summary>
        /// Get a reference to the key for the PSF identified by psfOrdinal.
        /// </summary>
        /// <param name="psfOrdinal">The ordinal of the PSF in its parent PSFGroup</param>
        /// <param name="keyPointerSize">Size of the KeyPointer{TPSFKey} struct</param>
        /// <returns>A reference to the key for the PSF identified by psfOrdinal.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal ref TPSFKey GetKeyRef(int psfOrdinal, int keyPointerSize)
            => ref GetKeyPointerRef(psfOrdinal, keyPointerSize).Key;

        internal class VarLenLength : IVariableLengthStruct<CompositeKey<TPSFKey>>
        {
            private readonly int size;

            internal VarLenLength(int keyPointerSize, int psfCount) => this.size = keyPointerSize * psfCount;

            public int GetInitialLength() => this.size;

            public int GetLength(ref CompositeKey<TPSFKey> _) => this.size;
        }

        /// <summary>
        /// This is the unused key comparer passed to the secondary FasterKV
        /// </summary>
        internal class UnusedKeyComparer : IFasterEqualityComparer<CompositeKey<TPSFKey>>
        {
            public long GetHashCode64(ref CompositeKey<TPSFKey> cKey)
                => throw new PSFInternalErrorException("Must use KeyAccessor instead");

            public bool Equals(ref CompositeKey<TPSFKey> cKey1, ref CompositeKey<TPSFKey> cKey2)
                => throw new PSFInternalErrorException("Must use KeyAccessor instead");
        }
    }
}
