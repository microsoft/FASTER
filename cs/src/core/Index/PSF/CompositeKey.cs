// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

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
        /// <param name="keyPointerSize">Size of the <see cref="KeyPointer{TPSFKey}"/> struct</param>
        /// <returns>A reference to the key for the PSF identified by psfOrdinal.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal ref TPSFKey GetKeyRef(int psfOrdinal, int keyPointerSize)
            => ref GetKeyPointerRef(psfOrdinal, keyPointerSize).Key;

        /// <summary>
        /// Returns a reference to the CompositeKey from a reference to the first <see cref="KeyPointer{TPSFKey}"/>
        /// </summary>
        /// <param name="firstKeyPointerRef">A reference to the first <see cref="KeyPointer{TPSFKey}"/>, typed as TPSFKey</param>
        /// <remarks>Used when converting the CompositeKey to/from the TPSFKey type for secondary FKV operations</remarks>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static ref CompositeKey<TPSFKey> CastFromFirstKeyPointerRefAsKeyRef(ref TPSFKey firstKeyPointerRef)
            => ref Unsafe.AsRef<CompositeKey<TPSFKey>>((byte*)Unsafe.AsPointer(ref firstKeyPointerRef));

        /// <summary>
        /// Converts this CompositeKey reference to a reference to the first <see cref="KeyPointer{TPSFKey}"/>, typed as TPSFKey.
        /// </summary>
        /// <remarks>Used when converting the CompositeKey to/from the TPSFKey type for secondary FKV operations</remarks>
        /// <returns>A reference to the first <see cref="KeyPointer{TPSFKey}"/>, typed as TPSFKey</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal ref TPSFKey CastToFirstKeyPointerRefAsKeyRef()
            => ref Unsafe.AsRef<TPSFKey>((byte*)Unsafe.AsPointer(ref this));

        internal class VarLenLength : IVariableLengthStruct<TPSFKey>
        {
            private readonly int size;

            internal VarLenLength(int keyPointerSize, int psfCount) => this.size = keyPointerSize * psfCount;

            public int GetInitialLength() => this.size;

            public int GetLength(ref TPSFKey _) => this.size;
        }

        /// <summary>
        /// This is the unused key comparer passed to the secondary FasterKV
        /// </summary>
        internal class UnusedKeyComparer : IFasterEqualityComparer<TPSFKey>
        {
            public long GetHashCode64(ref TPSFKey cKey)
                => throw new PSFInternalErrorException("Must use KeyAccessor instead (psfOrdinal is required)");

            public bool Equals(ref TPSFKey cKey1, ref TPSFKey cKey2)
                => throw new PSFInternalErrorException("Must use KeyAccessor instead (psfOrdinal is required)");
        }
    }
}
