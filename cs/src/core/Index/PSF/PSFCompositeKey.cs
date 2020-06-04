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
    public unsafe struct PSFCompositeKey<TPSFKey>
        where TPSFKey : struct
    {
        // This class is entirely a "reinterpret_cast<TPSFKey*>" implementation; there are no data members.
        internal void CopyTo(ref PSFCompositeKey<TPSFKey> other, int keySize, int chainHeight)
        {
            var thisKeysPointer = (byte*)Unsafe.AsPointer(ref this);
            var otherKeysPointer = (byte*)Unsafe.AsPointer(ref other);
            var len = keySize * chainHeight;
            Buffer.MemoryCopy(thisKeysPointer, otherKeysPointer, len, len);
        }

        /// <summary>
        /// Get a reference to the key for the PSF identified by psfOrdinal.
        /// </summary>
        /// <param name="psfOrdinal">The ordinal of the PSF in its parent PSFGroup</param>
        /// <param name="keySize">Size of the PSFKey struct</param>
        /// <returns>A reference to the key for the PSF identified by psfOrdinal.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ref TPSFKey GetKeyRef(int psfOrdinal, int keySize) 
            => ref Unsafe.AsRef<TPSFKey>((byte*)Unsafe.AsPointer(ref this) + keySize * psfOrdinal);

        /// <summary>
        /// Equality comparison
        /// </summary>
        /// <param name="comparer">The key comparer to use</param>
        /// <param name="psfOrdinal">The ordinal of the PSF in its parent PSFGroup</param>
        /// <param name="keySize">Size of the PSFKey struct</param>
        /// <param name="otherKey">The key to compare to</param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool Equals(IFasterEqualityComparer<TPSFKey> comparer, int psfOrdinal, int keySize, ref TPSFKey otherKey) 
            => comparer.Equals(ref GetKeyRef(psfOrdinal, keySize), ref otherKey);

        internal class VarLenLength : IVariableLengthStruct<PSFCompositeKey<TPSFKey>>
        {
            private readonly int size;

            internal VarLenLength(int keySize, int chainHeight) => this.size = keySize * chainHeight;

            public int GetAverageLength() => this.size;

            public int GetInitialLength<Input>(ref Input _) => this.size;

            public int GetLength(ref PSFCompositeKey<TPSFKey> _) => this.size;
        }

        internal class UnusedKeyComparer : IFasterEqualityComparer<PSFCompositeKey<TPSFKey>>
        {
            public long GetHashCode64(ref PSFCompositeKey<TPSFKey> cKey)
                => throw new InvalidOperationException("Must use the overload with PSF ordinal");

            public bool Equals(ref PSFCompositeKey<TPSFKey> cKey1, ref PSFCompositeKey<TPSFKey> cKey2)
                => throw new InvalidOperationException("Must use the overload with PSF ordinal");
        }
        internal unsafe struct PtrWrapper : IDisposable
        {
            private readonly SectorAlignedMemory mem;
            private readonly int size;

            internal PtrWrapper(int size, SectorAlignedBufferPool pool)
            {
                this.size = size;
                this.mem = pool.Get(size);
            }

            internal void Set(ref TPSFKey key) 
                => Buffer.MemoryCopy(Unsafe.AsPointer(ref key), mem.GetValidPointer(), this.size, this.size);

            internal ref PSFCompositeKey<TPSFKey> GetRef() 
                => ref Unsafe.AsRef<PSFCompositeKey<TPSFKey>>(mem.GetValidPointer());

            public void Dispose() => mem.Return();
        }
    }

    internal interface ICompositeKeyComparer<TCompositeKey>
    {
        int ChainHeight { get; }

        long GetHashCode64(int psfOrdinal, ref TCompositeKey cKey);

        bool Equals(bool isQuery, int psfOrdinal, ref TCompositeKey queryKey, ref TCompositeKey storedKey);
    }

    internal class PSFCompositeKeyComparer<TPSFKey> : ICompositeKeyComparer<PSFCompositeKey<TPSFKey>>
        where TPSFKey : struct
    {
        private readonly IFasterEqualityComparer<TPSFKey> userComparer;
        private readonly int keySize;
        public int ChainHeight { get; }

        internal PSFCompositeKeyComparer(IFasterEqualityComparer<TPSFKey> ucmp, int ksize, int chainHeight)
        {
            this.userComparer = ucmp;
            this.keySize = ksize;
            this.ChainHeight = chainHeight;
        }

        public long GetHashCode64(int psfOrdinal, ref PSFCompositeKey<TPSFKey> cKey)
            => this.userComparer.GetHashCode64(ref cKey.GetKeyRef(psfOrdinal, this.keySize));

        public bool Equals(bool isQuery, int psfOrdinal, ref PSFCompositeKey<TPSFKey> queryKey, ref PSFCompositeKey<TPSFKey> storedKey)
            => userComparer.Equals(
                // For a query, the composite key consists of only one key, the query key, at ordinal 0.
                ref queryKey.GetKeyRef(isQuery ? 0 : psfOrdinal, this.keySize),
                ref storedKey.GetKeyRef(psfOrdinal, this.keySize));
    }
}
