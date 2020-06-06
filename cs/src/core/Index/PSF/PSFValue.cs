// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;

namespace FASTER.core
{
    /// <summary>
    /// The TKVValue in the secondary, PSF-implementing FasterKV instances; it wraps the <typeparamref name="TRecordId"/>
    /// and stores the links in the TPSFKey chains.
    /// </summary>
    /// <typeparam name="TRecordId">The type of the provider's record identifier</typeparam>
    public unsafe struct PSFValue<TRecordId>
        where TRecordId : struct
    {
        /// <summary>
        /// LogicalAddress for FasterKV and FasterLog; something else for another data provider.
        /// </summary>
        public TRecordId RecordId;

        internal void CopyTo(ref PSFValue<TRecordId> other, int recordIdSize, int psfCount)
        {
            other.RecordId = this.RecordId;
            var thisChainPointer = ((byte*)Unsafe.AsPointer(ref this) + recordIdSize);
            var otherChainPointer = ((byte*)Unsafe.AsPointer(ref other) + recordIdSize);
            var len = sizeof(long) * psfCount;   // The chains links are "long logicalAddress".
            Buffer.MemoryCopy(thisChainPointer, otherChainPointer, len, len);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal byte* GetRecordIdPtr()
            => (byte*)Unsafe.AsPointer(ref this);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal long* GetChainLinkPtrs(int recordIdSize)
            => (long*)((byte*)Unsafe.AsPointer(ref this) + recordIdSize);

        /// <inheritdoc/>
        public override string ToString() => $"RecordId = {this.RecordId}";

        internal class VarLenLength : IVariableLengthStruct<PSFValue<TRecordId>>
        {
            private readonly int size;

            internal VarLenLength(int recordIdSize, int psfCount) => this.size = recordIdSize + sizeof(long) * psfCount;

            public int GetAverageLength() => this.size;

            public int GetInitialLength<Input>(ref Input _) => this.size;

            public int GetLength(ref PSFValue<TRecordId> _) => this.size;
        }

        internal class Accessor : IPSFValueAccessor<PSFValue<TRecordId>>
        {
            internal Accessor(int psfCount, int recordIdSize)
            {
                this.PSFCount = psfCount;
                this.RecordIdSize = recordIdSize;
            }

            public int PSFCount { get; }

            public int RecordIdSize { get; }

            public long* GetChainLinkPtrs(ref PSFValue<TRecordId> value)
                => value.GetChainLinkPtrs(this.RecordIdSize);

            public unsafe void SetRecordId<TRecId>(ref PSFValue<TRecordId> value, TRecId recordId)
            {
                var recIdPtr = value.GetRecordIdPtr();
                Buffer.MemoryCopy(Unsafe.AsPointer(ref recordId), recIdPtr, this.RecordIdSize, this.RecordIdSize);
            }
        }
    }
}
