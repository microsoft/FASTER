// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;

namespace FASTER.core
{
    internal struct KeyPointer<TPSFKey>
    {
        /// <summary>
        /// The previous address in the hash chain. May be for a different PsfOrdinal than this one due to hash collisions.
        /// </summary>
        internal long PrevAddress;

        /// <summary>
        /// The ordinal of the current <see cref="PSF{TPSFKey, TRecordId}"/>.
        /// </summary>
        internal byte PsfOrdinal;   // Note: consistent with Constants.kInvalidPsfOrdinal

        /// <summary>
        /// Flags regarding the PSF.
        /// </summary>
        internal byte Flags;        // TODO: Change to PSFResultFlags

        internal ushort Reserved;   // TODOperf: Can be used for offset to start of key list, especially if we allow variable # of keys

        /// <summary>
        /// The Key returned by the <see cref="PSF{TPSFKey, TRecordId}"/> execution.
        /// </summary>
        internal TPSFKey Key;       // TODOperf: for Key size > 4, reinterpret this an offset to the actual value (after the KeyPointer list)

        private const ushort NullFlag = 0x0001;

        internal bool IsNull
        {
            get => (this.Flags & NullFlag) == NullFlag;
            set => this.Flags = (byte)(value ? (this.Flags | NullFlag) : (this.Flags & ~NullFlag));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal unsafe static ref KeyPointer<TPSFKey> CastFromKeyRef(ref TPSFKey keyRef)
            => ref Unsafe.AsRef<KeyPointer<TPSFKey>>((byte*)Unsafe.AsPointer(ref keyRef));
    }
}
