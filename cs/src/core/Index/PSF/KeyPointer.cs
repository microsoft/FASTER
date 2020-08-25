// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;

namespace FASTER.core
{
    internal struct KeyPointer<TPSFKey>
    {
        #region Fields
        /// <summary>
        /// The previous address in the hash chain. May be for a different PsfOrdinal than this one due to hash collisions.
        /// </summary>
        internal long PreviousAddress;

        /// <summary>
        /// The offset to the start of the record
        /// </summary>
        private ushort offsetToStartOfKeys;

        /// <summary>
        /// The ordinal of the current <see cref="PSF{TPSFKey, TRecordId}"/>.
        /// </summary>
        private byte psfOrdinal;           // Note: 'byte' is consistent with Constants.kInvalidPsfOrdinal

        /// <summary>
        /// Flags regarding the PSF.
        /// </summary>
        private byte flags;

        /// <summary>
        /// The Key returned by the <see cref="PSF{TPSFKey, TRecordId}"/> execution.
        /// </summary>
        internal TPSFKey Key;               // TODOperf: for Key size > 4, reinterpret this an offset to the actual value (after the KeyPointer list)
        #endregion Fields

        internal void Initialize(int psfOrdinal, ref TPSFKey key)
        {
            this.PreviousAddress = Constants.kInvalidAddress;
            this.offsetToStartOfKeys = 0;
            this.PsfOrdinal = psfOrdinal;
            this.flags = 0;
            this.Key = key;
        }

        #region Accessors
        // For Insert, this identifies a null PSF result (the record does not match the PSF and is not included
        // in any TPSFKey chain for it). Also used in PSFChangeTracker to determine whether to set kUnlinkOldBit.
        private const byte kIsNullBit = 0x01;

        // If Key size is > 4, then reinterpret the Key as an offset to the actual key. (TODOperf not implemented)
        private const byte kIsOutOfLineKeyBit = 0x02;

        // For Update, the TPSFKey has changed; remove this record from the previous TPSFKey chain.
        private const byte kUnlinkOldBit = 0x04;

        // For Update and insert, the TPSFKey has changed; add this record to the new TPSFKey chain.
        private const byte kLinkNewBit = 0x08;

        internal bool IsNull
        {
            get => (this.flags & kIsNullBit) != 0;
            set => this.flags = value ? (byte)(this.flags | kIsNullBit) : (byte)(this.flags & ~kIsNullBit);
        }

        internal bool IsUnlinkOld
        {
            get => (this.flags & kUnlinkOldBit) != 0;
            set => this.flags = value ? (byte)(this.flags | kUnlinkOldBit) : (byte)(this.flags & ~kUnlinkOldBit);
        }

        internal bool IsLinkNew
        {
            get => (this.flags & kLinkNewBit) != 0;
            set => this.flags = value ? (byte)(this.flags | kLinkNewBit) : (byte)(this.flags & ~kLinkNewBit);
        }

        internal bool IsOutOfLineKey
        {
            get => (this.flags & kIsOutOfLineKeyBit) != 0;
            set => this.flags = value ? (byte)(this.flags | kIsOutOfLineKeyBit) : (byte)(this.flags & ~kIsOutOfLineKeyBit);
        }

        internal bool HasChanges => (this.flags & (kUnlinkOldBit | kLinkNewBit)) != 0;

        internal int PsfOrdinal
        {
            get => this.psfOrdinal;
            set => this.psfOrdinal = (byte)value;
        }

        internal int OffsetToStartOfKeys
        {
            get => this.offsetToStartOfKeys;
            set => this.offsetToStartOfKeys = (ushort)value;
        }
        #endregion Accessors

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void ClearUpdateFlags() => this.flags = (byte)(this.flags & ~(kUnlinkOldBit | kLinkNewBit));

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal unsafe static ref KeyPointer<TPSFKey> CastFromKeyRef(ref TPSFKey keyRef)
            => ref Unsafe.AsRef<KeyPointer<TPSFKey>>((byte*)Unsafe.AsPointer(ref keyRef));
    }
}
