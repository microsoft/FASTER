// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

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
        internal ushort PsfOrdinal;

        /// <summary>
        /// Flags regarding the PSF.
        /// </summary>
        internal ushort Flags;

        /// <summary>
        /// The Key returned by the <see cref="PSF{TPSFKey, TRecordId}"/> execution.
        /// </summary>
        internal TPSFKey Key;

        private const ushort NullFlag = 0x0001;

        internal bool IsNull
        {
            get => (this.Flags & NullFlag) == NullFlag;
            set => this.Flags = (ushort)(value ? (this.Flags | NullFlag) : (this.Flags & ~NullFlag));
        }
    }
}
