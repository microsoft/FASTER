// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;

namespace FASTER.core
{
    /// <summary>
    /// Identifies which log regions records will be copied from to <see cref="ReadCopyTo"/>. This specification is
    /// evaluated in hierarchical order, from that on the FasterKV ctor, which may be overridden by those in 
    /// <see cref="ClientSession{Key, Value, Input, Output, Context, Functions}"/>.NewSession(), which may be overridden
    /// by those at the individual Read() level.
    /// </summary>
    public enum ReadCopyFrom : byte
    {
        /// <summary>'default' value; inherit settings from the previous hierarchy level(s).</summary>
        Inherit = 0,

        /// <summary>Do not copy.</summary>
        None,

        /// <summary>From larger-than-memory device (e.g. disk storage).</summary>
        Device,

        /// <summary>From <see cref="Device"/> or from the immutable region of the log.</summary>
        AllImmutable
    }

    /// <summary>
    /// Identifies the destination of records copied from <see cref="ReadCopyFrom"/>.
    /// </summary>
    public enum ReadCopyTo : byte
    {
        /// <summary>'default' value; inherit settings from the previous hierarchy level(s).</summary>
        Inherit = 0,

        /// <summary>Do not copy.</summary>
        None,

        /// <summary>Copy to the tail of the main log (or splice into the readcache/mainlog boundary, if readcache records are present).</summary>
        MainLog,

        /// <summary>Copy to the readcache. This requires that <see cref="ReadCacheSettings"/> be supplied to the FasterKV ctor.</summary>
        ReadCache
    }

    /// <summary>
    /// Options for automatically copying immutable records on Read().
    /// </summary>
    public struct ReadCopyOptions
    {
        /// <summary>Which immutable regions to copy records from.</summary>
        public ReadCopyFrom CopyFrom;

        /// <summary>The destination for copies records.</summary>
        public ReadCopyTo CopyTo;

        internal bool IsActive => CopyFrom != ReadCopyFrom.None && CopyTo != ReadCopyTo.None;

        /// <summary>Constructor.</summary>
        public ReadCopyOptions(ReadCopyFrom from, ReadCopyTo to)
        {
            this.CopyFrom = from;
            this.CopyTo = to;
        }

        internal ReadCopyOptions Merge(ReadCopyOptions other) => this = Merge(this, other);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static ReadCopyOptions Merge(ReadCopyOptions upper, ReadCopyOptions lower) 
            => new(lower.CopyFrom == ReadCopyFrom.Inherit ? upper.CopyFrom : lower.CopyFrom,
                   lower.CopyTo == ReadCopyTo.Inherit ? upper.CopyTo : lower.CopyTo);

        /// <summary>A default instance that does no copying.</summary>
        public static ReadCopyOptions None => new() { CopyFrom = ReadCopyFrom.None, CopyTo = ReadCopyTo.None };

        /// <inheritdoc/>
        public override string ToString() => $"from: {CopyFrom}, to {CopyTo}";
    }

    /// <summary>
    /// Options for the Read() operation
    /// </summary>
    public struct ReadOptions
    {
        /// <summary>
        /// The address to start the read search at; if this is Constants.kInvalidAddress, the search starts with the key, as in other forms of Read.
        /// For ReadAtAddress it is the address to read at.
        /// Can be populated from <see cref="RecordMetadata.RecordInfo"/>.PreviousAddress for chained reads.
        /// </summary>
        public long StartAddress { get; internal set; }

        /// <summary>
        /// The minimum address to search at (records are searched toward the head--that is, in decreasing address order);
        /// if this is Constants.kInvalidAddress, the search continues until no further records are found, as in other forms of Read.
        /// </summary>
        public long StopAddress { get; internal set; }

        /// <summary>
        /// Options for automatically copying immutable records on Read().
        /// </summary>
        public ReadCopyOptions CopyOptions { get; internal set; }

        /// <summary>
        /// The hashcode of the key for this operation
        /// </summary>
        public long? KeyHash { get; internal set; }

        /// <inheritdoc/>
        public readonly override string ToString() => $"startAddr: {StartAddress}, stopAddr {StopAddress}, copy {{{CopyOptions}}}, keyHash {Utility.GetHashString(KeyHash)}";
    }

    /// <summary>
    /// Options for the Read() operation
    /// </summary>
    public struct RMWOptions
    {
        /// <summary>
        /// The hashcode of the key for this operation
        /// </summary>
        public long? KeyHash { get; internal set; }

        /// <inheritdoc/>
        public readonly override string ToString() => $"keyHash {Utility.GetHashString(KeyHash)}";
    }

    /// <summary>
    /// Options for the Read() operation
    /// </summary>
    public struct UpsertOptions
    {
        /// <summary>
        /// The hashcode of the key for this operation
        /// </summary>
        public long? KeyHash { get; internal set; }

        /// <inheritdoc/>
        public readonly override string ToString() => $"keyHash {Utility.GetHashString(KeyHash)}";
    }

    /// <summary>
    /// Options for the Read() operation
    /// </summary>
    public struct DeleteOptions
    {
        /// <summary>
        /// The hashcode of the key for this operation
        /// </summary>
        public long? KeyHash { get; internal set; }

        /// <inheritdoc/>
        public readonly override string ToString() => $"keyHash {Utility.GetHashString(KeyHash)}";
    }
}
