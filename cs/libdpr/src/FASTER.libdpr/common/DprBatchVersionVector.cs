using System;
using System.Collections;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace FASTER.libdpr
{
    /// <summary>
    ///     on-wire format for a vector of version numbers. Does not own or allocate underlying memory.
    /// </summary>
    public unsafe ref struct DprBatchVersionVector
    {
        private readonly ReadOnlySpan<byte> responseHead;

        /// <summary>
        ///     Construct a new VersionVector to be backed by the given byte*
        /// </summary>
        /// <param name="vectorHead"> Reference to the response bytes</param>
        public DprBatchVersionVector(ReadOnlySpan<byte> responseHead)
        {
            this.responseHead = responseHead;
            if (responseHead.IsEmpty)
                Count = 0;
            else
            {
                fixed (byte* h = responseHead)
                {
                    var l = (long*) (h + Unsafe.AsRef<DprBatchHeader>(h).VersionVectorOffset);
                    Count = (int) l[0];
                }
            }
        }

        public int Count { get; }

        public long this[int index]
        {
            get
            {
                fixed (byte* h = responseHead)
                {
                    var l = (long*) (h + Unsafe.AsRef<DprBatchHeader>(h).VersionVectorOffset);
                    return l[index + 1];
                }
            }
        }
    }
}