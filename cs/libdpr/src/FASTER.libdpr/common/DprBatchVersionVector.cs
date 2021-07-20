using System;
using System.Collections;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace FASTER.libdpr
{
    /// <summary>
    ///     on-wire format for a vector of version numbers. Does not own or allocate underlying memory.
    /// </summary>
    public unsafe struct DprBatchVersionVector : IReadOnlyList<long>
    {
        private readonly ReadOnlyMemory<byte> responseHead;

        /// <summary>
        ///     Construct a new VersionVector to be backed by the given byte*
        /// </summary>
        /// <param name="vectorHead"> Reference to the response bytes</param>
        public DprBatchVersionVector(ReadOnlyMemory<byte> responseHead)
        {
            this.responseHead = responseHead;
            fixed (byte* h = responseHead.Span)
            {
                fixed (byte* b = Unsafe.AsRef<DprBatchResponseHeader>(h).versions)
                {
                    var l = (long*) b;
                    Count = (int) l[0];
                }
            }
        }

        private class DprBatchVersionVectorEnumerator : IEnumerator<long>
        {
            private readonly ReadOnlyMemory<byte> start;
            private long index = -1;

            public DprBatchVersionVectorEnumerator(ReadOnlyMemory<byte> start)
            {
                this.start = start;
            }

            public bool MoveNext()
            {
                fixed (byte* h = start.Span)
                {
                    fixed (byte* b = Unsafe.AsRef<DprBatchResponseHeader>(h).versions)
                    {
                        var l = (long*) b;
                        return ++index < l[0];
                    }
                }
            }

            public void Reset()
            {
                index = 0;
            }

            public long Current
            {
                get
                {
                    fixed (byte* h = start.Span)
                    {
                        fixed (byte* b = Unsafe.AsRef<DprBatchResponseHeader>(h).versions)
                        {
                            var l = (long*) b;
                            return l[index + 1];
                        }
                    }
                }
            }

            object IEnumerator.Current => Current;

            public void Dispose()
            {
            }
        }

        public IEnumerator<long> GetEnumerator()
        {
            return new DprBatchVersionVectorEnumerator(responseHead);
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public int Count { get; }

        public long this[int index]
        {
            get
            {
                fixed (byte* h = responseHead.Span)
                {
                    fixed (byte* b = Unsafe.AsRef<DprBatchResponseHeader>(h).versions)
                    {
                        var l = (long*) b;
                        return l[index + 1];
                    }
                }
            }
        }
    }
}