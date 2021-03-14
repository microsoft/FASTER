using System.Collections;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace FASTER.libdpr
{
    /// <summary>
    /// on-wire format for a vector of version numbers. Does not own or allocate underlying memory.
    /// </summary>
    public unsafe struct DprBatchVersionVector : IEnumerable<long>
    {
        private readonly byte *vectorHead;

        /// <summary>
        /// Construct a new VersionVector to be backed by the given byte*
        /// </summary>
        /// <param name="vectorHead"></param>
        public DprBatchVersionVector(byte *vectorHead)
        {
            this.vectorHead = vectorHead;
        }

        private class DprBatchVersionVectorEnumerator : IEnumerator<long>
        {
            private readonly long* start;
            private long index;

            public DprBatchVersionVectorEnumerator(byte* start)
            {
                this.start = (long*) start;
            }

            public bool MoveNext()
            {
                if (index + 1 >= start[0]) return false;
                index++;
                return true;
            }

            public void Reset()
            {
                index = 0;
            }
            
            public long Current => start[index + 1];

            object IEnumerator.Current => Current;

            public void Dispose()
            {
            }
        }

        /// <summary>
        /// Returns an enumerator through version numbers
        /// </summary>
        /// <returns>an enumerator through version numbers</returns>
        public IEnumerator<long> GetEnumerator()
        {
            return new DprBatchVersionVectorEnumerator(vectorHead);
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }

    /// <summary>
    /// A class that tracks version information per batch.
    ///
    /// Conceptually a tracker is a batch offset -> version mapping, where the user of this library writes down for
    /// each operation the version it was executed at, or NOT_EXECUTED if the operation was skipped for some reason.  
    /// </summary>
    public class DprBatchVersionTracker
    {
        /// <summary> Special value to use when an operation was not executed for whatever reason </summary>
        public const long NotExecuted = 0;

        // TODO(Tianyu): Move towards more sophisticated representation if necessary
        private const int DefaultBatchSize = 1024;
        private readonly List<long> versions = new List<long>(DefaultBatchSize);

        /// <summary>
        /// Records that the operation at the given batch offset was executed at the given version
        /// </summary>
        /// <param name="batchOffset"> operation as identified by its position in a batch</param>
        /// <param name="executedVersion">the version said operation was executed at</param>
        public void MarkOneOperationVersion(int batchOffset, long executedVersion)
        {
            while (versions.Count <= batchOffset)
                versions.Add(NotExecuted);

            versions[batchOffset] = executedVersion;
        }

        /// <summary>
        /// Records that a contiguous range of operations within a batch were all executed at the given version
        /// </summary>
        /// <param name="batchOffsetStart">
        /// start of operation range  as identified by its position in a batch, inclusive
        /// </param>
        /// <param name="batchOffsetEnd">
        /// end of operation range as identified by its position in a batch, exclusive
        /// </param>
        /// <param name="executedVersion">the version said operations were executed at</param>
        public void MarkOperationRangesVersion(int batchOffsetStart, int batchOffsetEnd, long executedVersion)
        {
            for (var i = batchOffsetStart; i < batchOffsetEnd; i++)
                MarkOneOperationVersion(i, executedVersion);
        }

        /// <summary>
        /// Computes and returns size of the current version vector when encoded onto the wire, in bytes.
        /// </summary>
        /// <returns>size of the current version vector when encoded onto the wire, in bytes</returns>
        public int EncodingSize()
        {
            return (versions.Count + 1) * sizeof(long);
        }

        /// <summary>
        /// Serializes the content of this version tracker onto the supplied response header. The header is assumed have
        /// enough space allocated to fit this information.
        /// </summary>
        /// <param name="response"> Reference to the destination header </param>
        public unsafe void AppendOntoResponse(ref DprBatchResponseHeader response)
        {
            fixed (byte* start = &response.versions[0])
            {
                Unsafe.AsRef<long>(start) = versions.Count;
                for (var i = 1; i <= versions.Count; i++)
                    Unsafe.AsRef<long>(start + sizeof(long) * i) = versions[i - 1];
            }
        }
    }
}