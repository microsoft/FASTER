using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace FASTER.libdpr
{
    /// <summary>
    ///     A class that tracks version information per batch.
    ///     Conceptually a tracker is a batch offset -> version mapping, where the user of this library writes down for
    ///     each operation the version it was executed at, or NOT_EXECUTED if the operation was skipped for some reason.
    /// </summary>
    public class DprBatchVersionTracker
    {
        /// <summary> Special value to use when an operation was not executed for whatever reason </summary>
        public const long NotExecuted = 0;
        
        // TODO(Tianyu): Move towards more sophisticated representation if necessary
        private const int DefaultBatchSize = 1024;
        internal long maxVersion = NotExecuted;
        private readonly List<long> versions = new List<long>(DefaultBatchSize);

        /// <summary>
        ///     Records that the operation at the given batch offset was executed at the given version
        /// </summary>
        /// <param name="batchOffset"> operation as identified by its position in a batch</param>
        /// <param name="executedVersion">the version said operation was executed at</param>
        public void MarkOneOperationVersion(int batchOffset, long executedVersion)
        {
            while (versions.Count <= batchOffset)
                versions.Add(NotExecuted);

            versions[batchOffset] = executedVersion;
            maxVersion = Math.Max(executedVersion, maxVersion);
        }

        /// <summary>
        ///     Records that a contiguous range of operations within a batch were all executed at the given version
        /// </summary>
        /// <param name="batchOffsetStart">
        ///     start of operation range  as identified by its position in a batch, inclusive
        /// </param>
        /// <param name="batchOffsetEnd">
        ///     end of operation range as identified by its position in a batch, exclusive
        /// </param>
        /// <param name="executedVersion">the version said operations were executed at</param>
        public void MarkOperationRangesVersion(int batchOffsetStart, int batchOffsetEnd, long executedVersion)
        {
            for (var i = batchOffsetStart; i < batchOffsetEnd; i++)
                MarkOneOperationVersion(i, executedVersion);
        }

        /// <summary>
        ///     Computes and returns size of the current version vector when encoded onto the wire, in bytes.
        /// </summary>
        /// <returns>size of the current version vector when encoded onto the wire, in bytes</returns>
        public int EncodingSize()
        {
            return (versions.Count + 1) * sizeof(long);
        }

        /// <summary>
        ///     Serializes the content of this version tracker onto the supplied response header. The header is assumed have
        ///     enough space allocated to fit this information.
        /// </summary>
        /// <param name="response"> Reference to the destination header </param>
        internal unsafe void AppendToHeader(ref DprBatchHeader response)
        {
            fixed (byte* start = response.data)
            {
                var versionVector = (long *)(start + response.VersionVectorOffset);
                versionVector[0] = versions.Count;

                for (var i = 0; i < versions.Count; i++)
                    versionVector[i + 1] = versions[i];
            }
        }

        internal void Reset()
        {
            versions.Clear();
            maxVersion = NotExecuted;
        }
    }
}