using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace FASTER.libdpr.versiontracking
{
    public class DprBatchVersionTracker
    {
        /// <summary></summary>
        public const long NOT_EXECUTED = 0;
        
        // TODO(Tianyu): Move towards more sophisticated representation if necessary
        private const int DEFAULT_BATCH_SIZE = 1024;
        private List<long> versions = new List<long>(DEFAULT_BATCH_SIZE);

        public void MarkOneOperationVersion(int batchOffset, long executedVersion)
        {
            // TODO(Tianyu): Is there no extend method with default value on C# lists?
            while (versions.Count <= batchOffset)
                versions.Add(NOT_EXECUTED);

            versions[batchOffset] = executedVersion;
        }

        public void MarkOperationRangesVersion(int batchOffsetStart, int batchOffsetEnd, long executedVersion)
        {
            for (var i = batchOffsetStart; i < batchOffsetEnd; i++)
                MarkOneOperationVersion(i, executedVersion);
        }

        public int EncodingSize()
        {
            return versions.Count * sizeof(long);
        }
        
        public unsafe void AppendOntoResponse(ref DprBatchResponseHeader response)
        {
            fixed (byte* start = &response.versions[0])
            {
                for (var i = 0; i < versions.Count; i++)
                    Unsafe.AsRef<long>(start + sizeof(long) * i) = versions[i];
            }
        }
    }
}