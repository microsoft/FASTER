using System.Collections;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace FASTER.libdpr
{
    public unsafe struct DprBatchVersionVector : IEnumerable<long>
    {
        private byte *vectorHead;

        public DprBatchVersionVector(byte *vectorHead)
        {
            this.vectorHead = vectorHead;
        }

        private class DprBatchVersionVectorEnumerator : IEnumerator<long>
        {
            private long* start;
            private long index = 0;

            public DprBatchVersionVectorEnumerator(byte* start)
            {
                this.start = (long*) start;
            }

            public bool MoveNext()
            {
                if (index >= start[0]) return false;
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

        public IEnumerator<long> GetEnumerator()
        {
            return new DprBatchVersionVectorEnumerator(vectorHead);
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }

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
            return (versions.Count + 1) * sizeof(long);
        }

        public unsafe void AppendOntoResponse(ref DprBatchResponseHeader response)
        {
            fixed (byte* start = &response.versions[0])
            {
                Unsafe.AsRef<long>(start) = versions.Count;
                for (var i = 1; i <= versions.Count; i++)
                    Unsafe.AsRef<long>(start + sizeof(long) * i) = versions[i];
            }
        }

        public unsafe void PopulateFromResponse(ref DprBatchResponseHeader response)
        {
            versions.Clear();
            fixed (byte* start = &response.versions[0])
            {
                var count = Unsafe.AsRef<long>(start);
                for (var i = 1; i <= count; i++)
                    versions[i] = Unsafe.AsRef<long>(start + sizeof(long) * i);
            }
        }
    }
}