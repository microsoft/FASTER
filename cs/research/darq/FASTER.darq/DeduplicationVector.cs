using System;
using System.Threading;

namespace FASTER.libdpr
{
    internal class DeduplicationVector : IStateObjectAttachment
    {
        private const int MAX_SIZE = 32;
        private long[] dvc = new long[MAX_SIZE];
        private int used = 0;

        public DeduplicationVector()
        {
            for (var i = 0; i < MAX_SIZE; i++)
                dvc[i] = -1;
        }

        public bool Process(WorkerId id, long lsn)
        {
            var result =  core.Utility.MonotonicUpdate(ref dvc[id.guid], lsn, out var old);
            if (old == -1)
                Interlocked.Increment(ref used);
            return result;
        }

        public int SerializedSize()
        {
            return used * 2 * sizeof(long) + sizeof(int);
        }

        public void SerializeTo(Span<byte> buffer)
        {
            var head = 0;
            BitConverter.TryWriteBytes(buffer.Slice(head), used);
            head += sizeof(int);
            for (var i = 0; i < MAX_SIZE; i++)
            {
                if (dvc[i] == -1) continue;
                BitConverter.TryWriteBytes(buffer.Slice(head), (long) i);
                head += sizeof(long);
                BitConverter.TryWriteBytes(buffer.Slice(head), dvc[i]);
                head += sizeof(long);
            }
        }

        public void RecoverFrom(ReadOnlySpan<byte> serialized)
        {
            unsafe
            {
                fixed (byte* b = serialized)
                {
                    var head = b;
                    var count = *(int*) head;
                    head += sizeof(int);
                    for (var i = 0; i < count; i++)
                    {
                        var worker = *(long*) head;
                        head += sizeof(long);
                        var val = *(long*) head;
                        head += sizeof(long);
                        dvc[worker] = val;
                    }
                    
                }
            }
        }
    }
}