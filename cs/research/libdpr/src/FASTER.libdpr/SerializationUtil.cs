using System;
using System.Collections;
using System.Collections.Generic;

namespace FASTER.libdpr
{
    internal static class SerializationUtil
    {
        internal static unsafe int SerializeCheckpointMetadata(Span<byte> buffer, long worldLine,
            WorkerVersion checkpointed, IEnumerable<WorkerVersion> deps)
        {
            fixed (byte* b = buffer)
            {
                var end = b + buffer.Length;
                var head = b;
                if (!Utility.TryWriteBytes(new Span<byte>(head, (int) (end - head)), worldLine)) return 0;
                head += sizeof(long);
                if (!Utility.TryWriteBytes(new Span<byte>(head, (int) (end - head)), checkpointed.Worker.guid))
                    return 0;
                head += sizeof(long);
                if (!Utility.TryWriteBytes(new Span<byte>(head, (int) (end - head)), checkpointed.Version))
                    return 0;
                head += sizeof(long);
                // skip 4 bytes of size field for now;
                var sizeField = (int *) head;
                if ((int) (end - head) < sizeof(int)) return 0;
                head += sizeof(int);
                var numDeps = 0;
                foreach (var wv in deps)
                {
                    numDeps++;
                    head += sizeof(long);
                    if (!Utility.TryWriteBytes(new Span<byte>(head, (int) (end - head)), wv.Worker.guid)) return 0;
                    head += sizeof(long);
                    if (!Utility.TryWriteBytes(new Span<byte>(head, (int) (end - head)), wv.Version)) return 0;
                }

                *sizeField = numDeps;
                return (int) (head - b);
            }
        }

        internal static unsafe int DeserializeCheckpointMetadata(ReadOnlySpan<byte> buffer, out long worldLine,
            out WorkerVersion checkpointed, out IEnumerable<WorkerVersion> deps)
        {
            fixed (byte* b = buffer)
            {
                var head = b;
                worldLine = *(long *) head;
                head += sizeof(long);
                var worker = *(long *) head;
                head += sizeof(long);
                var version = *(long *) head;
                head += sizeof(long);
                checkpointed = new WorkerVersion(worker, version);
                var d = new EnumerableSerializedDeps(head);
                deps = d;
                return (int) (d.GetDepsTail() - b);
            }
        }

        internal unsafe class EnumerableSerializedDeps : IEnumerable<WorkerVersion>
        {
            private readonly byte* head;

            public EnumerableSerializedDeps(byte* head)
            {
                this.head = head;
            }

            public byte* GetDepsTail()
            {
                var numDeps = *(int*) head;
                return head + sizeof(int) + 2 * sizeof(long) * numDeps;
            }

            public IEnumerator<WorkerVersion> GetEnumerator()
            {
                return new Enumerator(head);
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }

            private class Enumerator : IEnumerator<WorkerVersion>
            {
                private readonly byte* head;
                private readonly int size;
                private int ptr;

                public Enumerator(byte* head)
                {
                    this.head = head;
                    size = *(int *) head;
                    ptr = sizeof(int) - sizeof(long) * 2;
                }

                public bool MoveNext()
                {
                    ptr += sizeof(long) * 2;
                    return ptr < size;
                }

                public void Reset()
                {
                    ptr = sizeof(int) - sizeof(long) * 2;
                }

                public WorkerVersion Current =>
                    new WorkerVersion(*(long*) (head + ptr), *(long*) (head + ptr + sizeof(long)));

                object IEnumerator.Current => Current;

                public void Dispose()
                {
                }
            }
        }
    }
}