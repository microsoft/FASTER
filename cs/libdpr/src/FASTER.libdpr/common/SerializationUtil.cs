using System;
using System.Collections;
using System.Collections.Generic;

namespace FASTER.libdpr
{
    public static class SerializationUtil
    {
        public static int SerializeCheckpointMetadata(byte[] buf, int offset, long worldLine,
            WorkerVersion checkpointed, IEnumerable<WorkerVersion> deps)
        {
            if (!Utility.TryWriteBytes(new Span<byte>(buf, offset, buf.Length - offset), worldLine)) return 0;
            offset += sizeof(long);
            if (!Utility.TryWriteBytes(new Span<byte>(buf, offset, buf.Length - offset), checkpointed.Worker.guid))
                return 0;
            offset += sizeof(long);
            if (!Utility.TryWriteBytes(new Span<byte>(buf, offset, buf.Length - offset), checkpointed.Version))
                return 0;
            // skip 4 bytes of size field for now;
            var sizeField = offset;
            if (!Utility.TryWriteBytes(new Span<byte>(buf, offset, buf.Length - offset), 0)) return 0;
            var numDeps = 0;
            foreach (var wv in deps)
            {
                numDeps++;
                offset += sizeof(long);
                if (!Utility.TryWriteBytes(new Span<byte>(buf, offset, buf.Length - offset), wv.Worker.guid)) return 0;
                offset += sizeof(long);
                if (!Utility.TryWriteBytes(new Span<byte>(buf, offset, buf.Length - offset), wv.Version)) return 0;
            }

            if (!Utility.TryWriteBytes(new Span<byte>(buf, sizeField, buf.Length - sizeField), numDeps)) return 0;

            return offset;
        }

        public static void DeserializeCheckpointMetadata(byte[] buf, int offset, out long worldLine,
            out WorkerVersion checkpointed, out IEnumerable<WorkerVersion> deps)
        {
            var head = offset;
            worldLine = BitConverter.ToInt64(buf, head);
            head += sizeof(long);
            var worker = BitConverter.ToInt64(buf, head);
            head += sizeof(long);
            var version = BitConverter.ToInt64(buf, head);
            head += sizeof(long);
            checkpointed = new WorkerVersion(worker, version);
            deps = new EnumerableSerializedDeps(buf, head);
        }

        public class EnumerableSerializedDeps : IEnumerable<WorkerVersion>
        {
            private readonly byte[] buf;
            private readonly int offset;

            public EnumerableSerializedDeps(byte[] buf, int offset)
            {
                this.buf = buf;
                this.offset = offset;
            }

            public IEnumerator<WorkerVersion> GetEnumerator()
            {
                return new Enumerator(buf, offset);
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }

            private class Enumerator : IEnumerator<WorkerVersion>
            {
                private readonly byte[] buf;
                private readonly int offset;
                private readonly int size;
                private int ptr;

                public Enumerator(byte[] buf, int offset)
                {
                    this.buf = buf;
                    this.offset = offset;
                    size = BitConverter.ToInt32(buf, offset);
                    ptr = offset + sizeof(int) - sizeof(long) * 2;
                }

                public bool MoveNext()
                {
                    ptr += sizeof(long) * 2;
                    return ptr < offset + size;
                }

                public void Reset()
                {
                    ptr = offset + sizeof(int) - sizeof(long) * 2;
                }

                public WorkerVersion Current =>
                    new WorkerVersion(BitConverter.ToInt64(buf, ptr),
                        BitConverter.ToInt64(buf, ptr + sizeof(long)));

                object IEnumerator.Current => Current;

                public void Dispose()
                {
                }
            }
        }
    }
}