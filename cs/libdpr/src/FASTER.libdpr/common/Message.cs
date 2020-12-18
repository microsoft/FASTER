using System;
using System.Collections;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace FASTER.libdpr
{
    /// <summary>
    /// We expect underlying systems to implement this interface to allow libDPR to add metadata for DPR tracking.
    /// </summary>
    public interface IAppendableMessageBatch<Message> : IEnumerable<Message>
    {
        void AddBytes(Span<byte> bytes);

        Span<byte> GetAdditionalBytes();
    }

    /// <summary>
    /// A DprMessageHeader is DPR metadata associated with each operation of the state object
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = 8)]
    public unsafe struct DprMessageHeader
    {
        /// <summary>
        /// Special value for when the underlying state object implementation rejects an operation for whatever reason.
        /// This will prompt libDPR client to retry and disregard this failed execution in DPR tracking 
        /// </summary>
        public const long NOT_EXECUTED = 0;
        [FieldOffset(0)]
        public long data;
    }

    /// <summary>
    /// DPR metadata associated with each batch. Laid out continuously as:
    /// header | deps (WorkerVersion[]) | message metadata (DprMessageHeader[])
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = 44)]
    public unsafe struct DprBatchHeader
    {
        public const int HeaderSize = 44;
        [FieldOffset(0)]
        public fixed byte data[HeaderSize];
        [FieldOffset(0)]
        public Guid sessionId;
        // a batch should always consist of messages from the same world-lines on the client side.
        // We can artificially write the servers to not write reply batches with more than one world-line. 
        [FieldOffset(16)]
        public long worldLine;
        [FieldOffset(24)]
        public long versionLowerBound;
        [FieldOffset(32)]
        public int size;
        [FieldOffset(36)]
        public int numMessages;
        [FieldOffset(40)]
        public int numDeps;
        [FieldOffset(44)]
        public fixed byte deps[0];

        public ref byte MessageHeaderStart()
        {
            return ref deps[numDeps * sizeof(WorkerVersion)];
        }

        public void MarkOperationVersion(int batchOffset, long executedVersion)
        {
            fixed (byte* d = deps)
            {
                Unsafe.AsRef<DprMessageHeader>(d + numDeps * sizeof(WorkerVersion)).data = executedVersion;
            }
        }
    }
}