using System;
using System.Runtime.InteropServices;

namespace FASTER.libdpr
{
    /// <summary>
    /// DPR metadata associated with each batch. Laid out continuously as:
    /// header | deps (WorkerVersion[]) | message metadata (DprMessageHeader[])
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = 40)]
    public unsafe struct DprBatchRequestHeader
    {
        public const int HeaderSize = 40;
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
        public int batchId;
        [FieldOffset(36)]
        public int numDeps;
        [FieldOffset(40)]
        public fixed byte deps[0];
    }

    [StructLayout(LayoutKind.Explicit, Size = 28)]
    public unsafe struct DprBatchResponseHeader
    {
        public const int HeaderSize = 28;
        [FieldOffset(0)]
        public fixed byte data[HeaderSize];
        [FieldOffset(0)]
        public Guid sessionId;
        [FieldOffset(16)]
        public long worldLine;
        [FieldOffset(24)]
        public int batchId;
        [FieldOffset(28)]
        public fixed byte versions[0];
    }
}