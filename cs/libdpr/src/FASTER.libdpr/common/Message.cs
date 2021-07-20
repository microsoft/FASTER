using System;
using System.Runtime.InteropServices;

namespace FASTER.libdpr
{
    /// <summary>
    ///     DPR metadata associated with each batch. Laid out continuously as:
    ///     header | deps (WorkerVersion[])
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = 44)]
    public unsafe struct DprBatchRequestHeader
    {
        public const int HeaderSize = 44;
        [FieldOffset(0)] public fixed byte data[HeaderSize];

        [FieldOffset(0)] public Guid sessionId;

        // a batch should always consist of messages from the same world-lines on the client side.
        // We can artificially write the servers to not write reply batches with more than one world-line. 
        [FieldOffset(16)] public long worldLine;
        [FieldOffset(24)] public long versionLowerBound;
        [FieldOffset(32)] public int batchId;
        [FieldOffset(36)] public int numDeps;
        [FieldOffset(40)] public int numMessages;
        [FieldOffset(44)] public fixed byte deps[1];

        public int Size()
        {
            return HeaderSize + numDeps * sizeof(WorkerVersion);
        }
    }

    /// <summary>
    ///     DPR metadata associated with each batch. Laid out continuously as:
    ///     header | size (long) | version vector (long[])
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = 32)]
    public unsafe struct DprBatchResponseHeader
    {
        public const int HeaderSize = 32;
        [FieldOffset(0)] public fixed byte data[HeaderSize];
        [FieldOffset(0)] public Guid sessionId;
        [FieldOffset(16)] public long worldLine;
        [FieldOffset(24)] public int batchId;
        [FieldOffset(28)] public int batchSize;
        [FieldOffset(32)] public fixed byte versions[1];

        public int Size()
        {
            return batchSize;
        }
    }
}