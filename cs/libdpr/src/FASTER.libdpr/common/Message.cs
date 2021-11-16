using System;
using System.Runtime.InteropServices;

namespace FASTER.libdpr
{
    /// <summary>
    ///     DPR metadata associated with each batch. Laid out continuously as:
    ///     header | deps (WorkerVersion[])
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = 52)]
    public unsafe struct DprBatchRequestHeader
    {
        public const int HeaderSize = 52;
        [FieldOffset(0)] public fixed byte data[HeaderSize];

        [FieldOffset(0)] public Guid sessionId;
        [FieldOffset(16)] public Worker workerId;
        // a batch should always consist of messages from the same world-lines on the client side.
        // We can artificially write the servers to not write reply batches with more than one world-line. 
        [FieldOffset(24)] public long worldLine;
        [FieldOffset(32)] public long versionLowerBound;
        [FieldOffset(40)] public int batchId;
        [FieldOffset(44)] public int numDeps;
        [FieldOffset(48)] public int numMessages;
        [FieldOffset(52)] public fixed byte deps[1];

        public int Size()
        {
            return HeaderSize + numDeps * sizeof(WorkerVersion);
        }
    }

    /// <summary>
    ///     DPR metadata associated with each batch. Laid out continuously as:
    ///     header | size (long) | version vector (long[])
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = 48)]
    public unsafe struct DprBatchResponseHeader
    {
        public const int HeaderSize = 48;
        [FieldOffset(0)] public fixed byte data[HeaderSize];
        [FieldOffset(0)] public Guid sessionId;
        [FieldOffset(16)] public Worker workerId;
        [FieldOffset(16)] public long worldLine;
        [FieldOffset(24)] public long versionUpperBound;
        [FieldOffset(32)] public int batchId;
        [FieldOffset(36)] public int batchSize;
        [FieldOffset(40)] public fixed byte versions[1];

        public int Size()
        {
            return batchSize;
        }
    }
}