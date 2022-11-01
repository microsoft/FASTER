using System;
using System.Runtime.InteropServices;

namespace FASTER.libdpr
{
    /// <summary>
    ///     DPR metadata associated with each batch. Laid out continuously as:
    ///     header | deps (WorkerVersion[]) | versionTracking (long[])
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = 28)]
    public unsafe struct DprBatchHeader
    {
        public const int FixedLenSize = 28;
        [FieldOffset(0)] internal fixed byte data[FixedLenSize];
        [FieldOffset(0)] internal WorkerId SrcWorkerIdId;
        // a batch should always consist of messages from the same world-lines on the client side.
        // We can artificially write the servers to not write reply batches with more than one world-line. 
        [FieldOffset(8)] internal long worldLine;
        [FieldOffset(16)] internal long version;
        [FieldOffset(24)] internal int numClientDeps;
        internal int ClientDepsOffset => FixedLenSize;
    }
}