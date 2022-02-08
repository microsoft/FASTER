using System;
using System.Runtime.InteropServices;

namespace FASTER.libdpr
{
    /// <summary>
    ///     DPR metadata associated with each batch. Laid out continuously as:
    ///     header | deps (WorkerVersion[]) | versionTracking (long[])
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = 32)]
    internal unsafe struct DprBatchHeader
    {
        internal const int FixedLenSize = 32;
        [FieldOffset(0)] internal fixed byte data[FixedLenSize];
        [FieldOffset(0)] internal Worker srcWorkerId;
        // a batch should always consist of messages from the same world-lines on the client side.
        // We can artificially write the servers to not write reply batches with more than one world-line. 
        [FieldOffset(8)] internal long worldLine;
        [FieldOffset(16)] internal long version;
        [FieldOffset(24)] internal int batchId;
        [FieldOffset(28)] internal int numAdditionalDeps;

        internal int AdditionalDepsOffset => FixedLenSize;

        internal int VersionVectorOffset => FixedLenSize + numAdditionalDeps * sizeof(WorkerVersion);
    }
}