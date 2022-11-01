using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using FASTER.core;

namespace FASTER.libdpr
{
    public enum DprBatchStatus
    {
        OK, ROLLBACK, IGNORE
    }
    
    /// <summary>
    /// A DprSession is a DPR entity that cannot commit/restore state, but communicates with other DPR entities and may
    /// convey DPR dependencies (e.g., a client session).
    /// </summary>
    public class DprSession
    {
        private long version, worldLine, terminalWorldLine;
        private readonly LightDependencySet deps;
        
        /// <summary>
        /// Version of the session
        /// </summary>
        public long Version => version;

        /// <summary>
        /// WorldLine of the session
        /// </summary>
        public long WorldLine => worldLine;

        /// <summary>
        /// Create a DPR session working on the supplied worldLine (or 1 by default, in a cluster that has never failed)
        /// </summary>
        /// <param name="startWorldLine"> the worldLine to start at, or 1 by default </param>
        public DprSession(long startWorldLine = 1)
        {
            version = 1;
            worldLine = startWorldLine;
            deps = new LightDependencySet();
        }

        /// <summary>
        /// Restart a DPR session from one that has experience a rollback. The new DPR session works on the new worldLine
        /// and inherits any dependencies of the old session that survived the rollback.
        /// </summary>
        /// <param name="failedSession"></param>
        public DprSession(DprSession failedSession)
        {
            if (failedSession.terminalWorldLine == -1)
                throw new FasterException("Nothing to recover from!");
            version = failedSession.version;
            worldLine = failedSession.terminalWorldLine;
            terminalWorldLine = -1;
            deps = failedSession.deps;
        }

        /// <summary>
        /// Obtain a DPR header that encodes session dependency for an outgoing message
        /// </summary>
        /// <param name="headerBytes"> byte array to write header into </param>
        /// <returns> size of the header, or negative of the required size to fit if supplied header is to small </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe int ComputeHeaderForSend(Span<byte> headerBytes)
        {
            fixed (byte* b = headerBytes)
            {
                var bend = b + headerBytes.Length;
                ref var dprHeader = ref Unsafe.AsRef<DprBatchHeader>(b);

                // Populate header with relevant request information
                if (headerBytes.Length >= DprBatchHeader.FixedLenSize)
                {
                    dprHeader.SrcWorkerIdId = WorkerId.INVALID;
                    dprHeader.worldLine = worldLine;
                    dprHeader.version = version;
                    dprHeader.numClientDeps = 0;
                }

                // Populate tracking information into the batch
                var copyHead = b + dprHeader.ClientDepsOffset;
                foreach (var wv in deps)
                {
                    dprHeader.numClientDeps++;
                    // only copy if it fits
                    if (copyHead < bend - sizeof(WorkerVersion))
                        Unsafe.AsRef<WorkerVersion>(copyHead) = wv;
                    copyHead += sizeof(WorkerVersion);
                }               
                
                // Invert depends on whether or not we fit
                return (int) (copyHead <= bend ? copyHead - b : b - copyHead);
            }
        }

        /// <summary>
        /// Receive a message with the given header in this session. 
        /// </summary>
        /// <param name="dprMessage"> DPR header of the message to receive </param>
        /// <param name="version"> version of the message </param>
        /// <returns> status of the batch. If status is ROLLBACK, this session must be rolled-back </returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe DprBatchStatus ReceiveHeader(ReadOnlySpan<byte> dprMessage, out long version)
        {
            fixed (byte* h = dprMessage)
            {
                ref var responseHeader = ref Unsafe.AsRef<DprBatchHeader>(h);
                version = responseHeader.version;
                // Server uses negative worldLine to signal that the client is lower in worldline
                if (responseHeader.worldLine < 0)
                {
                    core.Utility.MonotonicUpdate(ref terminalWorldLine, -responseHeader.worldLine, out _);
                    return DprBatchStatus.ROLLBACK;
                }

                if (responseHeader.worldLine < worldLine)
                    return DprBatchStatus.IGNORE;

                Debug.Assert(responseHeader.worldLine == worldLine);
                
                // Add largest worker-version as dependency for future ops
                if (!responseHeader.SrcWorkerIdId.Equals(WorkerId.INVALID))
                    deps.Update(responseHeader.SrcWorkerIdId, responseHeader.version);
                else
                {
                    fixed (byte* d = responseHeader.data)
                    {
                        var depsHead = d + responseHeader.ClientDepsOffset;
                        for (var i = 0; i < responseHeader.numClientDeps; i++)
                        {
                            ref var wv = ref Unsafe.AsRef<WorkerVersion>(depsHead);
                            deps.Update(wv.WorkerId, wv.Version);
                            depsHead += sizeof(WorkerVersion);
                        }
                    }
                }

                // Update versioning information
                core.Utility.MonotonicUpdate(ref this.version, responseHeader.version, out _);

                // Remove deps only if this is a response header from a server session
                if (!responseHeader.SrcWorkerIdId.Equals(WorkerId.INVALID))
                {
                    // Update dependency tracking
                    var depsHead = h + responseHeader.ClientDepsOffset;
                    for (var i = 0; i < responseHeader.numClientDeps; i++)
                    {
                        ref var wv = ref Unsafe.AsRef<WorkerVersion>(depsHead);
                        if (wv.WorkerId.Equals(responseHeader.SrcWorkerIdId)) continue;
                        deps.TryRemove(wv.WorkerId, wv.Version);
                        depsHead += sizeof(WorkerVersion);
                    }
                }
            }

            return DprBatchStatus.OK;
        }
    }
}