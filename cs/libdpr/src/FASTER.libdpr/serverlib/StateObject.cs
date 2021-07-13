
using System;
using System.Collections;
using System.Collections.Generic;

namespace FASTER.libdpr
{
    /// <summary>
    /// Abstracts an underlying versioned state-store with non-blocking checkpointing and rollback functionalities.
    /// </summary>
    public interface IStateObject
    {
        /// <summary>
        /// Registers a set of callbacks that libDPR expects to be called at certain points by the underlying state
        /// object. This function will only be called once on start up, before any operations are started.
        /// </summary>
        /// <param name="callbacks">callbacks to be invoked</param>
        void Register(DprWorkerCallbacks callbacks);

        /// <summary>
        /// The current version of the object. Returned value should be a lower bound of the execution version of any
        /// future operations on this thread --- e.g., if this call returns 9, any future operations on the thread
        /// should execute in at least version 9.
        /// </summary>
        /// <returns>Current version (lower bound) of the state object</returns>
        long Version();

        /// <summary>
        /// Gets all DPR information associated with a version as a byte array
        /// </summary>
        delegate ReadOnlySpan<byte> DepsProvider(long version);
        
        /// <summary>
        /// Begins a checkpoint to advance to the targetVersion. Invocation of the call only guarantees that object
        /// version eventually reaches targetVersion, and the function may return without performing the checkpoint
        /// (e.g., if targetVersion is smaller than current version, or if another concurrent checkpoint is underway).
        /// libDPR expects to receive checkpoint-related information, such as the actual version checkpointed, through
        /// registered callback instead of this function.
        ///
        /// Depending on the DprFinder backend chosen, state objects may need to persist additional DPR information
        /// along with each checkpoint. If that is the case, the state object should invoke depsProvider after
        /// version end to obtain the bytes to write out. It is ok to ignore this requirement if a state object will
        /// only be connected to a DprFinder backend that does not require this. 
        /// </summary>
        /// <param name="depsProvider">function to get DPR byte header to write with each checkpoint</param>>
        /// <param name="targetVersion">The version to jump to, or -1 if unconditionally jumping to next version</param>
        void BeginCheckpoint(DepsProvider depsProvider, long targetVersion = -1);

        /// <summary>
        /// Recovers the state object to an earlier checkpoint, identified by the given version. After the function
        /// returns, all future operations should see the recovered state. A restore call must eventually succeed per
        /// invocation, but libDPR will only have one outstanding restore call at a time.
        /// </summary>
        /// <param name="version">Unique checkpoint for the state object to recover to</param>
        void BeginRestore(long version);

        /// <summary>
        /// Removes a version locally. This method is invoked when a version will no longer be recovered to (rolled
        /// back or a newer version is committed)
        /// </summary>
        /// <param name="version"> version number to remove </param>
        void PruneVersion(long version);

        /// <summary>
        /// Retrieves information about all locally available checkpoints. If the method is called, the state object
        /// must return the exact DPR header written out as part of the BeginCheckpoint call, for every checkpoint
        /// that was completed but not yet pruned.
        ///
        /// Not every DprFinder will call this method, so it is ok to not implement this method if a state object will
        /// only be connected to a DprFinder that does not require this.
        /// </summary>
        /// <returns>
        /// enumerable of (byte array, offset) that denotes the start of each locally present checkpoint's
        /// DPR header
        /// </returns>
        IEnumerable<(byte[], int)> GetUnprunedVersions();
    }
}