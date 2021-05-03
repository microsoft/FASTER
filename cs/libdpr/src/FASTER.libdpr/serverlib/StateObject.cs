
namespace FASTER.libdpr
{
    /// <summary>
    /// Abstracts an underlying versioned state-store with non-blocking checkpointing and rollback functionalities.
    /// </summary>
    /// <typeparam name="TToken">Type of token that uniquely identifies a checkpoint</typeparam>
    public interface IStateObject<TToken>
    {
        /// <summary>
        /// Registers a set of callbacks that libDPR expects to be called at certain points by the underlying state
        /// object. This function will only be called once on start up, before any operations are started.
        /// </summary>
        /// <param name="callbacks">callbacks to be invoked</param>
        void Register(DprWorkerCallbacks<TToken> callbacks);

        /// <summary>
        /// The current version of the object. Returned value should be a lower bound of the execution version of any
        /// future operations on this thread --- e.g., if this call returns 9, any future operations on the thread
        /// should execute in at least version 9.
        /// </summary>
        /// <returns>Current version (lower bound) of the state object</returns>
        long Version();
        
        /// <summary>
        /// Begins a checkpoint to advance to the targetVersion. Invocation of the call only guarantees that object
        /// version eventually reaches targetVersion, and the function may return without performing the checkpoint
        /// (e.g., if targetVersion is smaller than current version, or if another concurrent checkpoint is underway).
        /// libDPR expects to receive checkpoint-related information, such as token, through registered callback
        /// instead of this function.
        /// </summary>
        /// <param name="targetVersion">The version to jump to, or -1 if jumping to next version</param>
        void BeginCheckpoint(long targetVersion = -1);

        /// <summary>
        /// Recovers the state object to an earlier checkpoint, identified by the given token. After the function
        /// returns, all future operations should see the recovered state. A restore call must eventually succeed per
        /// invocation, but libDPR will only have one outstanding restore call at a time.
        /// </summary>
        /// <param name="token">Unique checkpoint for the state object to recover to</param>
        void BeginRestore(TToken token);
    }
}