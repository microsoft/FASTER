
using System;

namespace FASTER.libdpr
{
    public interface IStateObject<TToken>
    {
        void Register(DprWorkerCallbacks<TToken> callbacks);

        // TODO(Tianyu): This must be the lowest version any thread can execute in at the time of invocation. For FASTER,
        // this means that either it needs to return the lowest version of all active threads, or refresh a thread immediately
        // before executing a batch.
        long Version();
        /// <summary>
        /// Begins a checkpoint to advance to the targetVersion. After the function returns, no more new operations
        /// should be included in the returned checkpoint. When the returned checkpoint is recoverable on disk,
        /// which may be after its contents are determined (at which point the function is allowed to return), invokes
        /// the given callback with the largest persisted version and token. A checkpoint is allowed to fail arbitrarily,
        /// but libDPR may retry the same checkpoint call until object version is larger than or equal to targetVersion.
        /// </summary>
        /// <param name="onPersist">
        /// The action to invoke when the returned checkpoint is persistent. The callback is supplied with the
        /// largest persisted version (e.g. when bumping from version 5 to 7, the action with be invoked
        /// with 5), and the token representing the persisted checkpoint.
        /// </param>
        /// <param name="token">Unique identifier for a checkpoint that can be recovered to</param>
        /// <param name="targetVersion">The version to jump to, or -1 if jumping to next version</param>
        /// <returns>Whether the checkpoint is successful.</returns>
        bool BeginCheckpoint(out TToken token, long targetVersion = -1);

        /// <summary>
        /// Recovers the state object to an earlier checkpoint, identified by the given token. After the function
        /// returns, all future operations should see the recovered state. A restore call must succeed.
        /// </summary>
        /// <param name="token">Unique checkpoint for the state object to recover to</param>
        void BeginRestore(TToken token);
    }
}