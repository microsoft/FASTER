using System;
using System.Collections.Generic;
using FASTER.core;

namespace FASTER.libdpr
{
    /// <summary>
    /// Abstraction for a versioned state-store with checkpointing and rollback functionalities used in DPR.
    /// </summary>
    public interface IStateObject
    {
        /// <summary>
        /// Performs a checkpoint uniquely identified by the given version number along with the given metadata to be
        /// persisted. Implementers are allowed to return as soon as the checkpoint content is finalized, but before
        /// it is persistent, but must invoke onPersist afterwards. LibDPR will ensure that this function does not
        /// interleave with protected processing logic, other checkpoint requests, or restores. 
        /// </summary>
        /// <param name="version"> A monotonically increasing unique ID describing this checkpoint </param>>
        /// <param name="metadata"> Any metadata, in bytes, to be persisted along with the checkpoint </param>
        /// <param name="onPersist"> Callback to invoke when checkpoint is persistent </param>
        void PerformCheckpoint(long version, ReadOnlySpan<byte> metadata, Action onPersist);

        /// <summary>
        /// Recovers to a previous checkpoint as identified by the version number, along with any metadata. The function
        /// returns only after the state is restored for all future calls. LibDPR will not interleave batch operation,
        /// other checkpoint requests, or restore requests with this function.
        /// </summary>
        /// <param name="version"> unique ID for the checkpoint to recover </param>>
        /// <param name="metadata"> Any metadata, in bytes, persisted along with the checkpoint </param>
        void RestoreCheckpoint(long version, out ReadOnlySpan<byte> metadata);

        /// <summary>
        /// Removes a version from persistent storage. This method is only invoked when a version will no longer be
        /// recovered to.
        /// </summary>
        /// <param name="version"> unique ID for the checkpoint to remove </param>
        void PruneVersion(long version);

        /// <summary>
        /// Retrieves information about all unpruned checkpoints on persistent storage, along with persisted metadata. 
        /// </summary>
        /// <returns>
        /// enumerable of (byte array, start offset) that denotes the metadata of each unpruned checkpoint
        /// </returns>
        IEnumerable<(byte[], int)> GetUnprunedVersions();
    }
}