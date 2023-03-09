// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

namespace FASTER.core
{
    /// <summary>
    /// Checkpoint type
    /// </summary>
    public enum CheckpointType
    {
        /// <summary>
        /// Take separate snapshot of in-memory portion of log (default)
        /// </summary>
        Snapshot,

        /// <summary>
        /// Flush current log (move read-only to tail)
        /// (enables incremental checkpointing, but log grows faster)
        /// </summary>
        FoldOver
    }

    /// <summary>
    /// Checkpoint-related settings
    /// </summary>
    public class CheckpointSettings
    {
        /// <summary>
        /// Checkpoint manager
        /// </summary>
        public ICheckpointManager CheckpointManager = null;

        /// <summary>
        /// Use specified directory for storing and retrieving checkpoints
        /// using local storage device.
        /// </summary>
        public string CheckpointDir = null;

        /// <summary>
        /// Whether FASTER should remove outdated checkpoints automatically
        /// </summary>
        public bool RemoveOutdated = false;

        /// <summary>
        /// Whether we should throttle the disk IO for checkpoints and issue IO from separate task
        /// </summary>
        public bool ThrottleCheckpointFlush;
    }
}
