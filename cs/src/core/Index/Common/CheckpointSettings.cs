// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.


using System;

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
        /// Directory where checkpoints are stored
        /// </summary>
        public string CheckpointBasePath = "";

        /// <summary>
        /// Checkpoint device for given path
        /// </summary>
        public Func<string, IDevice> CheckpointDeviceFunc;

        /// <summary>
        /// Type of checkpoint
        /// </summary>
        public CheckpointType CheckPointType = CheckpointType.Snapshot;
    }
}
