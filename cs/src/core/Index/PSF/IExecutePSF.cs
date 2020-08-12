// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Threading;
using System.Threading.Tasks;

namespace FASTER.core
{
    /// <summary>
    /// This interface is implemented on a <see cref="PSFGroup{TProviderData, TPSFKey, TRecordId}"/> and decouples
    /// the <see cref="PSF{TPSFKey, TRecordId}"/> execution from the knowledge of the TKVKey and TKVValue of the
    /// primary FasterKV instance.
    /// </summary>
    /// <typeparam name="TProviderData"></typeparam>
    /// <typeparam name="TRecordId"></typeparam>
    public interface IExecutePSF<TProviderData, TRecordId>
        where TRecordId : struct
    {
        /// <summary>
        /// For each <see cref="PSF{TPSFKey, TRecordId}"/> in the <see cref="PSFGroup{TProviderData, TPSFKey, TRecordId}"/>,
        /// and store the resultant TPSFKey in the secondary FasterKV instance.
        /// </summary>
        /// <param name="data">The provider's data, e.g. <see cref="FasterKVProviderData{TKVKey, TKVValue}"/></param>
        /// <param name="recordId">The provider's record ID, e.g. long (logicalAddress) for FasterKV</param>
        /// <param name="phase">The phase of PSF operations in which this execution is being done</param>
        /// <param name="changeTracker">Tracks the <see cref="PSFExecutePhase.PreUpdate"/> values for comparison
        ///     to the <see cref="PSFExecutePhase.PostUpdate"/> values</param>
        Status ExecuteAndStore(TProviderData data, TRecordId recordId, PSFExecutePhase phase,
                               PSFChangeTracker<TProviderData, TRecordId> changeTracker);

        /// <summary>
        /// The identifier of this <see cref="PSFGroup{TProviderData, TPSFKey, TRecordId}"/>.
        /// </summary>
        long Id { get; }

        /// <summary>
        /// Get the TPSFKeys for the current (before updating) state of the RecordId
        /// <param name="changeTracker">The record of previous key values and updated values</param>
        /// </summary>
        Status GetBeforeKeys(PSFChangeTracker<TProviderData, TRecordId> changeTracker);

        /// <summary>
        /// Update the RecordId
        /// <param name="changeTracker">The record of previous key values and updated values</param>
        /// </summary>
        Status Update(PSFChangeTracker<TProviderData, TRecordId> changeTracker);

        /// <summary>
        /// Delete the RecordId
        /// <param name="changeTracker">The record of previous key values and updated values</param>
        /// </summary>
        Status Delete(PSFChangeTracker<TProviderData, TRecordId> changeTracker);

        /// <summary>
        /// Take a full checkpoint of the FasterKV implementing the group's PSFs.
        /// </summary>
        bool TakeFullCheckpoint();

        /// <summary>
        /// Complete ongoing checkpoint (spin-wait)
        /// </summary>
        ValueTask CompleteCheckpointAsync(CancellationToken token = default);

        /// <summary>
        /// Take a checkpoint of the Index (hashtable) only
        /// </summary>
        bool TakeIndexCheckpoint();

        /// <summary>
        /// Take a checkpoint of the hybrid log only
        /// </summary>
        bool TakeHybridLogCheckpoint();

        /// <summary>
        /// Recover from last successful checkpoints
        /// </summary>
        void Recover();

        /// <summary>
        /// Flush PSF logs until current tail (records are still retained in memory)
        /// </summary>
        /// <param name="wait">Synchronous wait for operation to complete</param>
        void FlushLog(bool wait);

        /// <summary>
        /// Flush PSF logs and evict all records from memory
        /// </summary>
        /// <param name="wait">Synchronous wait for operation to complete</param>
        /// <returns>When wait is false, this tells whether the full eviction was successfully registered with FASTER</returns>
        public bool FlushAndEvictLog(bool wait);

        /// <summary>
        /// Delete PSF logs entirely from memory. Cannot allocate on the log
        /// after this point. This is a synchronous operation.
        /// </summary>
        public void DisposeLogFromMemory();
    }
}
