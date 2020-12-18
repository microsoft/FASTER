using System;

namespace FASTER.libdpr
{
    /// <summary>
    /// ISimpleStateObject is a small API aimed towards more traditional state objects. We expect that
    /// operations always complete immediately (no pending) and checkpoints have no notion of versions.
    /// We require some implementation of operation descriptor and a way of telling if an operation belongs
    /// to a checkpoint, etc. For example, an op descriptor can be operation timestamp or an address on the
    /// log, and the checkpoint can check a descriptor against its time or persisted log offset. This leaves
    /// more freedom for underlying state object to coordinate concurrent checkpoints and operations.
    ///
    /// We expect SimpleStateObject to be less performant than the larger IStateObject API.
    /// </summary>
    /// <typeparam name="TBatch">Batch type, must implement AppendableMessageBatch</typeparam>
    /// <typeparam name="TMessage">Message type within each batch</typeparam>
    /// <typeparam name="TOpDescriptor">
    /// Operation descriptor that can be used to determine which checkpoint an operation belongs to.
    /// Not required to be globally unique, as libDPR does not use it to identify individual operations
    /// </typeparam>
    /// <typeparam name="TCheckpointToken">
    /// A checkpoint token that can be recovered to and efficiently
    /// determine membership of op descriptors
    /// </typeparam>
    public interface ISimpleStateObject<TBatch, TMessage, out TOpDescriptor, TCheckpointToken>
        where TBatch : IAppendableMessageBatch<TMessage>
        where TCheckpointToken : ICheckpointToken<TOpDescriptor>
    {
        /// <summary>
        /// Processes a single operation from Message
        /// </summary>
        /// <param name="request"></param>
        /// <param name="reply"></param>
        /// <param name="versionTracker"></param>
        TOpDescriptor Operate(ref TBatch request, TMessage message, ref TBatch reply);

        /// <summary>
        /// Begins a checkpoint to persist outstanding operations. After the function returns, no more new operations
        /// should be included in the returned checkpoint. When the returned checkpoint is recoverable on disk,
        /// which may be after its contents are determined (at which point the function is allowed to return), invokes
        /// the given callback with the largest persisted version and token. libDPR will not issue more than one
        /// outstanding checkpoint request at a time, but the checkpoint is expected to always succeed.
        /// </summary>
        /// <param name="onPersist">
        /// The action to invoke when the returned checkpoint is persistent. The callback is supplied with the token
        /// representing the persisted checkpoint.
        /// </param>
        /// <returns>A token that uniquely identifies the checkpoint taken</returns>
        TCheckpointToken BeginCheckpoint(Action<TCheckpointToken> onPersist);

        /// <summary>
        /// Recovers the state object to an earlier checkpoint, identified by the given token. After the function
        /// returns, all future operations should see the recovered state. libDPR will not issue more than one
        /// outstanding restore request at a time, but the restore is expected to always succeed.
        /// </summary>
        /// <param name="token">Unique checkpoint for the state object to recover to</param>
        /// <returns>Whether the restore is successful</returns>
        void RestoreCheckpoint(TCheckpointToken token);
    }

    /// <summary>
    /// A checkpoint token should uniquely identify a checkpoint that can be recovered to, and also efficiently
    /// tell if an operation belongs to it.
    /// </summary>
    /// <typeparam name="TOpDescriptor">Operation Descriptor Type</typeparam>
    public interface ICheckpointToken<in TOpDescriptor>
    {
        /// <summary></summary>
        /// <param name="op">operation to test</param>
        /// <returns>=If the given operation is part of the checkpoint</returns>
        bool Included(TOpDescriptor op);
    }
}