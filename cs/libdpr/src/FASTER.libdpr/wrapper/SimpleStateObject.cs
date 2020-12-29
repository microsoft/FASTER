using System;

namespace FASTER.libdpr
{
    /// <summary>
    /// Represents a byte buffer passed to libDPR whose scope may outlive its original owner. libDPR will call
    /// Dispose() on this object when it is finished with the underlying buffer. 
    /// </summary>
    public interface IDisposableBuffer : IDisposable
    {
        /// <summary></summary>
        /// <returns>Underlying buffer</returns>
        Span<byte> Bytes();
    }
    
    /// <summary>
    /// Abstracts a non-versioned state-store that performs checkpoints and rollbacks synchronously. This is a
    /// simpler API than IStateObject and can accomodate a wider range of state-stores.
    /// </summary>
    /// <typeparam name="TToken">Type of token that uniquely identifies a checkpoint</typeparam>
    public interface ISimpleStateObject<TToken>
    {
        /// <summary>
        /// Sends a batch to the underlying state-store for processing.
        /// 
        /// A batch is considered an atomic unit of work for SimpleStateObjects, meaning that a batch will not cross
        /// version boundaries, and will either be recoverable as a while or not at all. Notice that this only applies
        /// to recoverability and does not apply to higher-level application semantics -- applications may still see
        /// partial effects of a batch during normal operation. The underlying state-store may reject the
        /// batch by returning false without executing it, but must do so at a batch-granularity in accordance with
        /// the atomicity of batches(i.e., either all requests within a batch is executed, or none).
        /// </summary>
        /// <param name="request">Bytes from client encoding the request to underlying state-store</param>
        /// <param name="reply">Replies to send back to the client</param>
        /// <returns>Whether the batch was executed</returns>
        bool ProcessBatch(ReadOnlySpan<byte> request, out IDisposableBuffer reply);

        /// <summary>
        /// Blockingly performs a checkpoint. Invokes the supplied callback when contents of checkpoint are on
        /// persistent storage and recoverable. This function is allowed to return as soon as checkpoint content
        /// is finalized, but before contents are persistent. libDPR will not interleave batch operation, other
        /// checkpoint requests, or restore requests with this function. 
        /// </summary>
        /// <param name="onPersist">Callback to invoke when checkpoint is recoverable</param>
        void PerformCheckpoint(Action<TToken> onPersist);

        /// <summary>
        /// Blockingly recovers to a previous checkpoint as identified by the token. The function returns only after
        /// the state is restored for all future calls. libDPR will not interleave batch operation, other
        /// checkpoint requests, or restore requests with this function.
        /// </summary>
        /// <param name="token">Checkpoint to recover to</param>
        void RestoreCheckpoint(TToken token);
    }
}