using System;
using System.Runtime.InteropServices;
using System.Threading;

namespace FASTER.libdpr
{
    /// <summary>
    /// Wraps DPR components together for easy interface of a SimpleStateObject with a network layer. Do not
    /// expect this to be performant.
    /// </summary>
    /// <typeparam name="TStateObject">Underlying SimpleStateObject</typeparam>
    /// <typeparam name="TToken">Type of token that uniquely identifies a checkpoint</typeparam>
    public class DprWrapper<TStateObject, TToken>
        where TStateObject : ISimpleStateObject<TToken>
    {
        private TStateObject underlying;
        private SimpleStateObjectAdapter<TStateObject, TToken> wrapped;
        private DprManager<SimpleStateObjectAdapter<TStateObject, TToken>, TToken> dprManager;
        private ReaderWriterLockSlim opLatch;
        
        public DprWrapper(TStateObject underlying, IDprFinder dprFinder, Worker me, long checkpointMilli)
        {
            this.underlying = underlying;
            opLatch = new ReaderWriterLockSlim();
            wrapped = new SimpleStateObjectAdapter<TStateObject, TToken>(underlying, opLatch);
            dprManager = new DprManager<SimpleStateObjectAdapter<TStateObject, TToken>, TToken>(
                dprFinder, me, wrapped, checkpointMilli);
        }

        public void Start() => dprManager.Start();

        public void End() => dprManager.End();

        /// <summary>
        /// Processes a batch while handling all DPR-related work.
        /// </summary>
        /// <param name="request">Client request</param>
        /// <param name="dprRequest">DPR header that came with the request</param>
        /// <param name="dprResponse">DPR response header to write to</param>
        /// <returns>Response to write to client</returns>
        public IDisposableBuffer Process(ReadOnlySpan<byte> request, ReadOnlySpan<byte> dprRequest, Span<byte> dprResponse)
        {
            ref var castRequest = ref MemoryMarshal.GetReference(MemoryMarshal.Cast<byte, DprBatchRequestHeader>(dprRequest));
            // If batch rejected, simply send back dprHeader without executing
            if (!dprManager.RequestBatchBegin(dprRequest, dprResponse, out var tracker)) return null;
            
            // Otherwise, send to underlying state object for processing while preventing concurrent checkpoints
            opLatch.EnterReadLock();
            var success = underlying.ProcessBatch(request, out var result);
            // If the underlying state object decided to reject the batch, fill in 0's to denote this. Otherwise check
            // the wrapper for the correct version under latch protection
            tracker.MarkOperationRangesVersion(0, castRequest.numMessages, success ? wrapped.Version() : 0);
            opLatch.ExitReadLock();
            return result;
        }
    }
}