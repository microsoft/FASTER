using System;
using System.Runtime.InteropServices;
using System.Threading;

namespace FASTER.libdpr
{
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
        
        // TODO(Tianyu): Add in appendable batch abstraction
        public Span<byte> Process(ReadOnlySpan<byte> request, ReadOnlySpan<byte> dprRequest, Span<byte> dprResponse)
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