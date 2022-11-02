using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using FASTER.common;
using FASTER.libdpr;

namespace FASTER.client
{
    public class DarqBackgroundWorker : IDisposable
    {
        private DprServer<Darq> darqServer;
        private SimpleObjectPool<DarqMessage> messagePool;
        private ManualResetEventSlim terminationStart, terminationComplete;
        private IDarqClusterInfo clusterInfo;
        private DprClientSession clientSession;
        private DarqScanIterator iterator;
        private DarqProducerClient producerClient;
        private DarqCompletionTracker completionTracker;
        private long processedUpTo;
        private int batchSize, numBatched = 0;


        /// <summary>
        /// Constructs a new ColocatedDarqProcessorClient
        /// </summary>
        /// <param name="darqServer">DARQ DprServer that this consumer attaches to </param>
        /// <param name="clusterInfo"> information about the DARQ cluster </param>
        public DarqBackgroundWorker(DprServer<Darq> darqServer, IDarqClusterInfo clusterInfo, int batchSize = 16)
        {
            this.darqServer = darqServer;
            this.clusterInfo = clusterInfo;
            messagePool = new SimpleObjectPool<DarqMessage>(() => new DarqMessage(messagePool), 1 << 15);
            this.batchSize = batchSize;
        }

        public long ProcessingLag => darqServer.StateObject().log.TailAddress - processedUpTo;

        public void Dispose()
        {
            iterator?.Dispose();
            producerClient?.Dispose();
        }

        private unsafe bool TryReadEntry(out DarqMessage message, out DprBatchStatus status)
        {
            message = null;
            long nextAddress = 0;
            status = DprBatchStatus.OK;
            try
            {
                darqServer.RequestLocalBatchBegin(false);
                if (darqServer.WorldLine() > clientSession.WorldLine)
                {
                    status = DprBatchStatus.ROLLBACK;
                    return true;
                }

                if (!iterator.UnsafeGetNext(out var entry, out var entryLength,
                    out var lsn, out processedUpTo, out var type))
                    return false;

                completionTracker.AddEntry(lsn, processedUpTo);
                // Short circuit without looking at the entry -- no need to process in background
                if (type != DarqMessageType.OUT && type != DarqMessageType.COMPLETION)
                {
                    iterator.UnsafeRelease();
                    return true;
                }

                var wv = new WorkerVersion(darqServer.Me(),
                    darqServer.StateObject().VersionScheme().CurrentState().Version);
                // Copy out the entry before dropping protection
                message = messagePool.Checkout();
                message.Reset(type, lsn, processedUpTo, wv,
                    new ReadOnlySpan<byte>(entry, entryLength));
                iterator.UnsafeRelease();
            }
            finally
            {
                darqServer.SignalLocalBatchFinish(null, null);
            }

            return true;
        }

        private int numSent = 0;

        private unsafe void SendMessage(DarqMessage m)
        {
            Debug.Assert(m.GetMessageType() == DarqMessageType.OUT);
            fixed (byte* h = m.GetMessageBody)
            {
                var dest = *(Worker*) h;
                var body = new ReadOnlySpan<byte>(h + sizeof(Worker),
                    m.GetMessageBody.Length - sizeof(Worker));
                var completionTrackerLocal = completionTracker;
                var lsn = m.GetLsn();
                if (++numBatched < batchSize)
                {
                    producerClient.EnqueueMessageWithCallback(dest, body, darqServer.Me().guid, lsn,
                        _ => { completionTrackerLocal.RemoveEntry(lsn); }, forceFlush: false);
                }
                else
                {
                    numBatched = 0;
                    producerClient.EnqueueMessageWithCallback(dest, body, darqServer.Me().guid, lsn,
                        _ => { completionTrackerLocal.RemoveEntry(lsn); }, forceFlush: true);
                }

                numSent++;
                if (numSent % 100000 == 0)
                    Console.WriteLine($"background worker sent {numSent} messages");
            }

            m.Dispose();
        }

        private bool TryConsumeNext()
        {
            var hasNext = TryReadEntry(out var m, out var dprBatchStatus);
            Debug.Assert(dprBatchStatus != DprBatchStatus.IGNORE);

            if (dprBatchStatus == DprBatchStatus.ROLLBACK)
            {
                Console.WriteLine("Processor detected rollback, restarting");
                Reset();
                // Reset to next iteration without doing anything
                return true;
            }

            if (!hasNext)
                return false;

            if (m == null) return true;

            switch (m.GetMessageType())
            {
                case DarqMessageType.OUT:
                {
                    SendMessage(m);
                    break;
                }
                case DarqMessageType.COMPLETION:
                {
                    unsafe
                    {
                        fixed (byte* h = m.GetMessageBody)
                        {
                            for (var completed = (long*) h; completed < h + m.GetMessageBody.Length; completed++)
                                completionTracker.RemoveEntry(*completed);
                        }
                    }
                    completionTracker.RemoveEntry(m.GetLsn());
                    m.Dispose();
                    break;
                }
                default:
                    throw new NotImplementedException();
            }

            if (completionTracker.GetTruncateHead() > darqServer.StateObject().log.BeginAddress)
            {
                Console.WriteLine($"Truncating log until {completionTracker.GetTruncateHead()}");
                darqServer.StateObject().TruncateUntil(completionTracker.GetTruncateHead());
            }

            return true;
        }

        private void Reset()
        {
            clientSession = new DprClientSession(darqServer.WorldLine());
            producerClient = new DarqProducerClient(clusterInfo, clientSession);
            completionTracker = new DarqCompletionTracker();
            iterator = darqServer.StateObject().StartScan();
        }

        public async Task StartProcessing()
        {
            try
            {
                var terminationToken = new ManualResetEventSlim();
                if (Interlocked.CompareExchange(ref terminationStart, terminationToken, null) != null)
                    // already started
                    return;
                terminationComplete = new ManualResetEventSlim();

                Reset();
                Console.WriteLine($"Starting background send from address {darqServer.StateObject().log.BeginAddress}");
                while (!terminationStart.IsSet)
                {
                    while (TryConsumeNext())
                    {
                    }

                    // darqServer.StateObject().RefreshSafeReadTail();
                    producerClient.ForceFlush();
                    var iteratorWait = iterator.WaitAsync().AsTask();
                    if (await Task.WhenAny(iteratorWait, Task.Delay(5)) == iteratorWait)
                    {
                        // No more entries, can signal finished and return 
                        if (!iteratorWait.Result) break;
                    }
                    // Otherwise, just continue looping
                }

                producerClient.ForceFlush();
                terminationComplete.Set();
            }
            catch (Exception e)
            {
                // Just restart the failed background thread
                terminationComplete.Set();
                terminationStart = null;
                await StartProcessing();
            }
        }

        public async Task StopProcessingAsync()
        {
            var t = terminationStart;
            var c = terminationComplete;
            if (t == null) return;

            t.Set();
            while (!c.IsSet)
                await Task.Delay(10);
            terminationStart = null;
        }
    }
}