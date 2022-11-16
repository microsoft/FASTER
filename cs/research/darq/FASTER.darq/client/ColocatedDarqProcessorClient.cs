using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using FASTER.common;
using FASTER.core;
using FASTER.libdpr;

namespace FASTER.darq
{
    /// <summary>
    /// A DarqConsumer that runs in the same process as a DARQ instance
    /// </summary>
    public class ColocatedDarqProcessorClient : IDarqProcessorClient
    {
        private Darq darq;
        private SimpleObjectPool<DarqMessage> messagePool;
        private ManualResetEventSlim terminationStart, terminationComplete;
        private bool shouldSnapshotDpr;
        // TODO(Tianyu): For benchmark purposes only
        public Stopwatch sw = new ();

        // TODO(Tianyu): Reason about behavior in the case of rollback
        private long incarnation;
        private DarqScanIterator iterator;
        private Capabilities capabilities;
        private bool unsafeMode = false;

        private enum ProcessResult
        {
            CONTINUE,
            NO_ENTRY,
            TERMINATED
        }

        private class Capabilities : IDarqProcessorClientCapabilities
        {
            internal ColocatedDarqProcessorClient parent;
            internal DprSession session;

            public Capabilities(ColocatedDarqProcessorClient parent, DprSession session)
            {
                this.parent = parent;
                this.session = session;
            }

            public unsafe ValueTask<StepStatus> Step(StepRequest request)
            {
                Span<byte> header = default;
                // If step results in a version mismatch, rely on the scan to trigger a rollback for simplicity
                if (parent.unsafeMode)
                {
                    // TODO(Tianyu): magic number
                    var headerBytes = stackalloc byte[120];
                    header = new Span<byte>(headerBytes, 120);
                    session.ComputeHeaderForSend(header);
                    if (!parent.darq.ReceiveAndBeginProcessing(header))
                        return new ValueTask<StepStatus>(StepStatus.REINCARNATED);
                }
                else
                {
                    parent.darq.BeginProcessing();

                    if (parent.darq.WorldLine() != session.WorldLine)
                        return new ValueTask<StepStatus>(StepStatus.REINCARNATED);
                }


                var status = parent.darq.Step(parent.incarnation, request);
                request.Dispose();
                parent.darq.FinishProcessing();
                return new ValueTask<StepStatus>(status);
            }

            public DprSession StartUsingDprSessionExternally()
            {
                parent.unsafeMode = true;
                unsafe
                {
                    // Bring dependency of the underlying client session up to date as it was not used before
                    var headerBytes = stackalloc byte[DprBatchHeader.FixedLenSize];
                    var header = new Span<byte>(headerBytes, 120);
                    // TODO(Tianyu): hacky
                    parent.darq.BeginProcessing();
                    parent.darq.FinishProcessingAndSend(header);
                    var result = session.ReceiveHeader(header, out _);
                    Debug.Assert(result == DprBatchStatus.OK);
                }
                return session;
            }

            public void StopUsingDprSessionExternally()
            {
                parent.unsafeMode = false;
            }
        }

        /// <summary>
        /// Constructs a new ColocatedDarqProcessorClient
        /// </summary>
        /// <param name="darq">DARQ DprServer that this consumer attaches to </param>
        /// <param name="clusterInfo"> information about the DARQ cluster </param>
        public ColocatedDarqProcessorClient(Darq darq)
        {
            this.darq = darq;
            messagePool = new SimpleObjectPool<DarqMessage>(() => new DarqMessage(messagePool));
        }

        // public DprClientSession DprSession => clientSession;

        public void Dispose()
        {
            messagePool.Dispose();
            iterator.Dispose();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private unsafe bool TryReadEntry(out DarqMessage message, out DprBatchStatus status)
        {
            message = null;
            status = DprBatchStatus.OK;
            // TODO(Tianyu): magic number
            var headerBytes = stackalloc byte[DprBatchHeader.FixedLenSize];
            try
            {
                darq.BeginProcessing();
                if (!unsafeMode)
                {
                    // Manually check if worldLine matches without going through the heavyweight DPR path
                    if (darq.WorldLine() > capabilities.session.WorldLine)
                    {
                        status = DprBatchStatus.ROLLBACK;
                        return true;
                    }
                }

                if (!iterator.UnsafeGetNext(out var entry, out var entryLength,
                        out var lsn, out var nextLsn, out var type))
                    return false;

                // Short circuit without looking at the entry -- no need to process in background
                if (type != DarqMessageType.IN && type != DarqMessageType.SELF)
                {
                    iterator.UnsafeRelease();
                    return true;
                }

                var wv = new WorkerVersion(darq.Me(), darq.Version());
                // Copy out the entry before dropping protection
                message = messagePool.Checkout();
                message.Reset(type, lsn, nextLsn, wv, new ReadOnlySpan<byte>(entry, entryLength));
                iterator.UnsafeRelease();
            }
            finally
            {
                if (unsafeMode)
                {
                    var header = new Span<byte>(headerBytes, DprBatchHeader.FixedLenSize);
                    darq.FinishProcessingAndSend(header);
                    status = capabilities.session.ReceiveHeader(header, out _);
                }
                else
                {
                    darq.FinishProcessing();
                }
            }

            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private ProcessResult TryConsumeNext<T>(T processor) where T : IDarqProcessor
        {
            var hasNext = TryReadEntry(out var m, out var dprBatchStatus);

            if (dprBatchStatus is DprBatchStatus.IGNORE)
            {
                // TODO(Tianyu): should trigger DARQ rollback here, but difficult due to concurrency
                throw new NotImplementedException();
            }

            if (dprBatchStatus is DprBatchStatus.ROLLBACK)
            {
                Console.WriteLine("Processor detected rollback, restarting");
                OnProcessorClientRestart(processor);
                // Reset to next iteration without doing anything
                return ProcessResult.CONTINUE;
            }

            if (!hasNext)
                return ProcessResult.NO_ENTRY;

            // Not a message we need to worry about
            if (m == null) return ProcessResult.CONTINUE;

            if (!sw.IsRunning) sw.Start();
            switch (m.GetMessageType())
            {
                case DarqMessageType.IN:
                case DarqMessageType.SELF:
                    if (processor.ProcessMessage(m))
                        return ProcessResult.CONTINUE;
                    return ProcessResult.TERMINATED;
                default:
                    throw new NotImplementedException();
            }
        }

        private void OnProcessorClientRestart<T>(T processor) where T : IDarqProcessor
        {
            capabilities = new Capabilities(this, new DprSession(darq.WorldLine()));
            processor.OnRestart(capabilities);
            iterator = darq.StartScan(true);
        }

        /// <inheritdoc/>
        public void StartProcessing<T>(T processor) where T : IDarqProcessor
        {
            StartProcessingAsync(processor).GetAwaiter().GetResult();
        }

        /// <inheritdoc/>
        public async Task StartProcessingAsync<T>(T processor)
            where T : IDarqProcessor
        {
            try
            {
                var terminationToken = new ManualResetEventSlim();
                if (Interlocked.CompareExchange(ref terminationStart, terminationToken, null) != null)
                    // already started
                    throw new FasterException("Attempting to start a processor twice");
                terminationComplete = new ManualResetEventSlim();

                incarnation = darq.RegisterNewProcessor();
                OnProcessorClientRestart(processor);
                Console.WriteLine("Starting Processor...");
                while (!terminationToken.IsSet)
                {
                    ProcessResult result;
                    do
                    {
                        result = TryConsumeNext(processor);
                    } while (result == ProcessResult.CONTINUE);

                    if (result == ProcessResult.TERMINATED)
                        break;

                    // FASTER.darq.StateObject().RefreshSafeReadTail();
                    var iteratorWait = iterator.WaitAsync().AsTask();
                    if (await Task.WhenAny(iteratorWait, Task.Delay(10)) == iteratorWait)
                    {
                        // No more entries, can signal finished and return 
                        if (!iteratorWait.Result)
                            break;
                    }
                    // Otherwise, just continue looping
                }

                Console.WriteLine("Colocated processor has exited");
                terminationComplete.Set();
            }
            catch (Exception e)
            {
                Console.WriteLine("C# why you eat exceptions");
                Console.WriteLine(e.Message);
                Console.WriteLine(e.StackTrace);
            }
        }

        /// <inheritdoc/>
        public void StopProcessing()
        {
            StopProcessingAsync().GetAwaiter().GetResult();
        }

        /// <inheritdoc/>
        public async Task StopProcessingAsync()
        {
            var t = terminationStart;
            var c = terminationComplete;
            if (t == null) return;
            t.Set();
            while (!c.IsSet)
                await Task.Delay(10);
            terminationStart = null;
            iterator.Dispose();
            if (sw.IsRunning) sw.Stop();
        }
    }
}