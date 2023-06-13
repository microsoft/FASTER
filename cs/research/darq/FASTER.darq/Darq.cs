using System.Collections.Concurrent;
using System.Diagnostics;
using FASTER.common;
using FASTER.core;
using FASTER.libdpr;

namespace FASTER.darq
{
    /// <summary>
    /// Status of a step 
    /// </summary>
    public enum StepStatus
    {
        /// <summary>
        /// Step is not yet completed
        /// </summary>
        INCOMPLETE,

        /// <summary>
        /// Step is successfully completed
        /// </summary>
        SUCCESS,

        /// <summary>
        ///  Step cannot be completed because it is either ill-formed or because it is trying to consume
        ///  consumed messages
        /// </summary>
        INVALID,

        /// <summary>
        /// The step cannot be completed because it originated from a processor that is no longer allowed to
        /// update DARQ state (possibly due to another, newer processor taking over)
        /// </summary>
        REINCARNATED
    }

    internal class StepRequestHandle
    {
        internal volatile StepStatus status;
        internal long incarnation;
        internal IReadOnlySpanBatch stepMessages;
        internal ManualResetEventSlim done = new();

        internal void Reset(long incarnation, IReadOnlySpanBatch stepMessages)
        {
            this.incarnation = incarnation;
            status = StepStatus.INCOMPLETE;
            this.stepMessages = stepMessages;
            done.Reset();
        }
    }

    internal class LongValueAttachment : IStateObjectAttachment
    {
        internal long value;

        public int SerializedSize() => sizeof(long);

        public void SerializeTo(Span<byte> buffer)
        {
            BitConverter.TryWriteBytes(buffer, value);
        }

        public void RecoverFrom(ReadOnlySpan<byte> serialized)
        {
            unsafe
            {
                fixed (byte* b = serialized)
                    value = *(long*)b;
            }
        }
    }

    /// <summary>
    /// Underlying state object for DARQ
    /// </summary>
    public class DarqStateObject : IStateObject, IDisposable
    {
        internal DarqSettings settings;
        internal FasterLog log;
        internal ConcurrentDictionary<long, byte> incompleteMessages = new();
        private FasterLogSettings logSetting;

        /// <summary>
        ///  Constructs a new DarqStateObject using the given parameters
        /// </summary>
        /// <param name="settings">parameters for the DarqStateObject</param>
        public DarqStateObject(DarqSettings settings)
        {
            this.settings = settings;
            if (settings.LogDevice == null)
                throw new FasterException("Cannot initialize DARQ as no underlying device is specified. " +
                                          "Please supply DARQ with a device under DarqSettings.LogDevice");

            if (settings.LogCommitManager == null)
            {
                settings.LogCommitManager = new DeviceLogCommitCheckpointManager
                (new LocalStorageNamedDeviceFactory(),
                    new DefaultCheckpointNamingScheme(
                        settings.LogCommitDir ??
                        new FileInfo(settings.LogDevice.FileName).Directory.FullName));
            }

            logSetting = new FasterLogSettings
            {
                LogDevice = settings.LogDevice,
                PageSize = settings.PageSize,
                MemorySize = settings.MemorySize,
                SegmentSize = settings.SegmentSize,
                LogCommitManager = settings.LogCommitManager,
                LogCommitDir = settings.LogCommitDir,
                GetMemory = _ =>
                    throw new FasterException(
                        "DARQ should never do anything through a code path that needs to materialize into external mem buffer"),
                LogChecksum = settings.LogChecksum,
                MutableFraction = settings.MutableFraction,
                ReadOnlyMode = false,
                FastCommitMode = settings.FastCommitMode,
                RemoveOutdatedCommits = false,
                LogCommitPolicy = null,
                TryRecoverLatest = false,
                AutoRefreshSafeTailAddress = true,
                AutoCommit = false,
                TolerateDeviceFailure = false,
            };
            log = new FasterLog(logSetting);
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            if (settings.DeleteOnClose)
                settings.LogCommitManager.RemoveAllCommits();
            log.Dispose();
            settings.LogDevice.Dispose();
            settings.LogCommitManager.Dispose();
        }

        /// <inheritdoc/>
        public void PruneVersion(long version)
        {
            settings.LogCommitManager.RemoveCommit(version);
        }

        /// <inheritdoc/>
        public IEnumerable<(byte[], int)> GetUnprunedVersions()
        {
            var commits = settings.LogCommitManager.ListCommits().ToList();
            return commits.Select(commitNum =>
            {
                // TODO(Tianyu): hacky
                var newLog = new FasterLog(logSetting);
                newLog.Recover(commitNum);
                var commitCookie = newLog.RecoveredCookie;
                newLog.Dispose();
                return ValueTuple.Create(commitCookie, 0);
            });
        }

        /// <inheritdoc/>
        public void PerformCheckpoint(long version, ReadOnlySpan<byte> metadata, Action onPersist)
        {
            var commitCookie = metadata.ToArray();
            log.CommitStrongly(out var tail, out _, false, commitCookie, version, onPersist);
        }

        /// <inheritdoc/>
        public void RestoreCheckpoint(long version, out ReadOnlySpan<byte> metadata)
        {
            Console.WriteLine($"Restoring checkpoint {version}");
            incompleteMessages.Clear();

            // TODO(Tianyu): can presumably be more efficient through some type of in-mem truncation here
            log = new FasterLog(logSetting);
            log.Recover(version);
            metadata = log.RecoveredCookie;

            Console.WriteLine($"Log recovered, now restoring in-memory DARQ data structures");
            // Scan the log on recovery to repopulate in-memory auxiliary data structures
            unsafe
            {
                using var it = log.Scan(0, long.MaxValue);
                while (it.UnsafeGetNext(out byte* entry, out var len, out var lsn, out _))
                {
                    switch ((DarqMessageType)(*entry))
                    {
                        case DarqMessageType.IN:
                        case DarqMessageType.SELF:
                            incompleteMessages.TryAdd(lsn, 0);
                            break;
                        case DarqMessageType.COMPLETION:
                            var completed = (long*)(entry + sizeof(DarqMessageType));
                            while (completed < entry + len)
                                incompleteMessages.TryRemove(*completed++, out _);
                            break;
                        case DarqMessageType.OUT:
                            break;
                        default:
                            throw new NotImplementedException();
                    }

                    it.UnsafeRelease();
                }
            }

            Console.WriteLine($"Recovery Finished");
        }
    }

    /// <summary>
    /// DARQ data structure 
    /// </summary>
    public class Darq : DprWorker<DarqStateObject>, IDisposable
    {
        private readonly DeduplicationVector dvc;
        private readonly LongValueAttachment incarnation, largestSteppedLsn;
        private WorkQueueLIFO<StepRequestHandle> stepQueue;
        private ThreadLocalObjectPool<StepRequestHandle> stepRequestPool;

        /// <summary>
        /// Initialize DARQ with the given identity and parameters
        /// </summary>
        /// <param name="me">unique identity for this DARQ</param>
        /// <param name="darqSettings">parameters for DARQ</param>
        public Darq(WorkerId me, DarqSettings darqSettings) : base(me,
            new DarqStateObject(darqSettings), darqSettings.DprFinder)
        {
            dvc = new DeduplicationVector();
            incarnation = new LongValueAttachment();
            largestSteppedLsn = new LongValueAttachment();
            AddAttachment(dvc);
            AddAttachment(incarnation);
            AddAttachment(largestSteppedLsn);

            stepQueue = new WorkQueueLIFO<StepRequestHandle>(StepSequential);
            stepRequestPool = new ThreadLocalObjectPool<StepRequestHandle>(() => new StepRequestHandle());
        }

        /// <summary>
        /// Return the tail address that this DARQ will need to replay to upon failure recovery
        /// </summary>
        public long ReplayEnd => largestSteppedLsn.value;

        /// <summary>   
        /// Whether this DARQ is configured to be speculative
        /// </summary>
        public bool Speculative => dprFinder != null;

        /// <inheritdoc/>
        public void Dispose()
        {
            StateObject().Dispose();
        }

        private void EnqueueCallback(IReadOnlySpanBatch m, int idx, long addr)
        {
            StateObject().incompleteMessages.TryAdd(addr, 0);
        }

        /// <summary>
        /// Enqueue given entries into DARQ, optionally deduplicated using the supplied producer ID and sequence number. 
        /// </summary>
        /// <param name="entries">
        /// Entries to enqueue. must already be well-formed on a byte level with message types, etc.
        /// </param>
        /// <param name="producerId"> Unique id of the producer for deduplication, or -1 if not required</param>
        /// <param name="sequenceNum">
        /// sequence number for deduplication. DARQ will only accept enqueue requests with monotonically increasing
        /// sequence numbers from the same producer
        /// </param>
        /// <returns> whether enqueue is successful </returns>
        public bool EnqueueInputBatch(IReadOnlySpanBatch entries, WorkerId producerId, long sequenceNum)
        {
#if DEBUG
            unsafe
            {
                for (var i = 0; i < entries.TotalEntries(); i++)
                {
                    fixed (byte* h = entries.Get(i))
                    {
                        Debug.Assert((DarqMessageType)(*h) == DarqMessageType.IN);
                    }
                }
            }
#endif
            // Check that we are not executing duplicates and update dvc accordingly
            if (producerId.guid != -1 && !dvc.Process(producerId, sequenceNum))
                return false;

            StateObject().log.Enqueue(entries, EnqueueCallback);
            return true;
        }

        private void StepCallback(IReadOnlySpanBatch ms, int idx, long addr)
        {
            var entry = ms.Get(idx);
            // Get first byte for type
            if ((DarqMessageType)entry[0] == DarqMessageType.SELF ||
                (DarqMessageType)entry[0] == DarqMessageType.IN)
                StateObject().incompleteMessages.TryAdd(addr, 0);

            largestSteppedLsn.value = addr;
        }

        private unsafe void StepSequential(StepRequestHandle stepRequestHandle)
        {
            // Maintain incarnation number
            if (stepRequestHandle.incarnation != incarnation.value)
            {
                stepRequestHandle.status = StepStatus.REINCARNATED;
                stepRequestHandle.done.Set();
                return;
            }

            Debug.Assert(incarnation.value == stepRequestHandle.incarnation);

            // Validation of input batch
            var numTotalEntries = stepRequestHandle.stepMessages.TotalEntries();
            // Validate if The last entry of the step is a completion record that steps some previous message
            var lastEntry = stepRequestHandle.stepMessages.Get(numTotalEntries - 1);
            fixed (byte* h = lastEntry)
            {
                var end = h + lastEntry.Length;
                var messageType = (DarqMessageType)(*h);
                if (messageType == DarqMessageType.COMPLETION)
                {
                    Debug.Assert(lastEntry.Length % sizeof(long) == 1);
                    for (var head = h + sizeof(DarqMessageType); head < end; head += sizeof(long))
                    {
                        var completedLsn = *(long*)head;
                        if (!StateObject().incompleteMessages.TryRemove(completedLsn, out _))
                        {
                            // This means we are trying to step something twice. Roll back all previous steps before
                            // failing this step
                            for (var rollbackHead = h + sizeof(DarqMessageType);
                                 rollbackHead < head;
                                 rollbackHead += sizeof(long))
                                StateObject().incompleteMessages.TryAdd(*(long*)rollbackHead, 0);
                            stepRequestHandle.status = StepStatus.INVALID;
                            stepRequestHandle.done.Set();
                            Console.WriteLine($"step failed on lsn {completedLsn}");
                            return;
                        }
                    }
                }
            }
            StateObject().log.Enqueue(stepRequestHandle.stepMessages, StepCallback);
            stepRequestHandle.done.Set();
            stepRequestHandle.status = StepStatus.SUCCESS;
        }

        /// <summary>
        /// Step the DARQ with given incarnation number and step content
        /// </summary>
        /// <param name="incarnation"> incarnation number of the originating processor </param>
        /// <param name="stepMessages">
        /// Step content. must already be well-formed on a byte level with message
        /// types, etc. with the last entry being a completion record
        /// </param>
        /// <returns>step result</returns>
        public StepStatus Step(long incarnation, IReadOnlySpanBatch stepMessages)
        {
            var request = stepRequestPool.Checkout();
            request.Reset(incarnation, stepMessages);
            stepQueue.EnqueueAndTryWork(request, false);
            while (request.status == StepStatus.INCOMPLETE)
                request.done.Wait();
            var result = request.status;
            stepRequestPool.Return(request);
            return result;
        }

        /// <summary>
        /// Truncate DARQ until the given lsn
        /// </summary>
        /// <param name="lsn">truncation point</param>
        public void TruncateUntil(long lsn)
        {
            StateObject().log.TruncateUntil(lsn);
        }

        /// <summary>
        /// Registers a new processor the submit steps to this DARQ.
        /// </summary>
        /// <returns>the unique incarnation number assigned to this processor</returns>
        public long RegisterNewProcessor()
        {
            var tcs = new TaskCompletionSource<long>();
            // TODO(Tianyu): Can this deadlock against itself?
            versionScheme.GetUnderlyingEpoch()
                .BumpCurrentEpoch(() => tcs.SetResult(Interlocked.Increment(ref incarnation.value)));
            return tcs.Task.GetAwaiter().GetResult();
        }

        /// <summary>
        /// Scans the DARQ with an iterator 
        /// </summary>
        /// <param name="speculative">whether to speculatively scan </param>
        /// <returns></returns>
        public DarqScanIterator StartScan(bool speculative) =>
            new DarqScanIterator(StateObject().log, largestSteppedLsn.value, speculative);
    }
}