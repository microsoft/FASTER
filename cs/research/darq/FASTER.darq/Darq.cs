using System.Collections.Concurrent;
using System.Diagnostics;
using FASTER.common;
using FASTER.core;
using FASTER.libdpr;

namespace FASTER.darq
{
    public enum StepStatus
    {
        INCOMPLETE,
        SUCCESS,
        INVALID,
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

    public class DarqStateObject : IStateObject, IDisposable
    {
        internal DarqSettings settings;
        internal FasterLog log;
        internal ConcurrentDictionary<long, byte> incompleteMessages = new();
        private FasterLogSettings logSetting;

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
                // DARQ should never do anything through a code path that needs to materialize into external mem buffer
                GetMemory = _ => throw new NotImplementedException(),
                LogChecksum = settings.LogChecksum,
                MutableFraction = settings.MutableFraction,
                ReadOnlyMode = false,
                FastCommitMode = settings.FastCommitMode,
                RemoveOutdatedCommits = false,
                LogCommitPolicy = null,
                TryRecoverLatest = false,
                AutoRefreshSafeTailAddress = settings.Speculative,
                AutoCommit = false,
                TolerateDeviceFailure = false
            };
            log = new FasterLog(logSetting);
        }

        public void Dispose()
        {
            if (settings.DeleteOnClose)
                settings.LogCommitManager.RemoveAllCommits();
            log.Dispose();
            settings.LogDevice.Dispose();
            settings.LogCommitManager.Dispose();
        }

        public void PruneVersion(long version)
        {
            settings.LogCommitManager.RemoveCommit(version);
        }

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

        public void PerformCheckpoint(long version, ReadOnlySpan<byte> metadata, Action onPersist)
        {
            var commitCookie = metadata.ToArray();
            log.CommitStrongly(out var tail, out _, false, commitCookie, version, onPersist);
        }

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

    public class Darq : DprWorker<DarqStateObject>, IDisposable
    {
        private readonly DeduplicationVector dvc;
        private readonly LongValueAttachment incarnation, largestSteppedLsn;

        private WorkQueueLIFO<StepRequestHandle> stepQueue;

        private ThreadLocalObjectPool<StepRequestHandle> stepRequestPool;

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


        public void Dispose()
        {
            StateObject().Dispose();
        }

        private void EnqueueCallback(IReadOnlySpanBatch m, int idx, long addr)
        {
            StateObject().incompleteMessages.TryAdd(addr, 0);
        }

        public unsafe bool EnqueueInputBatch(IReadOnlySpanBatch entries, WorkerId inputId, long inputLsn)
        {
            for (var i = 0; i < entries.TotalEntries(); i++)
            {
                fixed (byte* h = entries.Get(i))
                {
                    Debug.Assert((DarqMessageType)(*h) == DarqMessageType.IN);
                }
            }

            // Check that we are not executing duplicates and update dvc accordingly
            if (inputId.guid != -1 && !dvc.Process(inputId, inputLsn))
                return false;

            // TODO(Tianyu): Need to resolve unsafe scan race
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
            // The last entry of the step must be a completion record that steps some previous message
            var lastEntry = stepRequestHandle.stepMessages.Get(numTotalEntries - 1);
            fixed (byte* h = lastEntry)
            {
                var end = h + lastEntry.Length;
                var messageType = (DarqMessageType)(*h);
                Debug.Assert(messageType == DarqMessageType.COMPLETION);
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
                        Console.WriteLine($"step unexpectedly failed on lsn {completedLsn}");
                        return;
                    }
                }
            }

            // TODO(Tianyu): Need to resolve unsafe scan race
            StateObject().log.Enqueue(stepRequestHandle.stepMessages, StepCallback);

            stepRequestHandle.done.Set();
            stepRequestHandle.status = StepStatus.SUCCESS;
        }

        public StepStatus Step(long incarnation, IReadOnlySpanBatch stepMessages)
        {
            var request = stepRequestPool.Checkout();
            request.Reset(incarnation, stepMessages);
            stepQueue.EnqueueAndTryWork(request, false);
            if (request.status == StepStatus.INCOMPLETE)
                request.done.Wait();
            var result = request.status;
            stepRequestPool.Return(request);
            return result;
        }

        public void TruncateUntil(long lsn)
        {
            StateObject().log.TruncateUntil(lsn);
        }

        public long RegisterNewProcessor()
        {
            var tcs = new TaskCompletionSource<long>();            
            // TODO(Tianyu): Can this deadlock against itself?
            versionScheme.GetUnderlyingEpoch().BumpCurrentEpoch(() => tcs.SetResult(Interlocked.Increment(ref incarnation.value)));
            return tcs.Task.GetAwaiter().GetResult();
        }

        public DarqScanIterator StartScan() => new DarqScanIterator(StateObject().log, largestSteppedLsn.value, StateObject().settings.Speculative);
    }
}