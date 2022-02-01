// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace FASTER.core
{
    internal enum ReadStatus { Pending, Done };
    internal enum FlushStatus { Pending, Done };

    internal class RecoveryStatus
    {
        public long endPage;
        public long snapshotEndPage;
        public long untilAddress;
        public int capacity;
        public CheckpointType checkpointType;

        public IDevice recoveryDevice;
        public long recoveryDevicePageOffset;
        public IDevice objectLogRecoveryDevice;

        // These are circular buffers of 'capacity' size; the indexing wraps due to hlog.GetPageIndexForPage().
        public ReadStatus[] readStatus;
        public FlushStatus[] flushStatus;

        private readonly SemaphoreSlim readSemaphore = new(0);
        private readonly SemaphoreSlim flushSemaphore = new(0);

        public RecoveryStatus(int capacity,
                              long endPage, long untilAddress, CheckpointType checkpointType)
        {
            this.capacity = capacity;
            this.endPage = endPage;
            this.untilAddress = untilAddress;
            this.checkpointType = checkpointType;

            readStatus = new ReadStatus[capacity];
            flushStatus = new FlushStatus[capacity];
            for (int i = 0; i < capacity; i++)
            {
                flushStatus[i] = FlushStatus.Done;
                readStatus[i] = ReadStatus.Pending;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void SignalRead(int pageIndex)
        {
            this.readStatus[pageIndex] = ReadStatus.Done;
            this.readSemaphore.Release();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void WaitRead(int pageIndex)
        {
            while (this.readStatus[pageIndex] == ReadStatus.Pending)
                this.readSemaphore.Wait();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal async ValueTask WaitReadAsync(int pageIndex, CancellationToken cancellationToken)
        {
            while (this.readStatus[pageIndex] == ReadStatus.Pending)
                await this.readSemaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void SignalFlushed(int pageIndex)
        {
            this.flushStatus[pageIndex] = FlushStatus.Done;
            this.flushSemaphore.Release();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void WaitFlush(int pageIndex)
        {
            while (this.flushStatus[pageIndex] == FlushStatus.Pending)
                this.flushSemaphore.Wait();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal async ValueTask WaitFlushAsync(int pageIndex, CancellationToken cancellationToken)
        {
            while (this.flushStatus[pageIndex] == FlushStatus.Pending)
                await this.flushSemaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
        }

        internal void Dispose()
        {
            recoveryDevice.Dispose();
            objectLogRecoveryDevice.Dispose();
        }
    }

    internal struct RecoveryOptions
    {
        internal long headAddress;
        internal long tailAddress;
        internal bool undoNextVersion;

        internal bool clearLocks => this.headAddress != Constants.kInvalidAddress;

        internal RecoveryOptions(bool clearLocks, long headAddress, long tailAddress, bool undoNextVer)
        {
            this.headAddress = clearLocks ? headAddress : Constants.kInvalidAddress;
            this.tailAddress = clearLocks ? tailAddress : Constants.kInvalidAddress;
            this.undoNextVersion = undoNextVer;
        }
    }

    public partial class FasterKV<Key, Value> : FasterBase, IFasterKV<Key, Value>
    {
        private void FindRecoveryInfo(long requestedVersion, out HybridLogCheckpointInfo recoveredHlcInfo,
            out IndexCheckpointInfo recoveredICInfo)
        {

            Debug.WriteLine("********* Primary Recovery Information ********");

            HybridLogCheckpointInfo current, closest = default;
            Guid closestToken = default;
            long closestVersion = long.MaxValue;
            byte[] cookie = default;
            
            // Traverse through all current tokens to find either the largest version or the version that's closest to
            // but smaller than the requested version. Need to iterate through all unpruned versions because file system
            // is not guaranteed to return tokens in order of freshness.
            foreach (var hybridLogToken in checkpointManager.GetLogCheckpointTokens())
            {
                try
                {
                    current = new HybridLogCheckpointInfo();
                    current.Recover(hybridLogToken, checkpointManager, hlog.LogPageSizeBits, 
                        out var currCookie, false);
                    var distanceToTarget = (requestedVersion == -1 ? long.MaxValue : requestedVersion) - current.info.version;
                    // This is larger than intended version, cannot recover to this.
                    if (distanceToTarget < 0) continue;
                    // We have found the exact version to recover to --- the above conditional establishes that the
                    // checkpointed version is <= requested version, and if next version is larger than requestedVersion,
                    // there cannot be any closer version. 
                    if (current.info.nextVersion > requestedVersion)
                    {
                        closest = current;
                        closestToken = hybridLogToken;
                        cookie = currCookie;
                        break;
                    }
                    
                    // Otherwise, write it down and wait to see if there's a closer one;
                    if (distanceToTarget < closestVersion)
                    {
                        closestVersion = distanceToTarget;
                        closest.Dispose();
                        closest = current;
                        closestToken = hybridLogToken;
                        cookie = currCookie;
                    }
                    else
                    {
                        current.Dispose();
                    }
                }
                catch
                {
                    continue;
                }

                Debug.WriteLine("HybridLog Checkpoint: {0}", hybridLogToken);
            }

            recoveredHlcInfo = closest;
            recoveredCommitCookie = cookie;
            if (recoveredHlcInfo.IsDefault())
                throw new FasterException("Unable to find valid HybridLog token");

            if (recoveredHlcInfo.deltaLog != null)
            {
                recoveredHlcInfo.Dispose();
                // need to actually scan delta log now
                recoveredHlcInfo.Recover(closestToken, checkpointManager, hlog.LogPageSizeBits, out _, true);
            }
            recoveredHlcInfo.info.DebugPrint();

            recoveredICInfo = default;
            foreach (var indexToken in checkpointManager.GetIndexCheckpointTokens())
            {
                try
                {
                    // Recovery appropriate context information
                    recoveredICInfo = new IndexCheckpointInfo();
                    recoveredICInfo.Recover(indexToken, checkpointManager);
                }
                catch
                {
                    continue;
                }

                if (!IsCompatible(recoveredICInfo.info, recoveredHlcInfo.info))
                {
                    recoveredICInfo = default;
                    continue;
                }

                Debug.WriteLine("Index Checkpoint: {0}", indexToken);
                recoveredICInfo.info.DebugPrint();
                break;
            }

            if (recoveredICInfo.IsDefault())
            {
                Debug.WriteLine("No index checkpoint found, recovering from beginning of log");
            }
        }

        private static bool IsCompatible(in IndexRecoveryInfo indexInfo, in HybridLogRecoveryInfo recoveryInfo)
        {
            var l1 = indexInfo.finalLogicalAddress;
            var l2 = recoveryInfo.finalLogicalAddress;
            return l1 <= l2;
        }

        private void InternalRecover(Guid indexToken, Guid hybridLogToken, int numPagesToPreload, bool undoNextVersion, long recoverTo)
        {
            GetRecoveryInfo(indexToken, hybridLogToken, out HybridLogCheckpointInfo recoveredHLCInfo, out IndexCheckpointInfo recoveredICInfo);
            if (recoverTo != -1 && recoveredHLCInfo.deltaLog == null)
            {
                throw new FasterException("Recovering to a specific version within a token is only supported for incremental snapshots");
            }
            InternalRecover(recoveredICInfo, recoveredHLCInfo, numPagesToPreload, undoNextVersion, recoverTo);
        }

        private ValueTask InternalRecoverAsync(Guid indexToken, Guid hybridLogToken, int numPagesToPreload, bool undoNextVersion, long recoverTo, CancellationToken cancellationToken)
        {
            GetRecoveryInfo(indexToken, hybridLogToken, out HybridLogCheckpointInfo recoveredHLCInfo, out IndexCheckpointInfo recoveredICInfo);
            return InternalRecoverAsync(recoveredICInfo, recoveredHLCInfo, numPagesToPreload, undoNextVersion, recoverTo, cancellationToken);
        }

        private void GetRecoveryInfo(Guid indexToken, Guid hybridLogToken, out HybridLogCheckpointInfo recoveredHLCInfo, out IndexCheckpointInfo recoveredICInfo)
        {
            Debug.WriteLine("********* Primary Recovery Information ********");
            Debug.WriteLine("Index Checkpoint: {0}", indexToken);
            Debug.WriteLine("HybridLog Checkpoint: {0}", hybridLogToken);


            // Recovery appropriate context information
            recoveredHLCInfo = new HybridLogCheckpointInfo();
            recoveredHLCInfo.Recover(hybridLogToken, checkpointManager, hlog.LogPageSizeBits, out recoveredCommitCookie, true);
            recoveredHLCInfo.info.DebugPrint();
            try
            {
                recoveredICInfo = new IndexCheckpointInfo();
                if (indexToken != default)
                {
                    recoveredICInfo.Recover(indexToken, checkpointManager);
                    recoveredICInfo.info.DebugPrint();
                }
            }
            catch
            {
                recoveredICInfo = default;
            }

            if (recoveredICInfo.IsDefault())
            {
                Debug.WriteLine("Invalid index checkpoint token, recovering from beginning of log");
            }
            else
            {
                // Check if the two checkpoints are compatible for recovery
                if (!IsCompatible(recoveredICInfo.info, recoveredHLCInfo.info))
                {
                    throw new FasterException("Cannot recover from (" + indexToken.ToString() + "," + hybridLogToken.ToString() + ") checkpoint pair!\n");
                }
            }
        }

        private void InternalRecover(IndexCheckpointInfo recoveredICInfo, HybridLogCheckpointInfo recoveredHLCInfo, int numPagesToPreload, bool undoNextVersion, long recoverTo)
        {
            if (!RecoverToInitialPage(recoveredICInfo, recoveredHLCInfo, out long recoverFromAddress))
                RecoverFuzzyIndex(recoveredICInfo);

            if (!SetRecoveryPageRanges(recoveredHLCInfo, numPagesToPreload, recoverFromAddress, out long tailAddress, out long headAddress, out long scanFromAddress))
                return;
            RecoveryOptions options = new(recoveredHLCInfo.info.manualLockingActive, headAddress, tailAddress, undoNextVersion);

            long readOnlyAddress;
            // Make index consistent for version v
            if (recoveredHLCInfo.info.useSnapshotFile == 0)
            {
                RecoverHybridLog(scanFromAddress, recoverFromAddress, recoveredHLCInfo.info.finalLogicalAddress, recoveredHLCInfo.info.nextVersion, CheckpointType.FoldOver, options);
                readOnlyAddress = tailAddress;
            }
            else
            {
                if (recoveredHLCInfo.info.flushedLogicalAddress < headAddress)
                    headAddress = recoveredHLCInfo.info.flushedLogicalAddress;

                // First recover from index starting point (fromAddress) to snapshot starting point (flushedLogicalAddress)
                RecoverHybridLog(scanFromAddress, recoverFromAddress, recoveredHLCInfo.info.flushedLogicalAddress, recoveredHLCInfo.info.nextVersion, CheckpointType.Snapshot, options);
                // Then recover snapshot into mutable region
                RecoverHybridLogFromSnapshotFile(recoveredHLCInfo.info.flushedLogicalAddress, recoverFromAddress, recoveredHLCInfo.info.finalLogicalAddress, recoveredHLCInfo.info.startLogicalAddress,
                                recoveredHLCInfo.info.snapshotFinalLogicalAddress, recoveredHLCInfo.info.nextVersion, recoveredHLCInfo.info.guid, options, recoveredHLCInfo.deltaLog, recoverTo);

                readOnlyAddress = recoveredHLCInfo.info.flushedLogicalAddress;
            }

            DoPostRecovery(recoveredICInfo, recoveredHLCInfo, tailAddress, ref headAddress, ref readOnlyAddress);
        }

        private async ValueTask InternalRecoverAsync(IndexCheckpointInfo recoveredICInfo, HybridLogCheckpointInfo recoveredHLCInfo, int numPagesToPreload, bool undoNextVersion, long recoverTo, CancellationToken cancellationToken)
        {
            if (!RecoverToInitialPage(recoveredICInfo, recoveredHLCInfo, out long recoverFromAddress))
                await RecoverFuzzyIndexAsync(recoveredICInfo, cancellationToken).ConfigureAwait(false);

            if (!SetRecoveryPageRanges(recoveredHLCInfo, numPagesToPreload, recoverFromAddress, out long tailAddress, out long headAddress, out long scanFromAddress))
                return;
            RecoveryOptions options = new(recoveredHLCInfo.info.manualLockingActive, headAddress, tailAddress, undoNextVersion);

            long readOnlyAddress;
            // Make index consistent for version v
            if (recoveredHLCInfo.info.useSnapshotFile == 0)
            {
                await RecoverHybridLogAsync(scanFromAddress, recoverFromAddress, recoveredHLCInfo.info.finalLogicalAddress, recoveredHLCInfo.info.nextVersion, CheckpointType.FoldOver,
                                            options, cancellationToken).ConfigureAwait(false);
                readOnlyAddress = tailAddress;
            }
            else
            {
                if (recoveredHLCInfo.info.flushedLogicalAddress < headAddress)
                    headAddress = recoveredHLCInfo.info.flushedLogicalAddress;

                // First recover from index starting point (fromAddress) to snapshot starting point (flushedLogicalAddress)
                await RecoverHybridLogAsync(scanFromAddress, recoverFromAddress, recoveredHLCInfo.info.flushedLogicalAddress, recoveredHLCInfo.info.nextVersion, CheckpointType.Snapshot,
                                           new RecoveryOptions(recoveredHLCInfo.info.manualLockingActive, headAddress, tailAddress, undoNextVersion), cancellationToken).ConfigureAwait(false);
                // Then recover snapshot into mutable region
                await RecoverHybridLogFromSnapshotFileAsync(recoveredHLCInfo.info.flushedLogicalAddress, recoverFromAddress, recoveredHLCInfo.info.finalLogicalAddress, recoveredHLCInfo.info.startLogicalAddress,
                                        recoveredHLCInfo.info.snapshotFinalLogicalAddress, recoveredHLCInfo.info.nextVersion, recoveredHLCInfo.info.guid, options, recoveredHLCInfo.deltaLog, recoverTo, cancellationToken).ConfigureAwait(false);

                readOnlyAddress = recoveredHLCInfo.info.flushedLogicalAddress;
            }

            DoPostRecovery(recoveredICInfo, recoveredHLCInfo, tailAddress, ref headAddress, ref readOnlyAddress);
        }

        private void DoPostRecovery(IndexCheckpointInfo recoveredICInfo, HybridLogCheckpointInfo recoveredHLCInfo, long tailAddress, ref long headAddress, ref long readOnlyAddress)
        {
            // Adjust head and read-only address post-recovery
            var _head = (1 + (tailAddress >> hlog.LogPageSizeBits) - hlog.GetCapacityNumPages()) << hlog.LogPageSizeBits;
            if (_head > headAddress)
                headAddress = _head;
            if (readOnlyAddress < headAddress)
                readOnlyAddress = headAddress;

            // Recover session information
            hlog.RecoveryReset(tailAddress, headAddress, recoveredHLCInfo.info.beginAddress, readOnlyAddress);
            _recoveredSessions = recoveredHLCInfo.info.continueTokens;
            _recoveredSessionNameMap = recoveredHLCInfo.info.sessionNameMap;
            maxSessionId = recoveredHLCInfo.info.maxSessionId;
            checkpointManager.OnRecovery(recoveredICInfo.info.token, recoveredHLCInfo.info.guid);
            recoveredHLCInfo.Dispose();
        }

        /// <summary>
        /// Compute recovery address and determine where to recover to
        /// </summary>
        /// <param name="recoveredICInfo">IndexCheckpointInfo</param>
        /// <param name="recoveredHLCInfo">HybridLogCheckpointInfo</param>
        /// <param name="recoverFromAddress">Address from which to perform recovery (undo v+1 records)</param>
        /// <returns>Whether we are recovering to the initial page</returns>
        private bool RecoverToInitialPage(IndexCheckpointInfo recoveredICInfo, HybridLogCheckpointInfo recoveredHLCInfo, out long recoverFromAddress)
        {
            // Ensure active state machine to null
            currentSyncStateMachine = null;

            // Set new system state after recovery
            systemState = SystemState.Make(Phase.REST, recoveredHLCInfo.info.version + 1);

            if (!recoveredICInfo.IsDefault() && recoveryCountdown != null)
            {
                Debug.WriteLine("Ignoring index checkpoint as we have already recovered index previously");
                recoveredICInfo = default;
            }

            if (recoveredICInfo.IsDefault())
            {
                // No index checkpoint - recover from begin of log
                recoverFromAddress = recoveredHLCInfo.info.beginAddress;

                // Unless we recovered previously until some hlog address
                if (hlog.FlushedUntilAddress > recoverFromAddress)
                    recoverFromAddress = hlog.FlushedUntilAddress;

                // Start recovery at least from beginning of fuzzy log region
                // Needed if we are recovering to the same checkpoint a second time, with undo
                // set to true during the second time.
                if (recoveredHLCInfo.info.startLogicalAddress < recoverFromAddress)
                    recoverFromAddress = recoveredHLCInfo.info.startLogicalAddress;
            }
            else
            {
                recoverFromAddress = recoveredHLCInfo.info.beginAddress;

                if (recoveredICInfo.info.startLogicalAddress > recoverFromAddress)
                {
                    // Index checkpoint given - recover to that
                    recoverFromAddress = recoveredICInfo.info.startLogicalAddress;
                    return false;
                }
            }

            return true;
        }

        private bool SetRecoveryPageRanges(HybridLogCheckpointInfo recoveredHLCInfo, int numPagesToPreload, long fromAddress, out long tailAddress, out long headAddress, out long scanFromAddress)
        {
            if ((recoveredHLCInfo.info.useSnapshotFile == 0) && (recoveredHLCInfo.info.finalLogicalAddress <= hlog.GetTailAddress()))
            {
                tailAddress = headAddress = scanFromAddress = default;
                return false;
            }

            // Recover segment offsets for object log
            if (recoveredHLCInfo.info.objectLogSegmentOffsets != null)
                Array.Copy(recoveredHLCInfo.info.objectLogSegmentOffsets,
                    hlog.GetSegmentOffsets(),
                    recoveredHLCInfo.info.objectLogSegmentOffsets.Length);

            tailAddress = recoveredHLCInfo.info.finalLogicalAddress;
            headAddress = recoveredHLCInfo.info.headAddress;
            if (numPagesToPreload != -1)
            {
                var head = (hlog.GetPage(tailAddress) - numPagesToPreload) << hlog.LogPageSizeBits;
                if (head > headAddress)
                    headAddress = head;
            }

            scanFromAddress = headAddress;
            if (fromAddress < scanFromAddress)
                scanFromAddress = fromAddress;

            // Adjust head address if we need to anyway preload
            if (scanFromAddress < headAddress)
            {
                headAddress = scanFromAddress;
                if (headAddress < recoveredHLCInfo.info.headAddress)
                    headAddress = recoveredHLCInfo.info.headAddress;
            }

            if (hlog.FlushedUntilAddress > scanFromAddress)
                scanFromAddress = hlog.FlushedUntilAddress;
            return true;
        }

        private void RecoverHybridLog(long scanFromAddress, long recoverFromAddress, long untilAddress, long nextVersion, CheckpointType checkpointType, RecoveryOptions options)
        {
            if (untilAddress <= scanFromAddress)
                return;
            var recoveryStatus = GetPageRangesToRead(scanFromAddress, untilAddress, checkpointType, out long startPage, out long endPage, out int capacity, out int numPagesToReadFirst);

            // Issue request to read pages as much as possible
            hlog.AsyncReadPagesFromDevice(startPage, numPagesToReadFirst, untilAddress, hlog.AsyncReadPagesCallbackForRecovery, recoveryStatus);

            for (long page = startPage; page < endPage; page++)
            {
                // Ensure page has been read into memory
                int pageIndex = hlog.GetPageIndexForPage(page);
                recoveryStatus.WaitRead(pageIndex);

                ProcessReadPage(recoverFromAddress, untilAddress, nextVersion, options, recoveryStatus, endPage, capacity, page, pageIndex);
            }

            WaitUntilAllPagesHaveBeenFlushed(startPage, endPage, recoveryStatus);
        }

        private async ValueTask RecoverHybridLogAsync(long scanFromAddress, long recoverFromAddress, long untilAddress, long nextVersion, CheckpointType checkpointType, RecoveryOptions options, CancellationToken cancellationToken)
        {
            if (untilAddress <= scanFromAddress)
                return;
            var recoveryStatus = GetPageRangesToRead(scanFromAddress, untilAddress, checkpointType, out long startPage, out long endPage, out int capacity, out int numPagesToReadFirst);

            // Issue request to read pages as much as possible
            hlog.AsyncReadPagesFromDevice(startPage, numPagesToReadFirst, untilAddress, hlog.AsyncReadPagesCallbackForRecovery, recoveryStatus);

            for (long page = startPage; page < endPage; page++)
            {
                // Ensure page has been read into memory
                int pageIndex = hlog.GetPageIndexForPage(page);
                await recoveryStatus.WaitReadAsync(pageIndex, cancellationToken).ConfigureAwait(false);

                ProcessReadPage(recoverFromAddress, untilAddress, nextVersion, options, recoveryStatus, endPage, capacity, page, pageIndex);
            }

            await WaitUntilAllPagesHaveBeenFlushedAsync(startPage, endPage, recoveryStatus, cancellationToken).ConfigureAwait(false);
        }

        private RecoveryStatus GetPageRangesToRead(long scanFromAddress, long untilAddress, CheckpointType checkpointType, out long startPage, out long endPage, out int capacity, out int numPagesToReadFirst)
        {
            startPage = hlog.GetPage(scanFromAddress);
            endPage = hlog.GetPage(untilAddress);
            if (untilAddress > hlog.GetStartLogicalAddress(endPage))
            {
                endPage++;
            }

            capacity = hlog.GetCapacityNumPages();
            int totalPagesToRead = (int)(endPage - startPage);
            numPagesToReadFirst = Math.Min(capacity, totalPagesToRead);
            return new RecoveryStatus(capacity, endPage, untilAddress, checkpointType);
        }

        private void ProcessReadPage(long recoverFromAddress, long untilAddress, long nextVersion, RecoveryOptions options, RecoveryStatus recoveryStatus, long endPage, int capacity, long page, int pageIndex)
        {
            if (ProcessReadPage(recoverFromAddress, untilAddress, nextVersion, options, recoveryStatus, page, pageIndex))
            {
                // Page was modified due to undoFutureVersion. Flush it to disk; the callback issues the after-capacity read request if necessary.
                hlog.AsyncFlushPages(page, 1, AsyncFlushPageCallbackForRecovery, recoveryStatus);
                return;
            }

            // We do not need to flush
            recoveryStatus.flushStatus[pageIndex] = FlushStatus.Done;

            // Issue next read if there are more pages past 'capacity' from this one.
            if (page + capacity < endPage)
            {
                recoveryStatus.readStatus[pageIndex] = ReadStatus.Pending;
                hlog.AsyncReadPagesFromDevice(page + capacity, 1, untilAddress, hlog.AsyncReadPagesCallbackForRecovery, recoveryStatus);
            }
        }

        private bool ProcessReadPage(long recoverFromAddress, long untilAddress, long nextVersion, RecoveryOptions options, RecoveryStatus recoveryStatus, long page, int pageIndex)
        {
            var startLogicalAddress = hlog.GetStartLogicalAddress(page);
            var endLogicalAddress = hlog.GetStartLogicalAddress(page + 1);

            if (options.clearLocks)
            {
                if (options.headAddress >= endLogicalAddress)
                    return false;
            }
            else if (recoverFromAddress >= endLogicalAddress)
                return false;

            var pageFromAddress = 0L;
            var pageUntilAddress = hlog.GetPageSize();
            if (options.clearLocks)
            {
                if (options.headAddress > startLogicalAddress)
                    pageFromAddress = hlog.GetOffsetInPage(options.headAddress);

                if (options.tailAddress < endLogicalAddress)
                    pageUntilAddress = hlog.GetOffsetInPage(options.tailAddress);
            }
            else 
            {
                if (recoverFromAddress > startLogicalAddress)
                    pageFromAddress = hlog.GetOffsetInPage(recoverFromAddress);

                if (untilAddress < endLogicalAddress)
                    pageUntilAddress = hlog.GetOffsetInPage(untilAddress);
            }

            var physicalAddress = hlog.GetPhysicalAddress(startLogicalAddress);
            if (RecoverFromPage(recoverFromAddress, pageFromAddress, pageUntilAddress, startLogicalAddress, physicalAddress, nextVersion, options.undoNextVersion, options.clearLocks))
            {
                // The current page was modified due to undoFutureVersion; caller will flush it to storage and issue a read request if necessary.
                recoveryStatus.readStatus[pageIndex] = ReadStatus.Pending;
                recoveryStatus.flushStatus[pageIndex] = FlushStatus.Pending;
                return true;
            }

            return false;
        }

        private void WaitUntilAllPagesHaveBeenFlushed(long startPage, long endPage, RecoveryStatus recoveryStatus)
        {
            for (long page = startPage; page < endPage; page++)
                recoveryStatus.WaitFlush(hlog.GetPageIndexForPage(page));
        }

        private async ValueTask WaitUntilAllPagesHaveBeenFlushedAsync(long startPage, long endPage, RecoveryStatus recoveryStatus, CancellationToken cancellationToken)
        {
            for (long page = startPage; page < endPage; page++)
                await recoveryStatus.WaitFlushAsync(hlog.GetPageIndexForPage(page), cancellationToken).ConfigureAwait(false);
        }

        private void RecoverHybridLogFromSnapshotFile(long scanFromAddress, long recoverFromAddress, long untilAddress, long snapshotStartAddress, long snapshotEndAddress, long nextVersion, Guid guid, RecoveryOptions options, DeltaLog deltaLog, long recoverTo)
        {
            GetSnapshotPageRangesToRead(scanFromAddress, untilAddress, snapshotStartAddress, snapshotEndAddress, guid, out long startPage, out long endPage, out long snapshotEndPage, out int capacity, out var recoveryStatus, out int numPagesToReadFirst);

            hlog.AsyncReadPagesFromDevice(startPage, numPagesToReadFirst, snapshotEndAddress,
                                          hlog.AsyncReadPagesCallbackForRecovery,
                                          recoveryStatus, recoveryStatus.recoveryDevicePageOffset,
                                          recoveryStatus.recoveryDevice, recoveryStatus.objectLogRecoveryDevice);

            for (long page = startPage; page < endPage; page += capacity)
            {
                long end = Math.Min(page + capacity, endPage);
                for (long p = page; p < end; p++)
                {
                    int pageIndex = hlog.GetPageIndexForPage(p);
                    if (p < snapshotEndPage)
                    {
                        // Ensure the page is read from file
                        recoveryStatus.WaitRead(pageIndex);
                    }
                    else
                    {
                        recoveryStatus.WaitFlush(pageIndex);
                        if (!hlog.IsAllocated(pageIndex))
                            hlog.AllocatePage(pageIndex);
                        else
                            hlog.ClearPage(pageIndex);
                    }
                }

                ApplyDelta(scanFromAddress, recoverFromAddress, untilAddress, nextVersion, options, deltaLog, recoverTo, endPage, snapshotEndPage, capacity, recoveryStatus, page, end);
            }

            WaitUntilAllPagesHaveBeenFlushed(startPage, endPage, recoveryStatus);
            recoveryStatus.Dispose();
        }

        private async ValueTask RecoverHybridLogFromSnapshotFileAsync(long scanFromAddress, long recoverFromAddress, long untilAddress, long snapshotStartAddress, long snapshotEndAddress, long nextVersion, Guid guid, RecoveryOptions options, DeltaLog deltaLog, long recoverTo, CancellationToken cancellationToken)
        {
            GetSnapshotPageRangesToRead(scanFromAddress, untilAddress, snapshotStartAddress, snapshotEndAddress, guid, out long startPage, out long endPage, out long snapshotEndPage, out int capacity, out var recoveryStatus, out int numPagesToReadFirst);

            hlog.AsyncReadPagesFromDevice(startPage, numPagesToReadFirst, snapshotEndAddress,
                                          hlog.AsyncReadPagesCallbackForRecovery,
                                          recoveryStatus, recoveryStatus.recoveryDevicePageOffset,
                                          recoveryStatus.recoveryDevice, recoveryStatus.objectLogRecoveryDevice);

            for (long page = startPage; page < endPage; page += capacity)
            {
                long end = Math.Min(page + capacity, endPage);
                for (long p = page; p < end; p++)
                {
                    int pageIndex = hlog.GetPageIndexForPage(p);
                    if (p < snapshotEndPage)
                    {
                        // Ensure the page is read from file
                        await recoveryStatus.WaitReadAsync(pageIndex, cancellationToken).ConfigureAwait(false);
                    }
                    else
                    {
                        await recoveryStatus.WaitFlushAsync(pageIndex, cancellationToken).ConfigureAwait(false);
                        if (!hlog.IsAllocated(pageIndex))
                            hlog.AllocatePage(pageIndex);
                        else
                            hlog.ClearPage(pageIndex);
                    }
                }

                ApplyDelta(scanFromAddress, recoverFromAddress, untilAddress, nextVersion, options, deltaLog, recoverTo, endPage, snapshotEndPage, capacity, recoveryStatus, page, end);
            }

            await WaitUntilAllPagesHaveBeenFlushedAsync(startPage, endPage, recoveryStatus, cancellationToken).ConfigureAwait(false);
            recoveryStatus.Dispose();
        }

        private void ApplyDelta(long scanFromAddress, long recoverFromAddress, long untilAddress, long nextVersion, RecoveryOptions options, DeltaLog deltaLog, long recoverTo, long endPage, long snapshotEndPage, int capacity, RecoveryStatus recoveryStatus, long page, long end)
        {
            hlog.ApplyDelta(deltaLog, page, end, recoverTo);

            for (long p = page; p < end; p++)
            {
                int pageIndex = hlog.GetPageIndexForPage(p);

                var endLogicalAddress = hlog.GetStartLogicalAddress(p + 1);
                if ((recoverFromAddress < endLogicalAddress && recoverFromAddress < untilAddress)
                        || (options.clearLocks && options.headAddress < endLogicalAddress))
                    ProcessReadSnapshotPage(scanFromAddress, untilAddress, nextVersion, options, recoveryStatus, p, pageIndex);

                // Issue next read
                if (p + capacity < endPage)
                {
                    // Flush snapshot page to main log
                    // Flush callback will issue further reads or page clears
                    recoveryStatus.flushStatus[pageIndex] = FlushStatus.Pending;
                    if (p + capacity < snapshotEndPage)
                        recoveryStatus.readStatus[pageIndex] = ReadStatus.Pending;
                    hlog.AsyncFlushPages(p, 1, AsyncFlushPageCallbackForRecovery, recoveryStatus);
                }
            }
        }

        private void GetSnapshotPageRangesToRead(long fromAddress, long untilAddress, long snapshotStartAddress, long snapshotEndAddress, Guid guid, out long startPage, out long endPage, out long snapshotEndPage, out int capacity,
                                                 out RecoveryStatus recoveryStatus, out int numPagesToReadFirst)
        {
            // Compute startPage and endPage
            startPage = hlog.GetPage(fromAddress);
            endPage = hlog.GetPage(untilAddress);
            if (untilAddress > hlog.GetStartLogicalAddress(endPage))
                endPage++;
            long snapshotStartPage = hlog.GetPage(snapshotStartAddress);
            snapshotEndPage = hlog.GetPage(snapshotEndAddress);
            if (snapshotEndAddress > hlog.GetStartLogicalAddress(snapshotEndPage))
                snapshotEndPage++;

            // By default first page has one extra record
            capacity = hlog.GetCapacityNumPages();
            var recoveryDevice = checkpointManager.GetSnapshotLogDevice(guid);
            var objectLogRecoveryDevice = checkpointManager.GetSnapshotObjectLogDevice(guid);

            recoveryDevice.Initialize(hlog.GetSegmentSize());
            objectLogRecoveryDevice.Initialize(-1);
            recoveryStatus = new RecoveryStatus(capacity, endPage, untilAddress, CheckpointType.Snapshot)
            {
                recoveryDevice = recoveryDevice,
                objectLogRecoveryDevice = objectLogRecoveryDevice,
                recoveryDevicePageOffset = snapshotStartPage,
                snapshotEndPage = snapshotEndPage
            };

            // Initially issue read request for all pages that can be held in memory
            int totalPagesToRead = (int)(snapshotEndPage - startPage);
            numPagesToReadFirst = Math.Min(capacity, totalPagesToRead);
        }

        private void ProcessReadSnapshotPage(long fromAddress, long untilAddress, long nextVersion, RecoveryOptions options, RecoveryStatus recoveryStatus, long page, int pageIndex)
        {
            // Page at hand
            var startLogicalAddress = hlog.GetStartLogicalAddress(page);
            var endLogicalAddress = hlog.GetStartLogicalAddress(page + 1);

            // Perform recovery if page in fuzzy portion of the log or clearing locks
            if ((fromAddress < endLogicalAddress && fromAddress < untilAddress) || (options.clearLocks && options.headAddress < endLogicalAddress))
            {
                /*
                 * Handling corner-cases:
                 * ----------------------
                 * When fromAddress is in the middle of the page, then start recovery only from corresponding offset 
                 * in page. Similarly, if untilAddress falls in the middle of the page, perform recovery only until that
                 * offset. Otherwise, scan the entire page [0, PageSize)
                 * 
                 * If options.clearLocks, the read of this page overwrote the prior lock clearing, so we must redo it here.
                 */

                var pageFromAddress = 0L;
                var pageUntilAddress = hlog.GetPageSize();
                if (options.clearLocks)
                {
                    if (options.headAddress > startLogicalAddress && options.headAddress < endLogicalAddress)
                        pageFromAddress = hlog.GetOffsetInPage(options.headAddress);
                    if (endLogicalAddress > options.tailAddress)
                        pageUntilAddress = hlog.GetOffsetInPage(options.tailAddress);
                }
                else
                {
                    if (fromAddress > startLogicalAddress && fromAddress < endLogicalAddress)
                        pageFromAddress = hlog.GetOffsetInPage(fromAddress);
                    if (endLogicalAddress > untilAddress)
                        pageUntilAddress = hlog.GetOffsetInPage(untilAddress);
                }

                var physicalAddress = hlog.GetPhysicalAddress(startLogicalAddress);
                RecoverFromPage(fromAddress, pageFromAddress, pageUntilAddress,
                                startLogicalAddress, physicalAddress, nextVersion, options.undoNextVersion, options.clearLocks);
            }

            recoveryStatus.flushStatus[pageIndex] = FlushStatus.Done;
        }

        private unsafe bool RecoverFromPage(long startRecoveryAddress,
                                     long fromLogicalAddressInPage,
                                     long untilLogicalAddressInPage,
                                     long pageLogicalAddress,
                                     long pagePhysicalAddress,
                                     long nextVersion, bool undoNextVersion, bool clearLocks)
        {
            bool touched = false;

            var hash = default(long);
            var tag = default(ushort);
            var pointer = default(long);
            var recordStart = default(long);
            var bucket = default(HashBucket*);
            var entry = default(HashBucketEntry);
            var slot = default(int);

            pointer = fromLogicalAddressInPage;
            while (pointer < untilLogicalAddressInPage)
            {
                recordStart = pagePhysicalAddress + pointer;
                ref RecordInfo info = ref hlog.GetInfo(recordStart);

                if (info.IsNull())
                {
                    pointer += RecordInfo.GetLength();
                    continue;
                }

                if (!info.Invalid)
                {
                    hash = comparer.GetHashCode64(ref hlog.GetKey(recordStart));
                    tag = (ushort)((ulong)hash >> Constants.kHashTagShift);

                    entry = default;
                    FindOrCreateTag(hash, tag, ref bucket, ref slot, ref entry, hlog.BeginAddress);

                    if (!info.InNewVersion || !undoNextVersion)
                    {
                        entry.Address = pageLogicalAddress + pointer;
                        entry.Tag = tag;
                        entry.Pending = false;
                        entry.Tentative = false;
                        bucket->bucket_entries[slot] = entry.word;
                        if (clearLocks && info.IsLocked)
                        {
                            // We do not set 'touched' here as there is no need to write these pages back
                            info.ClearLocks();
                        }
                    }
                    else
                    {
                        touched = true;
                        info.SetInvalid();
                        if (info.PreviousAddress < startRecoveryAddress)
                        {
                            entry.Address = info.PreviousAddress;
                            entry.Tag = tag;
                            entry.Pending = false;
                            entry.Tentative = false;
                            bucket->bucket_entries[slot] = entry.word;
                        }
                    }
                }
                pointer += hlog.GetRecordSize(recordStart).Item2;
            }

            return touched;
        }


        private void AsyncFlushPageCallbackForRecovery(uint errorCode, uint numBytes, object context)
        {
            if (errorCode != 0)
            {
                Trace.TraceError("AsyncFlushPageCallbackForRecovery error: {0}", errorCode);
            }

            // Set the page status to flushed
            var result = (PageAsyncFlushResult<RecoveryStatus>)context;

            if (Interlocked.Decrement(ref result.count) == 0)
            {
                int pageIndex = hlog.GetPageIndexForPage(result.page);
                result.context.SignalFlushed(pageIndex);
                if (result.page + result.context.capacity < result.context.endPage)
                {
                    long readPage = result.page + result.context.capacity;
                    if (result.context.checkpointType == CheckpointType.FoldOver)
                    {
                        hlog.AsyncReadPagesFromDevice(readPage, 1, result.context.untilAddress, hlog.AsyncReadPagesCallbackForRecovery, result.context);
                    }
                    else
                    {
                        if (readPage < result.context.snapshotEndPage)
                        {
                            // If next page is in snapshot, issue retrieval for it
                            hlog.AsyncReadPagesFromDevice(readPage, 1, result.context.untilAddress, hlog.AsyncReadPagesCallbackForRecovery,
                                                            result.context,
                                                            result.context.recoveryDevicePageOffset,
                                                            result.context.recoveryDevice, result.context.objectLogRecoveryDevice);
                        }
                    }
                }
                result.Free();
            }
        }

        internal bool AtomicSwitch<Input, Output, Context>(FasterExecutionContext<Input, Output, Context> fromCtx, FasterExecutionContext<Input, Output, Context> toCtx, long version, ConcurrentDictionary<int, (string, CommitPoint)> tokens)
        {
            lock (toCtx)
            {
                if (toCtx.version < version)
                {
                    CopyContext(fromCtx, toCtx);
                    if (toCtx.serialNum != -1)
                    {
                        tokens.TryAdd(toCtx.sessionID, (toCtx.sessionName,
                            new CommitPoint
                            {
                                UntilSerialNo = toCtx.serialNum,
                                ExcludedSerialNos = toCtx.excludedSerialNos
                            }));
                    }
                    return true;
                }
            }
            return false;
        }
    }

    public abstract partial class AllocatorBase<Key, Value> : IDisposable
    {
        /// <summary>
        /// Restore log
        /// </summary>
        /// <param name="beginAddress"></param>
        /// <param name="headAddress"></param>
        /// <param name="fromAddress"></param>
        /// <param name="untilAddress"></param>
        /// <param name="numPagesToPreload">Number of pages to preload into memory after recovery</param>
        public void RestoreHybridLog(long beginAddress, long headAddress, long fromAddress, long untilAddress, int numPagesToPreload = -1)
        {
            if (RestoreHybridLogInitializePages(beginAddress, headAddress, fromAddress, untilAddress, numPagesToPreload, out var recoveryStatus, out long headPage, out long tailPage))
            { 
                for (long page = headPage; page <= tailPage; page++)
                    recoveryStatus.WaitRead(GetPageIndexForPage(page));
            }

            RecoveryReset(untilAddress, headAddress, beginAddress, untilAddress);
        }

        /// <summary>
        /// Restore log
        /// </summary>
        /// <param name="beginAddress"></param>
        /// <param name="headAddress"></param>
        /// <param name="fromAddress"></param>
        /// <param name="untilAddress"></param>
        /// <param name="numPagesToPreload">Number of pages to preload into memory after recovery</param>
        /// <param name="cancellationToken"></param>
        public async ValueTask RestoreHybridLogAsync(long beginAddress, long headAddress, long fromAddress, long untilAddress, int numPagesToPreload = -1, CancellationToken cancellationToken = default)
        {
            if (RestoreHybridLogInitializePages(beginAddress, headAddress, fromAddress, untilAddress, numPagesToPreload, out var recoveryStatus, out long headPage, out long tailPage))
            {
                for (long page = headPage; page <= tailPage; page++)
                    await recoveryStatus.WaitReadAsync(GetPageIndexForPage(page), cancellationToken).ConfigureAwait(false);
            }

            RecoveryReset(untilAddress, headAddress, beginAddress, untilAddress);
        }

        private bool RestoreHybridLogInitializePages(long beginAddress, long headAddress, long fromAddress, long untilAddress, int numPagesToPreload,
                                                     out RecoveryStatus recoveryStatus, out long headPage, out long tailPage)
        {
            if (numPagesToPreload != -1)
            {
                var head = (GetPage(untilAddress) - numPagesToPreload) << LogPageSizeBits;
                if (head > headAddress)
                    headAddress = head;
            }
            Debug.Assert(beginAddress <= headAddress);
            Debug.Assert(headAddress <= untilAddress);

            // Special cases: we do not load any records into memory
            if (
                (beginAddress == untilAddress) || // Empty log
                ((headAddress == untilAddress) && (GetOffsetInPage(headAddress) == 0)) // Empty in-memory page
                )
            {
                if (!IsAllocated(GetPageIndexForAddress(headAddress)))
                    AllocatePage(GetPageIndexForAddress(headAddress));
            }
            else
            {
                if (headAddress < fromAddress)
                {
                    tailPage = GetPage(fromAddress);
                    headPage = GetPage(headAddress);

                    recoveryStatus = new RecoveryStatus(GetCapacityNumPages(), tailPage, untilAddress, 0);
                    for (int i = 0; i < recoveryStatus.capacity; i++)
                    {
                        recoveryStatus.readStatus[i] = ReadStatus.Done;
                    }

                    var numPages = 0;
                    for (var page = headPage; page <= tailPage; page++)
                    {
                        var pageIndex = GetPageIndexForPage(page);
                        recoveryStatus.readStatus[pageIndex] = ReadStatus.Pending;
                        numPages++;
                    }

                    AsyncReadPagesFromDevice(headPage, numPages, untilAddress, AsyncReadPagesCallbackForRecovery, recoveryStatus);
                    return true;
                }
            }

            recoveryStatus = default;
            headPage = tailPage = 0;
            return false;
        }

        internal unsafe void AsyncReadPagesCallbackForRecovery(uint errorCode, uint numBytes, object context)
        {
            if (errorCode != 0)
            {
                Trace.TraceError("AsyncReadPagesCallbackForRecovery error: {0}", errorCode);
            }

            // Set the page status to "read done"
            var result = (PageAsyncReadResult<RecoveryStatus>)context;

            if (result.freeBuffer1 != null)
            {
                PopulatePage(result.freeBuffer1.GetValidPointer(), result.freeBuffer1.required_bytes, result.page);
                result.freeBuffer1.Return();
            }
            int pageIndex = GetPageIndexForPage(result.page);
            result.context.SignalRead(pageIndex);
        }
    }
}
