// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma warning disable 0162

using System;
using System.Diagnostics;
using System.Resources;
using System.Threading;

namespace FASTER.core
{
    internal enum ReadStatus { Pending, Done };
    internal enum FlushStatus { Pending, Done };

    internal class RecoveryStatus
    {
        public long startPage;
        public long endPage;
        public long untilAddress;
        public int capacity;
        public CheckpointType checkpointType;

        public IDevice recoveryDevice;
        public long recoveryDevicePageOffset;
        public IDevice objectLogRecoveryDevice;

        public ReadStatus[] readStatus;
        public FlushStatus[] flushStatus;
        
        public RecoveryStatus(int capacity,
                              long startPage,
                              long endPage, long untilAddress, CheckpointType checkpointType)
        {
            this.capacity = capacity;
            this.startPage = startPage;
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
    }


    public unsafe partial class FasterKV<Key, Value> : FasterBase, IFasterKV<Key, Value>
    {

        private void InternalRecoverFromLatestCheckpoints(int numPagesToPreload)
        {
            Debug.WriteLine("********* Primary Recovery Information ********");

            HybridLogCheckpointInfo recoveredHLCInfo = default;

            foreach (var hybridLogToken in checkpointManager.GetLogCheckpointTokens())
            {
                try
                {
                    recoveredHLCInfo = new HybridLogCheckpointInfo();
                    recoveredHLCInfo.Recover(hybridLogToken, checkpointManager);
                }
                catch
                {
                    continue;
                }

                Debug.WriteLine("HybridLog Checkpoint: {0}", hybridLogToken);
                break;
            }

            if (recoveredHLCInfo.IsDefault())
                throw new FasterException("Unable to find valid index token");

            recoveredHLCInfo.info.DebugPrint();

            IndexCheckpointInfo recoveredICInfo = default;

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

                if (!IsCompatible(recoveredICInfo.info, recoveredHLCInfo.info))
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

            InternalRecover(recoveredICInfo, recoveredHLCInfo, numPagesToPreload);
        }

        private bool IsCompatible(in IndexRecoveryInfo indexInfo, in HybridLogRecoveryInfo recoveryInfo)
        {
            var l1 = indexInfo.finalLogicalAddress;
            var l2 = recoveryInfo.finalLogicalAddress;
            return l1 <= l2;
        }

        private void InternalRecover(Guid indexToken, Guid hybridLogToken, int numPagesToPreload)
        {
            Debug.WriteLine("********* Primary Recovery Information ********");
            Debug.WriteLine("Index Checkpoint: {0}", indexToken);
            Debug.WriteLine("HybridLog Checkpoint: {0}", hybridLogToken);


            // Recovery appropriate context information
            var recoveredHLCInfo = new HybridLogCheckpointInfo();
            recoveredHLCInfo.Recover(hybridLogToken, checkpointManager);
            recoveredHLCInfo.info.DebugPrint();

            IndexCheckpointInfo recoveredICInfo;
            try
            {
                recoveredICInfo = new IndexCheckpointInfo();
                recoveredICInfo.Recover(indexToken, checkpointManager);
                recoveredICInfo.info.DebugPrint();
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

            InternalRecover(recoveredICInfo, recoveredHLCInfo, numPagesToPreload);
        }

        private void InternalRecover(IndexCheckpointInfo recoveredICInfo, HybridLogCheckpointInfo recoveredHLCInfo, int numPagesToPreload)
        {
            // Ensure active state machine to null
            currentSyncStateMachine = null;

            // Set new system state after recovery
            var v = recoveredHLCInfo.info.version;
            systemState.phase = Phase.REST;
            systemState.version = (v + 1);

            long fromAddress;

            if (!recoveredICInfo.IsDefault() && hlog.FlushedUntilAddress != hlog.GetFirstValidLogicalAddress(0))
            {
                Debug.WriteLine("Ignoring index checkpoint as we have already recovered index previously");
                recoveredICInfo = default;
            }

            if (recoveredICInfo.IsDefault())
            {
                // No index checkpoint - recover from begin of log
                fromAddress = recoveredHLCInfo.info.beginAddress;

                // Unless we recovered previously until some hlog address
                if (hlog.FlushedUntilAddress > fromAddress)
                    fromAddress = hlog.FlushedUntilAddress;
            }
            else
            {
                fromAddress = recoveredHLCInfo.info.beginAddress;

                if (recoveredICInfo.info.startLogicalAddress > fromAddress)
                {
                    // Index checkpoint given - recover to that
                    RecoverFuzzyIndex(recoveredICInfo);
                    fromAddress = recoveredICInfo.info.startLogicalAddress;
                }
            }

            if ((recoveredHLCInfo.info.useSnapshotFile == 0) && (recoveredHLCInfo.info.finalLogicalAddress <= hlog.GetTailAddress()))
            {
                return;
            }

            // Recover segment offsets for object log
            if (recoveredHLCInfo.info.objectLogSegmentOffsets != null)
                Array.Copy(recoveredHLCInfo.info.objectLogSegmentOffsets,
                    hlog.GetSegmentOffsets(),
                    recoveredHLCInfo.info.objectLogSegmentOffsets.Length);

            long tailAddress = recoveredHLCInfo.info.finalLogicalAddress;
            long headAddress = recoveredHLCInfo.info.headAddress;
            if (numPagesToPreload != -1)
            {
                var head = (hlog.GetPage(tailAddress) - numPagesToPreload) << hlog.LogPageSizeBits;
                if (head > headAddress)
                    headAddress = head;
            }

            long scanFromAddress = headAddress;
            if (fromAddress < scanFromAddress)
                scanFromAddress = fromAddress;

            // Adjust head address if we need to anyway preload
            if (scanFromAddress < headAddress)
            {
                headAddress = scanFromAddress;
                if (headAddress < recoveredHLCInfo.info.headAddress)
                    headAddress = recoveredHLCInfo.info.headAddress;
            }

            // Make index consistent for version v
            if (recoveredHLCInfo.info.useSnapshotFile == 0)
            {
                RecoverHybridLog(scanFromAddress, fromAddress, recoveredHLCInfo.info.finalLogicalAddress, recoveredHLCInfo.info.version);
                hlog.RecoveryReset(tailAddress, headAddress, recoveredHLCInfo.info.beginAddress, tailAddress);
            }
            else
            {
                // First recover from index starting point (fromAddress) to snapshot starting point (flushedLogicalAddress)
                RecoverHybridLog(scanFromAddress, fromAddress, recoveredHLCInfo.info.flushedLogicalAddress, recoveredHLCInfo.info.version);
                // Then recover snapshot into mutable region
                RecoverHybridLogFromSnapshotFile(fromAddress, recoveredHLCInfo.info);
                hlog.RecoveryReset(tailAddress, headAddress, recoveredHLCInfo.info.beginAddress, recoveredHLCInfo.info.flushedLogicalAddress);
            }

            // Recover session information
            _recoveredSessions = recoveredHLCInfo.info.continueTokens;
        }

        private void RecoverHybridLog(long scanFromAddress, long recoverFromAddress, long untilAddress, int version)
        {
            if (untilAddress < scanFromAddress)
                return;

            var startPage = hlog.GetPage(scanFromAddress);
            var endPage = hlog.GetPage(untilAddress);
            if (untilAddress > hlog.GetStartLogicalAddress(endPage))
            {
                endPage++;
            }

            var capacity = hlog.GetCapacityNumPages();
            var recoveryStatus = new RecoveryStatus(capacity, startPage, endPage, untilAddress, CheckpointType.FoldOver);

            int totalPagesToRead = (int)(endPage - startPage);
            int numPagesToReadFirst = Math.Min(capacity, totalPagesToRead);

            // Issue request to read pages as much as possible
            hlog.AsyncReadPagesFromDevice(startPage, numPagesToReadFirst, untilAddress, hlog.AsyncReadPagesCallbackForRecovery, recoveryStatus);

            for (long page = startPage; page < endPage; page++)
            {
                // Ensure page has been read into memory
                int pageIndex = hlog.GetPageIndexForPage(page);
                while (recoveryStatus.readStatus[pageIndex] == ReadStatus.Pending)
                {
                    Thread.Sleep(10);
                }

                var startLogicalAddress = hlog.GetStartLogicalAddress(page);
                var endLogicalAddress = hlog.GetStartLogicalAddress(page + 1);

                if (recoverFromAddress < endLogicalAddress)
                {
                    var pageFromAddress = 0L;
                    if (recoverFromAddress > startLogicalAddress)
                    {
                        pageFromAddress = hlog.GetOffsetInPage(recoverFromAddress);
                    }

                    var pageUntilAddress = hlog.GetPageSize();
                    if (untilAddress < endLogicalAddress)
                    {
                        pageUntilAddress = hlog.GetOffsetInPage(untilAddress);
                    }

                    var physicalAddress = hlog.GetPhysicalAddress(startLogicalAddress);
                    if (RecoverFromPage(recoverFromAddress, pageFromAddress, pageUntilAddress, startLogicalAddress, physicalAddress, version))
                    {
                        // OS thread flushes current page and issues a read request if necessary
                        recoveryStatus.readStatus[pageIndex] = ReadStatus.Pending;
                        recoveryStatus.flushStatus[pageIndex] = FlushStatus.Pending;

                        hlog.AsyncFlushPages(page, 1, AsyncFlushPageCallbackForRecovery, recoveryStatus);
                        continue;
                    }
                }

                recoveryStatus.flushStatus[pageIndex] = FlushStatus.Done;

                // Issue next read
                if (page + capacity < endPage)
                {
                    recoveryStatus.readStatus[pageIndex] = ReadStatus.Pending;
                    hlog.AsyncReadPagesFromDevice(page + capacity, 1, untilAddress, hlog.AsyncReadPagesCallbackForRecovery, recoveryStatus);
                }
            }

            // Assert that all pages have been flushed
            var done = false;
            while (!done)
            {
                done = true;
                for (long page = startPage; page < endPage; page++)
                {
                    int pageIndex = hlog.GetPageIndexForPage(page);
                    if (recoveryStatus.flushStatus[pageIndex] == FlushStatus.Pending)
                    {
                        done = false;
                        break;
                    }
                }
            }
        }

        private void RecoverHybridLogFromSnapshotFile(long fromAddress, HybridLogRecoveryInfo recoveryInfo)
        {
            var fileStartAddress = recoveryInfo.flushedLogicalAddress;
            var untilAddress = recoveryInfo.finalLogicalAddress;
            
            // Compute startPage and endPage
            var startPage = hlog.GetPage(fileStartAddress);
            var endPage = hlog.GetPage(untilAddress);
            if (untilAddress > hlog.GetStartLogicalAddress(endPage))
            {
                endPage++;
            }

            // By default first page has one extra record
            var capacity = hlog.GetCapacityNumPages();
            var recoveryDevice = checkpointManager.GetSnapshotLogDevice(recoveryInfo.guid);
            var objectLogRecoveryDevice = checkpointManager.GetSnapshotObjectLogDevice(recoveryInfo.guid);
            recoveryDevice.Initialize(hlog.GetSegmentSize());
            objectLogRecoveryDevice.Initialize(-1);
            var recoveryStatus = new RecoveryStatus(capacity, startPage, endPage, untilAddress, CheckpointType.Snapshot)
            {
                recoveryDevice = recoveryDevice,
                objectLogRecoveryDevice = objectLogRecoveryDevice,
                recoveryDevicePageOffset = startPage
            };

            // Initially issue read request for all pages that can be held in memory
            int totalPagesToRead = (int)(endPage - startPage);
            int numPagesToReadFirst = Math.Min(capacity, totalPagesToRead);

            hlog.AsyncReadPagesFromDevice(startPage, numPagesToReadFirst, untilAddress,
                            hlog.AsyncReadPagesCallbackForRecovery,
                            recoveryStatus,
                            recoveryStatus.recoveryDevicePageOffset,
                            recoveryStatus.recoveryDevice, recoveryStatus.objectLogRecoveryDevice);


            for (long page = startPage; page < endPage; page++)
            {

                // Ensure the page is read from file or flushed
                int pageIndex = hlog.GetPageIndexForPage(page);
                while (recoveryStatus.readStatus[pageIndex] == ReadStatus.Pending)
                {
                    Thread.Sleep(10);
                }

                // Page at hand
                var startLogicalAddress = hlog.GetStartLogicalAddress(page);
                var endLogicalAddress = hlog.GetStartLogicalAddress(page + 1);

                // Perform recovery if page in fuzzy portion of the log
                if ((fromAddress < endLogicalAddress) && (fromAddress < untilAddress))
                {
                    /*
                     * Handling corner-cases:
                     * ----------------------
                     * When fromAddress is in the middle of the page, 
                     * then start recovery only from corresponding offset 
                     * in page. Similarly, if untilAddress falls in the
                     * middle of the page, perform recovery only until that
                     * offset. Otherwise, scan the entire page [0, PageSize)
                     */
                    var pageFromAddress = 0L;
                    if (fromAddress > startLogicalAddress && fromAddress < endLogicalAddress)
                    {
                        pageFromAddress = hlog.GetOffsetInPage(fromAddress);
                    }

                    var pageUntilAddress = hlog.GetPageSize();
                    if (endLogicalAddress > untilAddress)
                    {
                        pageUntilAddress = hlog.GetOffsetInPage(untilAddress);
                    }

                    var physicalAddress = hlog.GetPhysicalAddress(startLogicalAddress);
                    RecoverFromPage(fromAddress, pageFromAddress, pageUntilAddress,
                                    startLogicalAddress, physicalAddress, recoveryInfo.version);

                }

                // OS thread flushes current page and issues a read request if necessary
                recoveryStatus.readStatus[pageIndex] = ReadStatus.Pending;
                recoveryStatus.flushStatus[pageIndex] = FlushStatus.Pending;

                // Write back records from snapshot to main hybrid log
                hlog.AsyncFlushPages(page, 1, AsyncFlushPageCallbackForRecovery, recoveryStatus);
            }

            // Assert and wait until all pages have been flushed
            var done = false;
            while (!done)
            {
                done = true;
                for (long page = startPage; page < endPage; page++)
                {
                    int pageIndex = hlog.GetPageIndexForPage(page);
                    if (recoveryStatus.flushStatus[pageIndex] == FlushStatus.Pending)
                    {
                        done = false;
                        break;
                    }
                }
            }

            recoveryStatus.recoveryDevice.Dispose();
            recoveryStatus.objectLogRecoveryDevice.Dispose();
        }

        private bool RecoverFromPage(long startRecoveryAddress,
                                     long fromLogicalAddressInPage,
                                     long untilLogicalAddressInPage,
                                     long pageLogicalAddress,
                                     long pagePhysicalAddress,
                                     int version)
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

                    if (info.Version <= version)
                    {
                        entry.Address = pageLogicalAddress + pointer;
                        entry.Tag = tag;
                        entry.Pending = false;
                        entry.Tentative = false;
                        bucket->bucket_entries[slot] = entry.word;
                    }
                    else
                    {
                        touched = true;
                        info.Invalid = true;
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
                pointer += hlog.GetRecordSize(recordStart);
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
                int index = hlog.GetPageIndexForPage(result.page);
                result.context.flushStatus[index] = FlushStatus.Done;
                if (result.page + result.context.capacity < result.context.endPage)
                {
                    long readPage = result.page + result.context.capacity;
                    if (result.context.checkpointType == CheckpointType.FoldOver)
                    {
                        hlog.AsyncReadPagesFromDevice(readPage, 1, result.context.untilAddress, hlog.AsyncReadPagesCallbackForRecovery, result.context);
                    }
                    else
                    {
                        hlog.AsyncReadPagesFromDevice(readPage, 1, result.context.untilAddress, hlog.AsyncReadPagesCallbackForRecovery,
                                                            result.context,
                                                            result.context.recoveryDevicePageOffset,
                                                            result.context.recoveryDevice, result.context.objectLogRecoveryDevice);
                    }
                }
                result.Free();
            }
        }
    }

    public unsafe abstract partial class AllocatorBase<Key, Value> : IDisposable
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
                    var tailPage = GetPage(fromAddress);
                    var headPage = GetPage(headAddress);

                    var recoveryStatus = new RecoveryStatus(GetCapacityNumPages(), headPage, tailPage, untilAddress, 0);
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

                    var done = false;
                    while (!done)
                    {
                        done = true;
                        for (long page = headPage; page <= tailPage; page++)
                        {
                            int pageIndex = GetPageIndexForPage(page);
                            if (recoveryStatus.readStatus[pageIndex] == ReadStatus.Pending)
                            {
                                done = false;
                                break;
                            }
                        }
                    }
                }
            }

            RecoveryReset(untilAddress, headAddress, beginAddress, untilAddress);
        }

        internal void AsyncReadPagesCallbackForRecovery(uint errorCode, uint numBytes, object context)
        {
            if (errorCode != 0)
            {
                Trace.TraceError("AsyncReadPagesCallbackForRecovery error: {0}", errorCode);
            }

            // Set the page status to flushed
            var result = (PageAsyncReadResult<RecoveryStatus>)context;

            if (result.freeBuffer1 != null)
            {
                PopulatePage(result.freeBuffer1.GetValidPointer(), result.freeBuffer1.required_bytes, result.page);
                result.freeBuffer1.Return();
            }
            int index = GetPageIndexForPage(result.page);
            result.context.readStatus[index] = ReadStatus.Done;
            Interlocked.MemoryBarrier();
        }
    }
}
