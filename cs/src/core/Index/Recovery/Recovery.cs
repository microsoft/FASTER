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

        public IDevice recoveryDevice;
        public long recoveryDevicePageOffset;
        public IDevice objectLogRecoveryDevice;

        public ReadStatus[] readStatus;
        public FlushStatus[] flushStatus;

        public RecoveryStatus(int capacity,
                              long startPage,
                              long endPage, long untilAddress)
        {
            this.capacity = capacity;
            this.startPage = startPage;
            this.endPage = endPage;
            this.untilAddress = untilAddress;
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

        private void InternalRecoverFromLatestCheckpoints()
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
                break;
            }

            if (recoveredICInfo.IsDefault())
                throw new FasterException("Unable to find valid index token");

            recoveredICInfo.info.DebugPrint();


            InternalRecover(recoveredICInfo, recoveredHLCInfo);
        }

        private bool IsCompatible(in IndexRecoveryInfo indexInfo, in HybridLogRecoveryInfo recoveryInfo)
        {
            var l1 = indexInfo.finalLogicalAddress;
            var l2 = recoveryInfo.finalLogicalAddress;
            return l1 <= l2;
        }

        private void InternalRecover(Guid indexToken, Guid hybridLogToken)
        {
            Debug.WriteLine("********* Primary Recovery Information ********");
            Debug.WriteLine("Index Checkpoint: {0}", indexToken);
            Debug.WriteLine("HybridLog Checkpoint: {0}", hybridLogToken);


            // Recovery appropriate context information
            var recoveredICInfo = new IndexCheckpointInfo();
            recoveredICInfo.Recover(indexToken, checkpointManager);
            recoveredICInfo.info.DebugPrint();

            var recoveredHLCInfo = new HybridLogCheckpointInfo();
            recoveredHLCInfo.Recover(hybridLogToken, checkpointManager);
            recoveredHLCInfo.info.DebugPrint();

            // Check if the two checkpoints are compatible for recovery
            if (!IsCompatible(recoveredICInfo.info, recoveredHLCInfo.info))
            {
                throw new FasterException("Cannot recover from (" + indexToken.ToString() + "," + hybridLogToken.ToString() + ") checkpoint pair!\n");
            }

            InternalRecover(recoveredICInfo, recoveredHLCInfo);
        }

        private void InternalRecover(IndexCheckpointInfo recoveredICInfo, HybridLogCheckpointInfo recoveredHLCInfo)
        {
            // Ensure active state machine to null
            currentSyncStateMachine = null;

            // Set new system state after recovery
            var v = recoveredHLCInfo.info.version;
            systemState.phase = Phase.REST;
            systemState.version = (v + 1);

            // Recover fuzzy index from checkpoint
            RecoverFuzzyIndex(recoveredICInfo);

            // Recover segment offsets for object log
            if (recoveredHLCInfo.info.objectLogSegmentOffsets != null)
                Array.Copy(recoveredHLCInfo.info.objectLogSegmentOffsets,
                    hlog.GetSegmentOffsets(),
                    recoveredHLCInfo.info.objectLogSegmentOffsets.Length);


            // Make index consistent for version v
            if (FoldOverSnapshot)
            {
                RecoverHybridLog(recoveredICInfo.info, recoveredHLCInfo.info);
            }
            else
            {
                RecoverHybridLogFromSnapshotFile(recoveredICInfo.info, recoveredHLCInfo.info);
            }


            // Read appropriate hybrid log pages into memory
            hlog.RestoreHybridLog(recoveredHLCInfo.info.finalLogicalAddress, recoveredHLCInfo.info.headAddress, recoveredHLCInfo.info.beginAddress);

            // Recover session information
            _recoveredSessions = recoveredHLCInfo.info.continueTokens;
        }

        private void RecoverHybridLog(IndexRecoveryInfo indexRecoveryInfo,
                                        HybridLogRecoveryInfo recoveryInfo)
        {
            var fromAddress = indexRecoveryInfo.startLogicalAddress;
            var untilAddress = recoveryInfo.finalLogicalAddress;

            var startPage = hlog.GetPage(fromAddress);
            var endPage = hlog.GetPage(untilAddress);
            if ((untilAddress > hlog.GetStartLogicalAddress(endPage)) && (untilAddress > fromAddress))
            {
                endPage++;
            }

            // By default first page has one extra record
            var capacity = hlog.GetCapacityNumPages();
            var recoveryStatus = new RecoveryStatus(capacity, startPage, endPage, untilAddress);

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

                // OS thread flushes current page and issues a read request if necessary
                recoveryStatus.readStatus[pageIndex] = ReadStatus.Pending;
                recoveryStatus.flushStatus[pageIndex] = FlushStatus.Pending;

                hlog.AsyncFlushPages(page, 1, AsyncFlushPageCallbackForRecovery, recoveryStatus);
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

        private void RecoverHybridLogFromSnapshotFile(
                                        IndexRecoveryInfo indexRecoveryInfo,
                                        HybridLogRecoveryInfo recoveryInfo)
        {
            var fileStartAddress = recoveryInfo.flushedLogicalAddress;
            var fromAddress = indexRecoveryInfo.startLogicalAddress;
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
            var recoveryStatus = new RecoveryStatus(capacity, startPage, endPage, untilAddress)
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

                // Ensure the page is read from file
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

            recoveryStatus.recoveryDevice.Close();
            recoveryStatus.objectLogRecoveryDevice.Close();
        }

        private void RecoverFromPage(long startRecoveryAddress,
                                     long fromLogicalAddressInPage,
                                     long untilLogicalAddressInPage,
                                     long pageLogicalAddress,
                                     long pagePhysicalAddress,
                                     int version)
        {
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
                    if (FoldOverSnapshot)
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
        /// <param name="untilAddress"></param>
        /// <param name="headAddress"></param>
        /// <param name="beginAddress"></param>
        public void RestoreHybridLog(long untilAddress, long headAddress, long beginAddress)
        {
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
                var tailPage = GetPage(untilAddress);
                var headPage = GetPage(headAddress);

                var recoveryStatus = new RecoveryStatus(GetCapacityNumPages(), headPage, tailPage, untilAddress);
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

            RecoveryReset(untilAddress, headAddress, beginAddress);
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
