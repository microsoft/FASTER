// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma warning disable 0162

using System;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;

namespace FASTER.core
{
    public unsafe partial class PersistentMemoryMalloc : IAllocator
    {
        public void AsyncReadPageFromDiskRecovery<TContext>(
                                        long readPageStart, 
                                        int numPages, 
                                        IOCompletionCallback callback, 
                                        TContext context,
                                        long recoveryDevicePageOffset = 0,
                                        IDevice recoveryDevice = null)
        {
            if (Key.HasObjectsToSerialize() || Value.HasObjectsToSerialize())
            {
                throw new Exception("Reading pages with object log not yet supported");
            }

            for (long readPage = readPageStart; readPage < (readPageStart + numPages); readPage++)
            {
                int pageIndex = (int)(readPage % BufferSize);
                if (values[pageIndex] == null)
                {
                    // Allocate a new page
                    AllocatePage(pageIndex);
                }
                else
                {
                    //Clear an old used page
                    Array.Clear(values[pageIndex], 0, values[pageIndex].Length);
                }
                var asyncResult = new PageAsyncReadResult<TContext>()
                {
                    page = readPage,
                    context = context
                };

                if(recoveryDevice == null)
                {
                    device.ReadAsync((ulong)(AlignedPageSizeBytes * readPage),
                                             pointers[pageIndex],
                                             PageSize,
                                             callback,
                                             asyncResult);
                }
                else
                {
                    ulong offsetInFile = (ulong)(AlignedPageSizeBytes * (readPage - recoveryDevicePageOffset));
                    recoveryDevice.ReadAsync(offsetInFile,
                                            pointers[pageIndex],
                                            PageSize,
                                            callback,
                                            asyncResult);
                }
            }
        }

        public void AsyncFlushPageToDiskRecovery<TContext>(
                                        long flushPageStart, 
                                        int numPages, 
                                        IOCompletionCallback callback, 
                                        TContext context)
        {
            for (long flushPage = flushPageStart; flushPage < (flushPageStart + numPages); flushPage++)
            {
                int pageIndex = GetPageIndexForPage(flushPage);
                var asyncResult = new PageAsyncFlushResult<TContext>()
                {
                    page = flushPage,
                    context = context
                };
                device.WriteAsync(pointers[flushPage % BufferSize],
                                  (ulong)(AlignedPageSizeBytes * flushPage),
                                  PageSize,
                                  callback,
                                  asyncResult);
            }
        }

        public void RecoveryReset(long tailAddress, long headAddress)
        {
            long tailPage = GetPage(tailAddress);
            long offsetInPage = GetOffsetInPage(tailAddress);
            TailPageOffset.Page = (int)tailPage;
            TailPageOffset.Offset = (int)offsetInPage;
            TailPageIndex = GetPageIndexForPage(TailPageOffset.Page);

            // issue read request to all pages until head lag
            HeadAddress = headAddress;
            SafeHeadAddress = headAddress;
            FlushedUntilAddress = headAddress;
            ReadOnlyAddress = tailAddress;
            SafeReadOnlyAddress = tailAddress;
            
            for(var addr = headAddress; addr < tailAddress; addr += PageSize)
            {
                var pageIndex = GetPageIndexForAddress(addr);
                PageStatusIndicator[pageIndex].PageFlushCloseStatus.PageCloseStatus = CloseStatus.Open;
            }
        }
    }

    public unsafe partial class FasterKV
    {
        protected void InternalRecover(Guid indexToken, Guid hybridLogToken)
        {
            _indexCheckpoint.Recover(indexToken);
            _hybridLogCheckpoint.Recover(hybridLogToken);

            var l1 = _indexCheckpoint.info.finalLogicalAddress;
            var l2 = _hybridLogCheckpoint.info.finalLogicalAddress;
            var v = _hybridLogCheckpoint.info.version;
            if (l1 > l2)
            {
                throw new Exception("Cannot recover from (" + indexToken.ToString() + "," + hybridLogToken.ToString() + ") checkpoint pair!\n");
            }

            _systemState.phase = Phase.REST;
            _systemState.version = (v + 1);

            RecoverFuzzyIndex(_indexCheckpoint.info);

            IsFuzzyIndexRecoveryComplete(true);

            DeleteTentativeEntries();

            if(Constants.kFoldOverSnapshot)
            {
                RecoverHybridLog(_indexCheckpoint.info, _hybridLogCheckpoint.info);
            }
            else
            {
                RecoverHybridLogFromSnapshotFile(_indexCheckpoint.info, _hybridLogCheckpoint.info);
            }

            RestoreHybridLog(_hybridLogCheckpoint.info.finalLogicalAddress);
        }

        private void RestoreHybridLog(long untilAddress)
        {
            
            var tailPage = hlog.GetPage(untilAddress);
            var headPage = default(long);
            if(untilAddress > hlog.GetStartLogicalAddress(tailPage))
            {
                headPage = (tailPage + 1) - hlog.GetHeadOffsetLagInPages(); ;
            }
            else
            {
                headPage = tailPage - hlog.GetHeadOffsetLagInPages();
            }
            headPage = headPage > 0 ? headPage : 0;

            var recoveryStatus = new RecoveryStatus(hlog.GetCapacityNumPages(), headPage, tailPage);
            for(int i = 0; i < recoveryStatus.capacity; i++)
            {
                recoveryStatus.readStatus[i] = ReadStatus.Done;
            }

            var numPages = 0;
            for (var page = headPage; page <= tailPage; page++)
            {
                var pageIndex = hlog.GetPageIndexForPage(page);
                recoveryStatus.readStatus[pageIndex] = ReadStatus.Pending;
                numPages++;
            }

            hlog.AsyncReadPageFromDiskRecovery(headPage, numPages, AsyncReadPagesCallback, recoveryStatus);

            var done = false;
            while (!done)
            {
                done = true;
                for (long page = headPage; page <= tailPage; page++)
                {
                    int pageIndex = hlog.GetPageIndexForPage(page);
                    if (recoveryStatus.readStatus[pageIndex] == ReadStatus.Pending)
                    {
                        done = false;
                        break;
                    }
                }
            }

            var headAddress = hlog.GetStartLogicalAddress(headPage);
            if(headAddress == 0)
            {
                headAddress = Constants.kFirstValidAddress;
            }
            hlog.RecoveryReset(untilAddress, headAddress);
        }

        enum ReadStatus { Pending, Done };
        enum FlushStatus { Pending, Done };
        class RecoveryStatus
        {
            public long startPage;
            public long endPage;
            public int capacity;

            public IDevice recoveryDevice;
            public long recoveryDevicePageOffset;

            public ReadStatus[] readStatus;
            public FlushStatus[] flushStatus;

            public RecoveryStatus(int capacity, 
                                  long startPage, 
                                  long endPage)
            {
                this.capacity = capacity;
                this.startPage = startPage;
                this.endPage = endPage;
                readStatus = new ReadStatus[capacity];
                flushStatus = new FlushStatus[capacity];
                for (int i = 0; i < capacity; i++)
                {
                    flushStatus[i] = FlushStatus.Done;
                    readStatus[i] = ReadStatus.Pending;
                }
            }
        }

        protected void RecoverHybridLog(IndexRecoveryInfo indexRecoveryInfo, 
                                        HybridLogRecoveryInfo recoveryInfo)
        {
            var fromAddress = indexRecoveryInfo.startLogicalAddress;
            var untilAddress = recoveryInfo.finalLogicalAddress;

            var startPage = hlog.GetPage(fromAddress);
            var endPage = hlog.GetPage(untilAddress);
            if(untilAddress > hlog.GetStartLogicalAddress(endPage))
            {
                endPage++;
            }

            // By default first page has one extra record
            var capacity = hlog.GetCapacityNumPages();
            var recoveryStatus = new RecoveryStatus(capacity, startPage, endPage);
            
            int totalPagesToRead = (int)(endPage - startPage);
            int numPagesToReadFirst = Math.Min(capacity, totalPagesToRead);

            // Issue request to read pages as much as possible
            hlog.AsyncReadPageFromDiskRecovery(startPage, numPagesToReadFirst, AsyncReadPagesCallback, recoveryStatus);

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

                hlog.AsyncFlushPageToDiskRecovery(page, 1, AsyncFlushPageCallback, recoveryStatus);
            }

            // Assert that all pages have been flushed
            var done = false;
            while(!done)
            {
                done = true;
                for(long page = startPage; page < endPage; page++)
                {
                    int pageIndex = hlog.GetPageIndexForPage(page);
                    if(recoveryStatus.flushStatus[pageIndex] == FlushStatus.Pending)
                    {
                        done = false;
                        break;
                    }
                }
            }

            
        }

        protected void RecoverHybridLogFromSnapshotFile(
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
            var recoveryStatus = new RecoveryStatus(capacity, startPage, endPage);

            recoveryStatus.recoveryDevice = new LocalStorageDevice(DirectoryConfiguration.GetHybridLogCheckpointFileName(recoveryInfo.guid), false, false, true);
            recoveryStatus.recoveryDevicePageOffset = startPage;

            // Initially issue read request for all pages that can be held in memory
            int totalPagesToRead = (int)(endPage - startPage);
            int numPagesToReadFirst = Math.Min(capacity, totalPagesToRead);


            hlog.AsyncReadPageFromDiskRecovery(startPage,
                                            numPagesToReadFirst,
                                            AsyncReadPagesCallback,
                                            recoveryStatus,
                                            recoveryStatus.recoveryDevicePageOffset,
                                            recoveryStatus.recoveryDevice);



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
                if (fromAddress < endLogicalAddress)
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
                    if(fromAddress > startLogicalAddress && fromAddress < endLogicalAddress)
                    {
                        pageFromAddress = hlog.GetOffsetInPage(fromAddress);
                    }

                    var pageUntilAddress = hlog.GetPageSize();
                    if(endLogicalAddress > untilAddress)
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
                hlog.AsyncFlushPageToDiskRecovery(page, 1, AsyncFlushPageCallback, recoveryStatus);
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
        }

        private void RecoverFromPage(long startRecoveryAddress, 
                                     long fromLogicalAddressInPage,
                                     long untilLogicalAddressInPage,
                                     long pageLogicalAddress,
                                     long pagePhysicalAddress,
                                     int version)
        {
            var key = default(Key*);
            var hash = default(long);
            var tag = default(ushort);
            var info = default(RecordInfo*);
            var pointer = default(long);
            var recordStart = default(long);
            var bucket = default(HashBucket*);
            var entry = default(HashBucketEntry);
            var slot = default(int);

            pointer = fromLogicalAddressInPage;
            while (pointer < untilLogicalAddressInPage)
            {
                recordStart = pagePhysicalAddress + pointer;
                info = Layout.GetInfo(recordStart);

                if (info->IsNull())
                {
                    pointer += RecordInfo.GetLength();
                    continue;
                }

                if (!info->Invalid)
                {
                    key = Layout.GetKey(recordStart);
                    hash = Key.GetHashCode(key);
                    tag = (ushort)((ulong)hash >> Constants.kHashTagShift);

                    entry = default(HashBucketEntry);
                    FindOrCreateTag(hash, tag, ref bucket, ref slot, ref entry);
                    
                    if (info->Version <= version)
                    {
                        entry.Address = pageLogicalAddress + pointer;
                        entry.Tag = tag;
                        entry.Pending = false;
                        entry.Tentative = false;
                        bucket->bucket_entries[slot] = entry.word;
                    }
                    else
                    {
                        info->Invalid = true;
                        if(info->PreviousAddress < startRecoveryAddress)
                        {
                            entry.Address = info->PreviousAddress;
                            entry.Tag = tag;
                            entry.Pending = false;
                            entry.Tentative = false;
                            bucket->bucket_entries[slot] = entry.word;
                        } 
                    }
                }
                pointer += Layout.GetPhysicalSize(recordStart);
            }
        }

        private void AsyncReadPagesCallback(uint errorCode, uint numBytes, NativeOverlapped* overlap)
        {
            //Set the page status to flushed
            var result = (PageAsyncReadResult<RecoveryStatus>)Overlapped.Unpack(overlap).AsyncResult;
            try
            {
                if (errorCode != 0)
                {
                    System.Diagnostics.Trace.TraceError("OverlappedStream GetQueuedCompletionStatus error: {0}", errorCode);
                }
            }
            catch (Exception ex)
            {
                System.Diagnostics.Trace.TraceError("Completion Callback error, {0}", ex.Message);
            }
            finally
            {
                int index = hlog.GetPageIndexForPage(result.page);
                result.context.readStatus[index] = ReadStatus.Done;
                Interlocked.MemoryBarrier();
                Overlapped.Free(overlap);
            }
        }

        private void AsyncFlushPageCallback(uint errorCode, uint numBytes,  NativeOverlapped* overlap)
        {
            //Set the page status to flushed
            var result = (PageAsyncFlushResult<RecoveryStatus>)Overlapped.Unpack(overlap).AsyncResult;
            try
            {
                if (errorCode != 0)
                {
                    System.Diagnostics.Trace.TraceError("OverlappedStream GetQueuedCompletionStatus error: {0}", errorCode);
                }
            }
            catch (Exception ex)
            {
                System.Diagnostics.Trace.TraceError("Completion Callback error, {0}", ex.Message);
            }
            finally
            {
                int index = hlog.GetPageIndexForPage(result.page);
                result.context.flushStatus[index] = FlushStatus.Done;
                if(result.page + result.context.capacity < result.context.endPage)
                {
                    long readPage = result.page + result.context.capacity;
                    if(Constants.kFoldOverSnapshot)
                    {
                        hlog.AsyncReadPageFromDiskRecovery(readPage, 1, AsyncReadPagesCallback, result.context);
                    }
                    else
                    {
                        hlog.AsyncReadPageFromDiskRecovery(readPage, 1, AsyncReadPagesCallback, 
                                                            result.context,
                                                            result.context.recoveryDevicePageOffset,
                                                            result.context.recoveryDevice);
                    }
                }
                Interlocked.MemoryBarrier();
                Overlapped.Free(overlap);
            }
        }



    }
}
