// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma warning disable 0162


using System;

namespace FASTER.core
{
    /// <summary>
    /// Wrapper to process log-related commands
    /// </summary>
    /// <typeparam name="Key"></typeparam>
    /// <typeparam name="Value"></typeparam>
    /// <typeparam name="Input"></typeparam>
    /// <typeparam name="Output"></typeparam>
    /// <typeparam name="Context"></typeparam>
    public class LogAccessor<Key, Value, Input, Output, Context>
        where Key : new()
        where Value : new()
    {
        private readonly IFasterKV<Key, Value, Input, Output, Context> fht;
        private readonly AllocatorBase<Key, Value> allocator;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="fht"></param>
        /// <param name="allocator"></param>
        public LogAccessor(IFasterKV<Key, Value, Input, Output, Context> fht, AllocatorBase<Key, Value> allocator)
        {
            this.fht = fht;
            this.allocator = allocator;
        }

        /// <summary>
        /// Tail address of log
        /// </summary>
        public long TailAddress => allocator.GetTailAddress();

        /// <summary>
        /// Read-only address of log, i.e. boundary between read-only region and mutable region
        /// </summary>
        public long ReadOnlyAddress => allocator.ReadOnlyAddress;

        /// <summary>
        /// Safe read-only address of log, i.e. boundary between read-only region and mutable region
        /// </summary>
        public long SafeReadOnlyAddress => allocator.SafeReadOnlyAddress;

        /// <summary>
        /// Head address of log, i.e. beginning of in-memory regions
        /// </summary>
        public long HeadAddress => allocator.HeadAddress;

        /// <summary>
        /// Beginning address of log
        /// </summary>
        public long BeginAddress => allocator.BeginAddress;

        /// <summary>
        /// Truncate the log until, but not including, untilAddress
        /// </summary>
        /// <param name="untilAddress"></param>
        public void ShiftBeginAddress(long untilAddress)
        {
            allocator.ShiftBeginAddress(untilAddress);
        }


        /// <summary>
        /// Shift log head address to prune memory foorprint of hybrid log
        /// </summary>
        /// <param name="newHeadAddress">Address to shift head until</param>
        /// <param name="wait">Wait to ensure shift is registered (may involve page flushing)</param>
        /// <returns>When wait is false, this tells whether the shift to newHeadAddress was successfully registered with FASTER</returns>
        public bool ShiftHeadAddress(long newHeadAddress, bool wait)
        {
            // First shift read-only
            ShiftReadOnlyAddress(newHeadAddress, wait);

            // Then shift head address
            var updatedHeadAddress = allocator.ShiftHeadAddress(newHeadAddress);

            return updatedHeadAddress >= newHeadAddress;
        }

        /// <summary>
        /// Shift log read-only address
        /// </summary>
        /// <param name="newReadOnlyAddress">Address to shift read-only until</param>
        /// <param name="wait">Wait to ensure shift is complete (may involve page flushing)</param>
        public void ShiftReadOnlyAddress(long newReadOnlyAddress, bool wait)
        {
            allocator.ShiftReadOnlyAddress(newReadOnlyAddress);

            // Wait for flush to complete
            while (wait && allocator.FlushedUntilAddress < newReadOnlyAddress)
                fht.Refresh();
        }

        /// <summary>
        /// Scan the log given address range
        /// </summary>
        /// <param name="beginAddress"></param>
        /// <param name="endAddress"></param>
        /// <param name="scanBufferingMode"></param>
        /// <returns></returns>
        public IFasterScanIterator<Key, Value> Scan(long beginAddress, long endAddress, ScanBufferingMode scanBufferingMode = ScanBufferingMode.DoublePageBuffering)
        {
            return allocator.Scan(beginAddress, endAddress, scanBufferingMode);
        }

        /// <summary>
        /// Flush log until current tail (records are still retained in memory)
        /// </summary>
        /// <param name="wait">Synchronous wait for operation to complete</param>
        public void Flush(bool wait)
        {
            ShiftReadOnlyAddress(allocator.GetTailAddress(), wait);
        }

        /// <summary>
        /// Flush log and evict all records from memory
        /// </summary>
        /// <param name="wait">Synchronous wait for operation to complete</param>
        /// <returns>When wait is false, this tells whether the full eviction was successfully registered with FASTER</returns>
        public bool FlushAndEvict(bool wait)
        {
            return ShiftHeadAddress(allocator.GetTailAddress(), wait);
        }

        /// <summary>
        /// Delete log entirely from memory. Cannot allocate on the log
        /// after this point. This is a synchronous operation.
        /// </summary>
        public void DisposeFromMemory()
        {
            // Ensure we have flushed and evicted
            FlushAndEvict(true);

            // Delete from memory
            allocator.DeleteFromMemory();
        }

        /// <summary>
        /// Compact the log until specified address, moving active
        /// records to the tail of the log
        /// </summary>
        /// <param name="untilAddress"></param>
        public void Compact(long untilAddress)
        {
            long originalUntilAddress = untilAddress;

            var tempKv = new FasterKV<Key, Value, Input, Output, Context, LogCompactFunctions>
                (fht.IndexSize, new LogCompactFunctions(), new LogSettings(), comparer: fht.Comparer);
            tempKv.StartSession();

            int cnt = 0;

            using (var iter1 = fht.Log.Scan(fht.Log.BeginAddress, untilAddress))
            {
                while (iter1.GetNext(out RecordInfo recordInfo, out Key key, out Value value))
                {
                    if (recordInfo.Tombstone)
                        tempKv.Delete(ref key, default(Context), 0);
                    else
                        tempKv.Upsert(ref key, ref value, default(Context), 0);

                    if (++cnt % 1000 == 0)
                    {
                        fht.Refresh();
                        tempKv.Refresh();
                    }
                }
            }

            // TODO: Scan until SafeReadOnlyAddress
            long scanUntil = untilAddress;
            LogScanForValidity(ref untilAddress, ref scanUntil, ref tempKv);

            // Make sure key wasn't inserted between SafeReadOnlyAddress and TailAddress

            cnt = 0;
            using (var iter3 = tempKv.Log.Scan(tempKv.Log.BeginAddress, tempKv.Log.TailAddress))
            {
                while (iter3.GetNext(out RecordInfo recordInfo, out Key key, out Value value))
                {
                    if (!recordInfo.Tombstone)
                    {
                        if (fht.ContainsKeyInMemory(ref key, scanUntil) == Status.NOTFOUND)
                            fht.Upsert(ref key, ref value, default(Context), 0);
                    }
                    if (++cnt % 1000 == 0)
                    {
                        fht.Refresh();
                        tempKv.Refresh();
                    }
                    if (scanUntil < fht.Log.SafeReadOnlyAddress)
                    {
                        LogScanForValidity(ref untilAddress, ref scanUntil, ref tempKv);
                    }
                }
            }
            tempKv.StopSession();
            tempKv.Dispose();

            ShiftBeginAddress(originalUntilAddress);
        }

        private void LogScanForValidity(ref long untilAddress, ref long scanUntil, ref FasterKV<Key, Value, Input, Output, Context, LogCompactFunctions> tempKv)
        {
            while (scanUntil < fht.Log.SafeReadOnlyAddress)
            {
                untilAddress = scanUntil;
                scanUntil = fht.Log.SafeReadOnlyAddress;
                int cnt = 0;
                using (var iter2 = fht.Log.Scan(untilAddress, scanUntil))
                {
                    while (iter2.GetNext(out RecordInfo recordInfo, out Key key, out Value value))
                    {
                        tempKv.Delete(ref key, default(Context), 0);

                        if (++cnt % 1000 == 0)
                        {
                            fht.Refresh();
                            tempKv.Refresh();
                        }
                    }
                }
                fht.Refresh();
            }
        }

        private class LogCompactFunctions : IFunctions<Key, Value, Input, Output, Context>
        {
            public void CheckpointCompletionCallback(Guid sessionId, long serialNum) { }
            public void ConcurrentReader(ref Key key, ref Input input, ref Value value, ref Output dst) { }
            public void ConcurrentWriter(ref Key key, ref Value src, ref Value dst) { dst = src; }
            public void CopyUpdater(ref Key key, ref Input input, ref Value oldValue, ref Value newValue) { }
            public void InitialUpdater(ref Key key, ref Input input, ref Value value) { }
            public void InPlaceUpdater(ref Key key, ref Input input, ref Value value) { }
            public void ReadCompletionCallback(ref Key key, ref Input input, ref Output output, Context ctx, Status status) { }
            public void RMWCompletionCallback(ref Key key, ref Input input, Context ctx, Status status) { }
            public void SingleReader(ref Key key, ref Input input, ref Value value, ref Output dst) { }
            public void SingleWriter(ref Key key, ref Value src, ref Value dst) { dst = src; }
            public void UpsertCompletionCallback(ref Key key, ref Value value, Context ctx) { }
            public void DeleteCompletionCallback(ref Key key, Context ctx) { }
        }
    }
}
