// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma warning disable 0162

using System;
using System.Runtime.CompilerServices;

namespace FASTER.core
{
    /// <summary>
    /// Wrapper to process log-related commands
    /// </summary>
    /// <typeparam name="Key"></typeparam>
    /// <typeparam name="Value"></typeparam>
    public sealed class LogAccessor<Key, Value> : IObservable<IFasterScanIterator<Key, Value>>
        where Key : new()
        where Value : new()
    {
        private readonly FasterKV<Key, Value> fht;
        private readonly AllocatorBase<Key, Value> allocator;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="fht"></param>
        /// <param name="allocator"></param>
        public LogAccessor(FasterKV<Key, Value> fht, AllocatorBase<Key, Value> allocator)
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
            if (!fht.epoch.ThisInstanceProtected())
            {
                fht.epoch.Resume();
                var updatedHeadAddress = allocator.ShiftHeadAddress(newHeadAddress);
                fht.epoch.Suspend();
                return updatedHeadAddress >= newHeadAddress;
            }
            else
            {
                var updatedHeadAddress = allocator.ShiftHeadAddress(newHeadAddress);
                return updatedHeadAddress >= newHeadAddress;
            }
        }

        /// <summary>
        /// Subscribe to records (in batches) as they become read-only in the log
        /// Currently, we support only one subscriber to the log (easy to extend)
        /// Subscriber only receives new log updates from the time of subscription onwards
        /// To scan the historical part of the log, use the Scan(...) method
        /// </summary>
        /// <param name="readOnlyObserver">Observer to which scan iterator is pushed</param>
        public IDisposable Subscribe(IObserver<IFasterScanIterator<Key, Value>> readOnlyObserver)
        {
            allocator.OnReadOnlyObserver = readOnlyObserver;
            return new LogSubscribeDisposable(allocator);
        }

        /// <summary>
        /// Wrapper to help dispose the subscription
        /// </summary>
        class LogSubscribeDisposable : IDisposable
        {
            private readonly AllocatorBase<Key, Value> allocator;

            public LogSubscribeDisposable(AllocatorBase<Key, Value> allocator)
            {
                this.allocator = allocator;
            }

            public void Dispose()
            {
                allocator.OnReadOnlyObserver = null;
            }
        }

        /// <summary>
        /// Shift log read-only address
        /// </summary>
        /// <param name="newReadOnlyAddress">Address to shift read-only until</param>
        /// <param name="wait">Wait to ensure shift is complete (may involve page flushing)</param>
        public void ShiftReadOnlyAddress(long newReadOnlyAddress, bool wait)
        {
            if (!fht.epoch.ThisInstanceProtected())
            {
                fht.epoch.Resume();
                allocator.ShiftReadOnlyAddress(newReadOnlyAddress);
                fht.epoch.Suspend();

                // Wait for flush to complete
                while (wait && allocator.FlushedUntilAddress < newReadOnlyAddress) ;
            }
            else
            {
                allocator.ShiftReadOnlyAddress(newReadOnlyAddress);

                // Wait for flush to complete
                while (wait && allocator.FlushedUntilAddress < newReadOnlyAddress)
                    fht.epoch.ProtectAndDrain();
            }
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
        /// records to the tail of the log. 
        /// Uses default compaction functions that only deletes explicitly deleted records, copying is implemeted by shallow copying values from source to destination.
        /// </summary>
        /// <param name="untilAddress"></param>
        public void Compact(long untilAddress)
        {
            if (allocator is VariableLengthBlittableAllocator<Key, Value> varLen)
            {
                var functions = new LogVariableCompactFunctions<Key, Value, DefaultVariableCompactionFunctions<Key, Value>>(varLen, default);
                var variableLengthStructSettings = new VariableLengthStructSettings<Key, Value>
                {
                    keyLength = varLen.KeyLength,
                    valueLength = varLen.ValueLength,
                };

                Compact(functions, default(DefaultVariableCompactionFunctions<Key, Value>), untilAddress, variableLengthStructSettings);
            }
            else
            {
                Compact(new LogCompactFunctions<Key, Value, DefaultCompactionFunctions<Key, Value>>(default), default(DefaultCompactionFunctions<Key, Value>), untilAddress, null);
            }
        }

        /// <summary>
        /// Compact the log until specified address, moving active
        /// records to the tail of the log.
        /// </summary>
        /// <param name="compactionFunctions">User provided compaction functions (see <see cref="ICompactionFunctions{Key, Value}"/>).</param>
        /// <param name="untilAddress"></param>
        public void Compact<CompactionFunctions>(CompactionFunctions compactionFunctions, long untilAddress)
            where CompactionFunctions : ICompactionFunctions<Key, Value>
        {
            if (allocator is VariableLengthBlittableAllocator<Key, Value> varLen)
            {
                var functions = new LogVariableCompactFunctions<Key, Value, CompactionFunctions>(varLen, compactionFunctions);
                var variableLengthStructSettings = new VariableLengthStructSettings<Key, Value>
                {
                    keyLength = varLen.KeyLength,
                    valueLength = varLen.ValueLength,
                };

                Compact(functions, compactionFunctions, untilAddress, variableLengthStructSettings);
            }
            else
            {
                Compact(new LogCompactFunctions<Key, Value, CompactionFunctions>(compactionFunctions), compactionFunctions, untilAddress, null);
            }
        }

        private unsafe void Compact<Functions, CompactionFunctions>(Functions functions, CompactionFunctions cf, long untilAddress, VariableLengthStructSettings<Key, Value> variableLengthStructSettings)
            where Functions : IFunctions<Key, Value, Empty, Empty, Empty>
            where CompactionFunctions : ICompactionFunctions<Key, Value>
        {
            var originalUntilAddress = untilAddress;

            using (var fhtSession = fht.NewSession<Empty, Empty, Empty, Functions>(functions))
            using (var tempKv = new FasterKV<Key, Value>(fht.IndexSize, new LogSettings(), comparer: fht.Comparer, variableLengthStructSettings: variableLengthStructSettings))
            using (var tempKvSession = tempKv.NewSession<Empty, Empty, Empty, Functions>(functions))
            {
                using (var iter1 = fht.Log.Scan(fht.Log.BeginAddress, untilAddress))
                {
                    while (iter1.GetNext(out var recordInfo))
                    {
                        ref var key = ref iter1.GetKey();
                        ref var value = ref iter1.GetValue();

                        if (recordInfo.Tombstone || cf.IsDeleted(key, value))
                            tempKvSession.Delete(ref key, default, 0);
                        else
                            tempKvSession.Upsert(ref key, ref value, default, 0);
                    }
                }

                // TODO: Scan until SafeReadOnlyAddress
                var scanUntil = untilAddress;
                LogScanForValidity(ref untilAddress, ref scanUntil, tempKvSession);

                // Make sure key wasn't inserted between SafeReadOnlyAddress and TailAddress
                using (var iter3 = tempKv.Log.Scan(tempKv.Log.BeginAddress, tempKv.Log.TailAddress))
                {
                    while (iter3.GetNext(out var recordInfo))
                    {
                        ref var key = ref iter3.GetKey();
                        ref var value = ref iter3.GetValue();

                        if (!recordInfo.Tombstone)
                        {
                            if (fhtSession.ContainsKeyInMemory(ref key, scanUntil) == Status.NOTFOUND)
                            {
                                // Check if recordInfo point to the newest record.
                                // With #164 it is possible that tempKv might have multiple records with the same
                                // key (ConcurrentWriter returns false). For this reason check the index
                                // whether the actual record has the same address (or maybe even deleted).
                                // If this is too much of a performance hit - we could try and add additional info
                                // to the recordInfo to indicate that it was replaced (but it would only for tempKv 
                                // not general case).
                                var bucket = default(HashBucket*);
                                var slot = default(int);

                                var hash = tempKv.Comparer.GetHashCode64(ref key);
                                var tag = (ushort)((ulong)hash >> Constants.kHashTagShift);

                                var entry = default(HashBucketEntry);
                                if (tempKv.FindTag(hash, tag, ref bucket, ref slot, ref entry) && entry.Address == iter3.CurrentAddress)
                                    fhtSession.Upsert(ref key, ref value, default, 0);
                            }
                        }
                        if (scanUntil < fht.Log.SafeReadOnlyAddress)
                        {
                            LogScanForValidity(ref untilAddress, ref scanUntil, tempKvSession);
                        }
                    }
                }
            }

            ShiftBeginAddress(originalUntilAddress);
        }

        private void LogScanForValidity<Input, Output, Context, Functions>(ref long untilAddress, ref long scanUntil, ClientSession<Key, Value, Input, Output, Context, Functions> tempKvSession)
            where Functions : IFunctions<Key, Value, Input, Output, Context>
        {
            while (scanUntil < fht.Log.SafeReadOnlyAddress)
            {
                untilAddress = scanUntil;
                scanUntil = fht.Log.SafeReadOnlyAddress;
                using (var iter2 = fht.Log.Scan(untilAddress, scanUntil))
                {
                    while (iter2.GetNext(out var _))
                    {
                        ref var key = ref iter2.GetKey();
                        ref var value = ref iter2.GetValue();

                        tempKvSession.Delete(ref key, default, 0);
                    }
                }
            }
        }
    }
}
