// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma warning disable 0162


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
    }
}
