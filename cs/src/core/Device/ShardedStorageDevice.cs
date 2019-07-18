using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading;

namespace FASTER.core
{
    /// <summary>
    /// Interface that encapsulates a sharding strategy that is used by <see cref="ShardedStorageDevice"/>. This
    /// allows users to customize their sharding behaviors. Some default implementations are supplied for common
    /// partitioning schemes.
    /// </summary>
    interface IPartitionScheme
    {
        /// <summary>
        /// A list of <see cref="IDevice"/> that represents the shards. Indexes into this list will be
        /// used as unique identifiers for the shards.
        /// </summary>
        IList<IDevice> Devices { get; }
        /// <summary>
        /// Maps a range in the unified logical address space into a contiguous physical chunk on a shard's address space.
        /// Because the given range may be sharded across multiple devices, only the largest contiguous chunk starting from
        /// start address but smaller than end address is returned in shard, shardStartAddress, and shardEndAddress.
        /// </summary>
        /// <param name="startAddress">start address of the range to map in the logical address space</param>
        /// <param name="endAddress">end address of the range to map in the logical address space</param>
        /// <param name="shard"> the shard (potentially part of) the given range resides in, given as index into <see cref="devices"/></param>
        /// <param name="shardStartAddress"> start address translated into physical start address on the returned shard </param>
        /// <param name="shardEndAddress">
        /// physical address of the end of the part of the range on the returned shard. This is not necessarily a translation of the end address
        /// given, as the tail of the range maybe on (a) different device(s).
        /// </param>
        /// <returns>
        /// the logical address translated from the returned shardEndAddress. If this is not equal to the given end address, the caller is
        /// expected to repeatedly call this method using the returned value as the new startAddress until the entire original range is
        /// covered.
        /// </returns>
        long MapRange(long startAddress, long endAddress, out int shard, out long shardStartAddress, out long shardEndAddress);
    }

    class RoundRobinPartitionScheme : IPartitionScheme
    {
        public IList<IDevice> Devices { get; }
        private readonly long chunkSize;

        public RoundRobinPartitionScheme(long chunkSize, IList<IDevice> devices)
        {
            Debug.Assert(devices.Count != 0, "There cannot be zero shards");
            Debug.Assert(chunkSize > 0, "chunk size should not be negative");
            this.Devices = devices;
            this.chunkSize = chunkSize;
            foreach (IDevice device in Devices)
            {
                Debug.Assert(chunkSize % device.SectorSize == 0, "A single device sector cannot be partitioned");
            }
        }

        public long MapRange(long startAddress, long endAddress, out int shard, out long shardStartAddress, out long shardEndAddress)
        {
            // TODO(Tianyu): Can do bitshift magic for faster translation if we enforce that chunk size is a multiple of 2 (which it definitely should be)
            long chunkId = startAddress / chunkSize;
            shard = (int)(chunkId % Devices.Count);
            shardStartAddress = startAddress % chunkSize;
            long chunkEndAddress = (chunkId + 1) * chunkSize;

            if (endAddress > chunkEndAddress)
            {
                shardEndAddress = shardStartAddress + chunkSize;
                return chunkEndAddress;
            }
            else
            {
                shardEndAddress = endAddress - startAddress + shardStartAddress;
                return endAddress;
            }
        }
    }

    class ShardedStorageDevice : StorageDeviceBase
    {
        private readonly IPartitionScheme partitions;

        // TODO(Tianyu): Fill with actual params to base class
        public ShardedStorageDevice(IPartitionScheme partitions) : base("", 512, -1)
        {
            this.partitions = partitions;
        }

        public override void Close()
        {
            foreach (IDevice device in partitions.Devices)
            {
                device.Close();
            }
        }

        public override void RemoveSegmentAsync(int segment, AsyncCallback callback, IAsyncResult result)
        {
            var countdown = new CountdownEvent(partitions.Devices.Count);
            foreach (IDevice shard in partitions.Devices)
            {
                shard.RemoveSegmentAsync(segment, ar =>
                {
                    if (countdown.Signal()) callback(ar);
                    countdown.Dispose();
                }, result);
            }
        }

        public unsafe override void WriteAsync(IntPtr sourceAddress, int segmentId, ulong destinationAddress, uint numBytesToWrite, IOCompletionCallback callback, IAsyncResult asyncResult)
        {
            // Starts off in one, in order to prevent some issued writes calling the callback before all parallel writes are issued.
            var countdown = new CountdownEvent(1);
            long currentWriteStart = (long)destinationAddress;
            long writeEnd = currentWriteStart + (long)numBytesToWrite;
            uint aggregateErrorCode = 0;
            while (currentWriteStart < writeEnd)
            {
                long newStart = partitions.MapRange(currentWriteStart, writeEnd, out int shard, out long shardStartAddress, out long shardEndAddress);
                ulong writeOffset = (ulong)currentWriteStart - destinationAddress;
                // Indicate that there is one more task to wait for
                countdown.AddCount();
                // Because more than one device can return with an error, it is important that we remember the most recent error code we saw. (It is okay to only
                // report one error out of many. It will be as if we failed on that error and cancelled all other reads, even though we issue reads in parallel and
                // wait until all of them are complete in the implementation) 
                // TODO(Tianyu): Can there be races on async result as we issue writes or reads in parallel?
                partitions.Devices[shard].WriteAsync(IntPtr.Add(sourceAddress, (int)writeOffset),
                                                     segmentId,
                                                     (ulong)shardStartAddress,
                                                     (uint)(shardEndAddress - shardStartAddress),
                                                     (e, n, o) =>
                                                     {
                                                         // TODO(Tianyu): this is incorrect if returned "bytes" written is allowed to be less than requested like POSIX.
                                                         if (e != 0) aggregateErrorCode = e;
                                                         if (countdown.Signal()) callback(aggregateErrorCode, n, o);
                                                         countdown.Dispose();
                                                     },
                                                     asyncResult);

                currentWriteStart = newStart;
            }

            // TODO(Tianyu): What do for the dumb overlapped wrapper...
            if (countdown.Signal()) callback(aggregateErrorCode, 0, null);
        }

        public unsafe override void ReadAsync(int segmentId, ulong sourceAddress, IntPtr destinationAddress, uint readLength, IOCompletionCallback callback, IAsyncResult asyncResult)
        {
            // Starts off in one, in order to prevent some issued writes calling the callback before all parallel writes are issued.
            var countdown = new CountdownEvent(1);
            long currentReadStart = (long)sourceAddress;
            long readEnd = currentReadStart + readLength;
            uint aggregateErrorCode = 0;
            while (currentReadStart < readEnd)
            {
                long newStart = partitions.MapRange(currentReadStart, readEnd, out int shard, out long shardStartAddress, out long shardEndAddress);
                ulong writeOffset = (ulong)currentReadStart - sourceAddress;
                // Because more than one device can return with an error, it is important that we remember the most recent error code we saw. (It is okay to only
                // report one error out of many. It will be as if we failed on that error and cancelled all other reads, even though we issue reads in parallel and
                // wait until all of them are complete in the implementation) 
                // TODO(Tianyu): Can there be races on async result as we issue writes or reads in parallel?
                countdown.AddCount();
                partitions.Devices[shard].ReadAsync(segmentId,
                                                    (ulong)currentReadStart,
                                                    IntPtr.Add(destinationAddress, (int)writeOffset),
                                                    (uint)(shardEndAddress - shardStartAddress),
                                                    (e, n, o) =>
                                                    {
                                                        // TODO(Tianyu): this is incorrect if returned "bytes" written is allowed to be less than requested like POSIX.
                                                        if (e != 0) aggregateErrorCode = e;
                                                        if (countdown.Signal()) callback(aggregateErrorCode, n, o);
                                                        countdown.Dispose();
                                                    },
                                                    asyncResult);

                currentReadStart = newStart;
            }

            // TODO(Tianyu): What do for the dumb overlapped wrapper...
            if (countdown.Signal()) callback(aggregateErrorCode, 0, null);
        }
    }
}
