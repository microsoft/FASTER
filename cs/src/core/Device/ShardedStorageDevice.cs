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
            } 

        }

        public int MapRangeToShard(long startAddress, long endAddress)
        {
        }
    }
    class ShardedStorageDevice : StorageDeviceBase
    {
        private readonly IList<IDevice> devices;

        public override void Close()
        {
            throw new NotImplementedException();
        }

        public override void ReadAsync(int segmentId, ulong sourceAddress, IntPtr destinationAddress, uint readLength, IOCompletionCallback callback, IAsyncResult asyncResult)
        {
            throw new NotImplementedException();
        }

        public override void RemoveSegmentAsync(int segment, AsyncCallback callback, IAsyncResult result)
        {
            throw new NotImplementedException();
        }

        public override void WriteAsync(IntPtr sourceAddress, int segmentId, ulong destinationAddress, uint numBytesToWrite, IOCompletionCallback callback, IAsyncResult asyncResult)
        {
            throw new NotImplementedException();
        }
    }
}
