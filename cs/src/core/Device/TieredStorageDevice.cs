using System;
using System.Collections.Generic;
using System.Text;
using System.Diagnostics;
using System.Threading;

namespace FASTER.core.Device
{
    class TieredStorageDevice : StorageDeviceBase
    {
        private readonly IList<IDevice> devices;
        private readonly int commitPoint;
        // Because it is assumed that tiers are inclusive with one another, we only need to store the starting address of the log portion avialable on each tier.
        // That implies this list is sorted in descending order with the last tier being 0 always.
        private readonly ulong[] tierStartAddresses;

        // TODO(Tianyu): So far, I don't believe sector size is used anywhere in the code. Therefore I am not reasoning about what the
        // sector size of a tiered storage should be when different tiers can have different sector sizes.
        /// <summary>
        /// Constructs a new TieredStorageDevice composed of the given devices.
        /// </summary>
        /// <param name="devices">
        /// List of devices to be used. The list should be given in order of hot to cold. Read is served from the
        /// device with smallest index in the list that has the requested data
        /// </param>
        /// <param name="commitPoint"></param>
        public TieredStorageDevice(int commitPoint, IList<IDevice> devices) : base(ComputeFileString(devices, commitPoint), 512, ComputeCapacity(devices))
        {
            Debug.Assert(commitPoint >= 0 && commitPoint < devices.Count, "commit point is out of range");
            this.devices = devices;
            this.commitPoint = commitPoint;
            tierStartAddresses = Array.CreateInstance(typeof(IDevice), devices.Count);
            tierStartAddresses.Initialize();
        }

        public TieredStorageDevice(int commitPoint, params IDevice[] devices) : this(commitPoint, (IList<IDevice>)devices)
        {
        }

        public override void Close()
        {
            foreach (IDevice device in devices)
            {
                device.Close();
            }
        }

        public override void DeleteAddressRange(long fromAddress, long toAddress)
        {
            // TODO(Tianyu): concurrency
            int fromStartTier = FindClosestDeviceContaining(fromAddress);
            int toStartTier = FindClosestDeviceContaining(toAddress);
            for (int i = fromStartTier; i < toStartTier; i++)
            {
                // Because our tiered storage is inclusive, 
                devices[i].DeleteAddressRange(Math.Max(fromAddress, tierStartAddresses[i]), toAddress);
            }
        }

        public override void DeleteSegmentRange(int fromSegment, int toSegment)
        {
            throw new NotSupportedException();
        }

        public override void ReadAsync(ulong alignedSourceAddress, IntPtr aligneDestinationAddress, uint alignedReadLength, IOCompletionCallback callback, IAsyncResult asyncResulte)
        {
            // TODO(Tianyu): This whole operation needs to be thread-safe with concurrent calls to writes, which may trigger a change in start address.
            IDevice closestDevice = devices[FindClosestDeviceContaining(alignedSoourceAddress)];
            // We can directly forward the address, because assuming an inclusive policy, all devices agree on the same address space. The only difference is that some segments may not
            // be present for certain devices. 
            closestDevice.ReadAsync(alignedSourceAddress, alignedDestinationAddress, alignedReadLength, callback, asyncResulte);
        }

        public override void ReadAsync(int segmentId, ulong sourceAddress, IntPtr destinationAddress, uint readLength, IOCompletionCallback callback, IAsyncResult asyncResult)
        {
            // If it is not guaranteed that all underlying tiers agree on a segment size, this API cannot have a meaningful implementation
            throw new NotSupportedException();
        }

        public override void WriteAsync(IntPtr sourceAddress, ulong alignedDestinationAddress, uint numBytesToWrite, IOCompletionCallback callback, IAsyncResult asyncResult)
        {
            int startTier = FindClosestDeviceContaining(alignedDestinationAddress);
            // TODO(Tianyu): Can you ever initiate a write that is after the commit point? Given FASTER's model of a read-only region, this will probably never happen.
            Debug.Assert(startTier >= commitPoint, "Write should not elide the commit point");
            // TODO(Tianyu): This whole operation needs to be thread-safe with concurrent calls to reads.
            for (int i = startTier; i < devices.Count; i++)
            {
                if (i == commitPoint)
                {
                    // Only if the write is complete on the commit point should we invoke the call back.
                    devices[i].WriteAsync(sourceAddress, alignedDestinationAddress, numBytesToWrite, callback, asyncResult);
                }
                else
                {
                    // Otherwise, simply issue the write without caring about callbacks
                    devices[i].WriteAsync(sourceAddress, alignedDestinationAddress, numBytesToWrite, (e, n, o) => { }, null);
                }
            }
        }

        public override void WriteAsync(IntPtr sourceAddress, int segmentId, ulong destinationAddress, uint numBytesToWrite, IOCompletionCallback callback, IAsyncResult asyncResult)
        {
            // If it is not guaranteed that all underlying tiers agree on a segment size, this API cannot have a meaningful implementation
            throw new NotSupportedException();
        }

        private static ulong ComputeCapacity(IList<IDevice> devices)
        {
            ulong result = 0;
            // The capacity of a tiered storage device is the sum of the capacity of its tiers
            foreach (IDevice device in devices)
            {
                // Unless the last tier device has unspecified storage capacity, in which case the tiered storage also has unspecified capacity
                if (device.Capacity == CAPACITY_UNSPECIFIED)
                {
                    // TODO(Tianyu): Is this assumption too strong?
                    Debug.Assert(device == devices[devices.Count - 1], "Only the last tier storage of a tiered storage device can have unspecified capacity");
                    return CAPACITY_UNSPECIFIED;
                }
                result += device.Capacity;
            }
            return result;
        }

        // TODO(Tianyu): Is the notion of file name still relevant in a tiered storage device?
        private static string ComputeFileString(IList<IDevice> devices, int commitPoint)
        {
            StringBuilder result = new StringBuilder();
            foreach (IDevice device in devices)
            {
                result.AppendFormat("{0}, file name {1}, capacity {2} bytes;", device.GetType().Name, device.FileName, device.Capacity == CAPACITY_UNSPECIFIED ? "unspecified" : device.Capacity.ToString());
            }
            result.AppendFormat("commit point: {0} at tier {1}", devices[commitPoint].GetType().Name, commitPoint);
            return result.ToString();
        }

        private int FindClosestDeviceContaining(ulong address)
        {
            // TODO(Tianyu): Will linear search be faster for small number of tiers (which would be the common case)?
            // binary search where the array is sorted in reverse order to the default ulong comparator
            int tier = Array.BinarySearch(tierStartAddresses, 0, tierStartAddresses.Length, alignedStartAddress, (x, y) => y.CompareTo(x));
            // Binary search returns either the index or bitwise complement of the index of the first element smaller than start address.
            // We want the first element with start address smaller than given address. 
            return tier >= 0 ? ++tier : ~tier;
        }
    }
}
