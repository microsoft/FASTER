using System;
using System.Collections.Generic;
using System.Text;
using System.Diagnostics;
using System.Threading;
using System.ComponentModel;

namespace FASTER.core
{
    class TieredStorageDevice : StorageDeviceBase
    {
        private readonly IList<IDevice> devices;
        private readonly int commitPoint;
        // TODO(Tianyu): For some retarded reason Interlocked provides no CompareExchange for unsigned primitives.
        // Because it is assumed that tiers are inclusive with one another, we only need to store the starting address of the log portion avialable on each tier.
        // That implies this list is sorted in descending order with the last tier being 0 always.
        private readonly long[] tierStartAddresses;
        // Because the device has no access to in-memory log tail information, we need to keep track of that ourselves. Currently this is done by keeping a high-water
        // mark of the addresses seen in the WriteAsyncMethod.
        private long logHead;

        // TODO(Tianyu): Not reasoning about what the sector size of a tiered storage should be when different tiers can have different sector sizes.
        /// <summary>
        /// Constructs a new TieredStorageDevice composed of the given devices.
        /// </summary>
        /// <param name="commitPoint">
        /// The index of an <see cref="IDevice">IDevice</see> in <see cref="devices"/>. When a write has been completed on the device,
        /// the write is considered persistent. It is guaranteed that the callback in <see cref="WriteAsync(IntPtr, ulong, uint, IOCompletionCallback, IAsyncResult)"/>
        /// will not be called until the write is completed on the commit point device.
        /// </param>
        /// <param name="devices">
        /// List of devices to be used. The list should be given in order of hot to cold. Read is served from the
        /// device with smallest index in the list that has the requested data
        /// </param>
        // TODO(Tianyu): Recovering from a tiered device is potentially difficult, because we also need to recover their respective ranges.
        public TieredStorageDevice(int commitPoint, IList<IDevice> devices) : base(ComputeFileString(devices, commitPoint), 512, ComputeCapacity(devices))
        {
            Debug.Assert(commitPoint >= 0 && commitPoint < devices.Count, "commit point is out of range");
            // TODO(Tianyu): Should assert that passed in devices are not yet initialized. This is more challenging for recovering.
            this.devices = devices;
            this.commitPoint = commitPoint;
            tierStartAddresses = (long[])Array.CreateInstance(typeof(long), devices.Count);
            tierStartAddresses.Initialize();
            // TODO(Tianyu): Change after figuring out how to deal with recovery.
            logHead = 0;
        }

        /// <summary>
        /// Constructs a new TieredStorageDevice composed of the given devices.
        /// </summary>
        /// <param name="commitPoint">
        /// The index of an <see cref="IDevice">IDevice</see> in <see cref="devices">devices</see>. When a write has been completed on the device,
        /// the write is considered persistent. It is guaranteed that the callback in <see cref="WriteAsync(IntPtr, ulong, uint, IOCompletionCallback, IAsyncResult)"/>
        /// will not be called until the write is completed on the commit point device.
        /// </param>
        /// <param name="devices">
        /// List of devices to be used. The list should be given in order of hot to cold. Read is served from the
        /// device with smallest index in the list that has the requested data
        /// </param>
        public TieredStorageDevice(int commitPoint, params IDevice[] devices) : this(commitPoint, (IList<IDevice>)devices)
        {
        }


        // TODO(Tianyu): Unclear whether this is the right design. Should we allow different tiers different segment sizes?
        public override void Initialize(long segmentSize)
        {
            foreach (IDevice devices in devices)
            {
                devices.Initialize(segmentSize);
            }
        }

        public override void Close()
        {
            foreach (IDevice device in devices)
            {
                // TODO(Tianyu): All writes need to have succeeded when we call this.
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
                devices[i].DeleteAddressRange((long)Math.Max(fromAddress, tierStartAddresses[i]), toAddress);
            }
        }

        public override void DeleteSegmentRange(int fromSegment, int toSegment)
        {
            throw new NotSupportedException();
        }

        public override void ReadAsync(ulong alignedSourceAddress, IntPtr alignedDestinationAddress, uint alignedReadLength, IOCompletionCallback callback, IAsyncResult asyncResulte)
        {
            // TODO(Tianyu): This whole operation needs to be thread-safe with concurrent calls to writes, which may trigger a change in start address.
            IDevice closestDevice = devices[FindClosestDeviceContaining((long)alignedSourceAddress)];
            // We can directly forward the address, because assuming an inclusive policy, all devices agree on the same address space. The only difference is that some segments may not
            // be present for certain devices. 
            closestDevice.ReadAsync(alignedSourceAddress, alignedDestinationAddress, alignedReadLength, callback, asyncResulte);
        }

        public override void ReadAsync(int segmentId, ulong sourceAddress, IntPtr destinationAddress, uint readLength, IOCompletionCallback callback, IAsyncResult asyncResult)
        {
            // If it is not guaranteed that all underlying tiers agree on a segment size, this API cannot have a meaningful implementation
            throw new NotSupportedException();
        }

        public override unsafe void WriteAsync(IntPtr sourceAddress, ulong alignedDestinationAddress, uint numBytesToWrite, IOCompletionCallback callback, IAsyncResult asyncResult)
        {
            long writeHead = (long)alignedDestinationAddress + numBytesToWrite;
            // TODO(Tianyu): Think more carefully about how this can interleave.
            UpdateLogHead(writeHead);
            for (int i = 0; i < devices.Count; i++)
            {
                UpdateDeviceRange(i, writeHead);
            }
            int startTier = FindClosestDeviceContaining((long)alignedDestinationAddress);
            // TODO(Tianyu): Can you ever initiate a write that is after the commit point? Given FASTER's model of a read-only region, this will probably never happen.
            Debug.Assert(startTier <= commitPoint, "Write should not elide the commit point");

            var countdown = new CountdownEvent(commitPoint + 1);  // number of devices to wait on
            // Issue writes to all tiers in parallel
            for (int i = startTier; i < devices.Count; i++)
            {
                if (i <= commitPoint)
                {
                    // All tiers before the commit point (incluisive) need to be persistent before the callback is invoked.
                    devices[i].WriteAsync(sourceAddress, alignedDestinationAddress, numBytesToWrite, (e, n, o) =>
                    {
                        // The last tier to finish invokes the callback
                        if (countdown.Signal())
                        {
                            callback(e, n, o);
                        }
                    }, asyncResult);
                }
                else
                {
                    // TODO(Tianyu): We may need some type of count down to verify that all writes are finished before closing a device.
                    // Some device may already provide said guarantee, however.

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

        private static long ComputeCapacity(IList<IDevice> devices)
        {
            long result = 0;
            // The capacity of a tiered storage device is the sum of the capacity of its tiers
            foreach (IDevice device in devices)
            {
                // Unless the last tier device has unspecified storage capacity, in which case the tiered storage also has unspecified capacity
                if (device.Capacity == Devices.CAPACITY_UNSPECIFIED)
                {
                    // TODO(Tianyu): Is this assumption too strong?
                    Debug.Assert(device == devices[devices.Count - 1], "Only the last tier storage of a tiered storage device can have unspecified capacity");
                    return Devices.CAPACITY_UNSPECIFIED;
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
                string formatString = "{0}, file name {1}, capacity {2} bytes;";
                string capacity = device.Capacity == Devices.CAPACITY_UNSPECIFIED ? "unspecified" : device.Capacity.ToString();
                result.AppendFormat(formatString, device.GetType().Name, device.FileName, capacity);
            }
            result.AppendFormat("commit point: {0} at tier {1}", devices[commitPoint].GetType().Name, commitPoint);
            return result.ToString();
        }

        private int FindClosestDeviceContaining(long address)
        {
            // Can use binary search, but 1) it might not be faster than linear on a array assumed small, and 2) C# built in does not guarantee first element is returned on duplicates.
            // Therefore we are sticking to the simpler approach at first.
            for (int i = 0; i < devices.Count; i++)
            {
                if (tierStartAddresses[i] <= address) return i;
            }
            // TODO(Tianyu): This exception should never be triggered if we enforce that the last tier has unbounded storage.
            throw new ArgumentException("No such address exists");
        }

        private void UpdateLogHead(long writeHead)
        {
            long logHeadLocal;
            do
            {
                logHeadLocal = logHead;
                if (logHeadLocal >= writeHead) return;
            } while (logHeadLocal != Interlocked.CompareExchange(ref logHead, writeHead, logHeadLocal));
        }

        private void UpdateDeviceRange(int tier, long writeHead)
        {
            IDevice device = devices[tier];
            // Never need to update range if storage is unbounded
            if (device.Capacity == Devices.CAPACITY_UNSPECIFIED) return;

            long oldLogTail = tierStartAddresses[tier];
            if (writeHead - oldLogTail > device.Capacity)
            {
                long newLogTail = writeHead - oldLogTail - device.Capacity;
                tierStartAddresses[tier] = newLogTail;
                // TODO(Tianyu): This should also be made async.
                // TODO(Tianyu): There will be a race here with readers. Epoch protection?
                device.DeleteAddressRange(oldLogTail, newLogTail);
            }
        }
    }
}
