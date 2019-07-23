using System;
using System.Collections.Generic;
using System.Text;
using System.Diagnostics;
using System.Threading;
using System.ComponentModel;
using System.Collections.Concurrent;

namespace FASTER.core
{
    /// <summary>
    /// A <see cref="TieredStorageDevice"/> logically composes multiple <see cref="IDevice"/> into a single storage device. It is assumed
    /// that some <see cref="IDevice"/> are used as caches while there is one that is considered the commit point, i.e. when a write is completed
    /// on the device, it is considered persistent. Reads are served from the closest device with available data. Writes are issued in parallel to
    /// all devices 
    /// </summary>
    class TieredStorageDevice : StorageDeviceBase
    {
        private readonly IList<IDevice> devices;
        private readonly int commitPoint;

        // TODO(Tianyu): Not reasoning about what the sector size of a tiered storage should be when different tiers can have different sector sizes.
        /// <summary>
        /// Constructs a new TieredStorageDevice composed of the given devices.
        /// </summary>
        /// <param name="commitPoint">
        /// The index of an <see cref="IDevice">IDevice</see> in <see cref="devices"/>. When a write has been completed on the device,
        /// the write is considered persistent. It is guaranteed that the callback in <see cref="WriteAsync(IntPtr, int, ulong, uint, IOCompletionCallback, IAsyncResult)"/>
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
        }

        /// <summary>
        /// Constructs a new TieredStorageDevice composed of the given devices.
        /// </summary>
        /// <param name="commitPoint">
        /// The index of an <see cref="IDevice">IDevice</see> in <see cref="devices">devices</see>. When a write has been completed on the device,
        /// the write is considered persistent. It is guaranteed that the callback in <see cref="WriteAsync(IntPtr, int, ulong, uint, IOCompletionCallback, IAsyncResult)"/>
        /// will not be called until the write is completed on commit point device and all previous tiers.
        /// </param>
        /// <param name="devices">
        /// List of devices to be used. The list should be given in order of hot to cold. Read is served from the
        /// device with smallest index in the list that has the requested data
        /// </param>
        public TieredStorageDevice(int commitPoint, params IDevice[] devices) : this(commitPoint, (IList<IDevice>)devices)
        {
        }


        // TODO(Tianyu): Unclear whether this is the right design. Should we allow different tiers different segment sizes?
        public override void Initialize(long segmentSize, LightEpoch epoch)
        {
            base.Initialize(segmentSize, epoch);

            foreach (IDevice devices in devices)
            {
                devices.Initialize(segmentSize, epoch);
            }
        }

        public override void Close()
        {
            foreach (IDevice device in devices)
            {
                device.Close();
            }
        }

        public override void ReadAsync(int segmentId, ulong sourceAddress, IntPtr destinationAddress, uint readLength, IOCompletionCallback callback, IAsyncResult asyncResult)
        {
            // This device is epoch-protected and cannot be stale while the operation is in flight
            IDevice closestDevice = devices[FindClosestDeviceContaining(segmentId)];
            // We can directly forward the address, because assuming an inclusive policy, all devices agree on the same address space. The only difference is that some segments may not
            // be present for certain devices. 
            closestDevice.ReadAsync(segmentId, sourceAddress, destinationAddress, readLength, callback, asyncResult);
        }

        public override unsafe void WriteAsync(IntPtr sourceAddress, int segmentId, ulong destinationAddress, uint numBytesToWrite, IOCompletionCallback callback, IAsyncResult asyncResult)
        {

            int startTier = FindClosestDeviceContaining(segmentId);
            // TODO(Tianyu): Can you ever initiate a write that is after the commit point? Given FASTER's model of a read-only region, this will probably never happen.
            Debug.Assert(startTier <= commitPoint, "Write should not elide the commit point");

            var countdown = new CountdownEvent(commitPoint + 1);  // number of devices to wait on
            // Issue writes to all tiers in parallel
            for (int i = startTier; i < devices.Count; i++)
            {
                if (i <= commitPoint)
                {
                  
                    // All tiers before the commit point (incluisive) need to be persistent before the callback is invoked.
                    devices[i].WriteAsync(sourceAddress, segmentId, destinationAddress, numBytesToWrite, (e, n, o) =>
                    {
                        // The last tier to finish invokes the callback
                        if (countdown.Signal())
                        {
                            callback(e, n, o);
                            countdown.Dispose();
                        }
                        
                    }, asyncResult);
                }
                else
                {
                    // Otherwise, simply issue the write without caring about callbacks
                    devices[i].WriteAsync(sourceAddress, segmentId, destinationAddress, numBytesToWrite, (e, n, o) => { }, null);
                }
            }
        }

        public override void RemoveSegmentAsync(int segment, AsyncCallback callback, IAsyncResult result)
        {
            int startTier = FindClosestDeviceContaining(segment);
            var countdown = new CountdownEvent(devices.Count);
            for(int i = startTier; i < devices.Count; i++)
            {
                devices[i].RemoveSegmentAsync(segment, r =>
                {
                    if (countdown.Signal())
                    {
                        callback(r);
                        countdown.Dispose();
                    }
                }, result);
            }
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

        private int FindClosestDeviceContaining(int segment)
        {
            // Can use binary search, but 1) it might not be faster than linear on a array assumed small, and 2) C# built in does not guarantee first element is returned on duplicates.
            // Therefore we are sticking to the simpler approach at first.
            for (int i = 0; i < devices.Count; i++)
            {
                if (devices[i].StartSegment <= segment) return i;
            }
            // TODO(Tianyu): This exception should never be triggered if we enforce that the last tier has unbounded storage.
            throw new ArgumentException("No such address exists");
        }
    }
}
