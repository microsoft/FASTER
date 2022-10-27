using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Threading;
using FASTER.core;

namespace FASTER.libdpr
{
    /// <summary>
    /// A PingPongDevice allows users to reliably and atomically update a value on storage (represented by arbitrary byte
    /// arrays) by using two storage devices and alternating between them. If the machine experience a failure before a
    /// write is complete, subsequent reads will return the value of most recent completed write. 
    /// </summary>
    public class PingPongDevice : IDisposable
    {
        private readonly MD5 checksumHasher = MD5.Create();

        private IDevice frontDevice, backDevice;
        private long versionCounter;

        /// <summary>
        /// Creates a new PingPongDevice from the given two devices.
        /// </summary>
        /// <param name="device1"> first device </param>
        /// <param name="device2"> second device </param>
        public PingPongDevice(IDevice device1, IDevice device2)
        {
            Debug.Assert(checksumHasher.HashSize == 128);
            var v1 = ReadFromDevice(device1, out _);
            var v2 = ReadFromDevice(device2, out _);
            if (v1 == -1 && v2 == -1)
            {
                // No prior writes available, start from scratch
                versionCounter = 0;
                frontDevice = device1;
                backDevice = device2;
            }
            else if (v1 > v2)
            {
                versionCounter = v1;
                frontDevice = device2;
                backDevice = device1;
            }
            else if (v2 > v1)
            {
                versionCounter = v2;
                frontDevice = device1;
                backDevice = device2;
            }
            else
            {
                throw new FasterException("The ping-pong device detects corrupted data from the given devices");
            }
        }

        /// <summary>
        /// Dispsoe
        /// </summary>
        public void Dispose()
        {
            frontDevice?.Dispose();
            backDevice?.Dispose();
            checksumHasher?.Dispose();
        }

        /// <summary>
        /// Reliably writes a byte array to the device to be retrieved later. The value is only retrievable if written
        /// completely; otherwise, the old value is preserved
        /// </summary>
        /// <param name="buf"></param>
        /// <param name="offset"></param>
        /// <param name="size"></param>
        public unsafe void WriteReliably(byte[] buf, int offset, int size)
        {
            var header = new MetadataHeader();
            header.size = size;
            header.version = ++versionCounter;
            var hash = checksumHasher.ComputeHash(buf, offset, size);

            fixed (byte* b = &hash[0])
            {
                Unsafe.CopyBlock(header.checksum, b, 16);
            }

            var countdown = new CountdownEvent(2);

            // Write of metadata block should be atomic
            Debug.Assert(frontDevice.SegmentSize == -1 || frontDevice.SegmentSize >= sizeof(MetadataHeader));
            frontDevice.WriteAsync((IntPtr) header.bytes, 0, 0, (uint) sizeof(MetadataHeader),
                (e, n, o) => { countdown.Signal(); }, null);

            var handle = GCHandle.Alloc(buf, GCHandleType.Pinned);
            // Skip one segment to avoid clobbering with metadata header write
            frontDevice.WriteAsync(handle.AddrOfPinnedObject(), 0, frontDevice.SectorSize, (uint) size,
                (e, n, o) =>
                {
                    countdown.Signal();
                    handle.Free();
                }, null);

            countdown.Wait();
            (frontDevice, backDevice) = (backDevice, frontDevice);
            countdown.Dispose();
        }

        private unsafe long ReadFromDevice(IDevice device, out byte[] buf)
        {
            var header = new MetadataHeader();
            var completed = new ManualResetEventSlim();
            device.ReadAsync(0, 0, (IntPtr) header.bytes, (uint) sizeof(MetadataHeader),
                (e, n, o) => completed.Set(), null);
            completed.Wait();

            buf = new byte[header.size];
            if (header.size == 0) return -1;

            completed = new ManualResetEventSlim();
            fixed (byte* b = &buf[0])
            {
                device.ReadAsync(0, device.SectorSize, (IntPtr) b, (uint) header.size,
                    (e, n, o) => completed.Set(), null);
                completed.Wait();
            }

            // Compare the hash with checksum
            var contentHash = checksumHasher.ComputeHash(buf);
            for (var i = 0; i < contentHash.Length; i++)
                if (header.checksum[i] != contentHash[i])
                    // Not a complete write, should discard
                    return -1;
            return header.version;
        }

        /// <summary>
        /// Read the last completed write on the device
        /// </summary>
        /// <returns> read bytes, or null if the device has never been written to </returns>
        public byte[] ReadLatestCompleteWrite()
        {
            var vfront = ReadFromDevice(frontDevice, out var bufFront);
            var vback = ReadFromDevice(backDevice, out var bufBack);
            if (vback == -1 && vfront == -1)
                // No available writes to read back in
                return null;

            return vfront > vback ? bufFront : bufBack;
        }

        [StructLayout(LayoutKind.Explicit, Size = 32)]
        private unsafe struct MetadataHeader
        {
            [FieldOffset(0)] internal fixed byte bytes[32];
            [FieldOffset(0)] internal long size;
            [FieldOffset(8)] internal long version;
            [FieldOffset(16)] internal fixed byte checksum[16];
        }
    }
}