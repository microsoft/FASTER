using System;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Threading;
using FASTER.core;

namespace FASTER.libdpr
{
    
    public class PingPongDevice : IDisposable
    {
        [StructLayout(LayoutKind.Explicit, Size = 32)]
        private unsafe struct MetadataHeader
        {
            [FieldOffset(0)]
            internal fixed byte bytes[32];
            [FieldOffset(0)]
            internal long size;
            [FieldOffset(8)]
            internal long version;
            [FieldOffset(16)]
            internal fixed byte checksum[16];
        }
        
        private IDevice frontDevice, backDevice;
        private MD5 checksumHasher = MD5.Create();
        private long versionCounter;

        public PingPongDevice(IDevice device1, IDevice device2)
        {
            Debug.Assert(checksumHasher.HashSize == 16);
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
                throw new InvalidDataException("The ping-pong devices should not point to the same version");
            }
        }

        public unsafe void WriteReliably(byte[] buf, int offset, int size)
        {
            var header = new MetadataHeader();
            header.size = size;
            header.version = versionCounter++;
            checksumHasher.TryComputeHash(new Span<byte>(buf, offset, size), new Span<byte>(header.checksum, 16), out _);
            
            var countdown = new CountdownEvent(2);

            // Write of metadata block should be atomic
            Debug.Assert(frontDevice.SegmentSize >= sizeof(MetadataHeader));
            frontDevice.WriteAsync((IntPtr) header.bytes, 0, 0, (uint) sizeof(MetadataHeader),
                (e, n, o) => countdown.Signal(), null);

            fixed (byte* b = &buf[offset])
            {
                // Skip one segment to avoid clobbering with metadata header write
                frontDevice.WriteAsync((IntPtr) b, 0, (uint) frontDevice.SectorSize, (uint) size,
                    (e, n, o) => countdown.Signal(), null);
                countdown.Wait();
            }

            (frontDevice, backDevice) = (backDevice, frontDevice);
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
                device.ReadAsync(0, (uint) device.SectorSize, (IntPtr) b, (uint) header.size,
                    (e, n, o) => completed.Set(), null);
                completed.Wait();
            }
            
            // Compare the hash with checksum
            var contentHash = checksumHasher.ComputeHash(buf);
            for (var i = 0; i < contentHash.Length; i++)
            {
                if (header.checksum[i] != contentHash[i])
                    // Not a complete write, should discard
                    return -1;
            }
            return header.version;
        }

        public bool ReadLatestCompleteWrite(out byte[] buf)
        {
            buf = null;
            var vfront = ReadFromDevice(frontDevice, out var bufFront);
            var vback = ReadFromDevice(backDevice, out var bufBack);
            if (vback == -1 && vfront == -1)
                // No available writes to read back in
                return false;

            buf = vfront > vback ? bufFront : bufBack;
            return true;
        }

        public void Dispose()
        {
            frontDevice?.Dispose();
            backDevice?.Dispose();
            checksumHasher?.Dispose();
        }
    }
}