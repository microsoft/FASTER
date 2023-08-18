// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Linq;
using System.Threading;
using FASTER.core;
using NUnit.Framework;

namespace FASTER.test
{
    [TestFixture]
    internal class DeviceTests
    {
        const int entryLength = 1024;
        SectorAlignedBufferPool bufferPool;
        readonly byte[] entry = new byte[entryLength];
        string path;
        SemaphoreSlim semaphore;

        [SetUp]
        public void Setup()
        {
            path = TestUtils.MethodTestDir + "/test.log";

            // Clean up log files from previous test runs in case they weren't cleaned up
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);

            // Set entry data
            for (int i = 0; i < entry.Length; i++)
                entry[i] = (byte)i;

            bufferPool = new SectorAlignedBufferPool(1, 512);
            semaphore = new SemaphoreSlim(0);
        }

        [TearDown]
        public void TearDown()
        {
            semaphore.Dispose();
            bufferPool.Free();

            // Clean up log files
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
        }

        [Test]
        public void NativeDeviceTest1()
        {
            // Create devices \ log for test for in memory device
            using var device = 
                // Devices.CreateLogDevice(path, deleteOnClose: true)
                new NativeStorageDevice(path, true);
                ;

            WriteInto(device, 0, entry, entryLength);
            ReadInto(device, 0, out var readEntry, entryLength);

            Assert.IsTrue(readEntry.SequenceEqual(entry));
        }

        unsafe void WriteInto(IDevice device, ulong address, byte[] buffer, int size)
        {
            long numBytesToWrite = size;
            numBytesToWrite = ((numBytesToWrite + (device.SectorSize - 1)) & ~(device.SectorSize - 1));

            var pbuffer = bufferPool.Get((int)numBytesToWrite);
            fixed (byte* bufferRaw = buffer)
            {
                Buffer.MemoryCopy(bufferRaw, pbuffer.aligned_pointer, size, size);
            }

            device.WriteAsync((IntPtr)pbuffer.aligned_pointer, address, (uint)numBytesToWrite, IOCallback, null);
            while (!semaphore.Wait(0)) device.TryComplete();

            pbuffer.Return();
        }

        unsafe void ReadInto(IDevice device, ulong address, out byte[] buffer, int size)
        {
            long numBytesToRead = size;
            numBytesToRead = ((numBytesToRead + (device.SectorSize - 1)) & ~(device.SectorSize - 1));

            var pbuffer = bufferPool.Get((int)numBytesToRead);
            device.ReadAsync(address, (IntPtr)pbuffer.aligned_pointer,
                (uint)numBytesToRead, IOCallback, null);
            while (!semaphore.Wait(0)) device.TryComplete();
            buffer = new byte[numBytesToRead];
            fixed (byte* bufferRaw = buffer)
                Buffer.MemoryCopy(pbuffer.aligned_pointer, bufferRaw, numBytesToRead, numBytesToRead);
            pbuffer.Return();
        }

        void IOCallback(uint errorCode, uint numBytes, object context)
        {
            if (errorCode != 0)
                Assert.Fail("OverlappedStream GetQueuedCompletionStatus error: {0}", errorCode);
            semaphore.Release();
        }
    }
}
