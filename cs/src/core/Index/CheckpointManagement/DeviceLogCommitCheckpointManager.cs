// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;

namespace FASTER.core
{
    /// <summary>
    /// Log commit manager for a generic IDevice
    /// </summary>
    public class DeviceLogCommitCheckpointManager : ILogCommitManager, ICheckpointManager
    {
        private readonly INamedDeviceFactory deviceFactory;
        private readonly ICheckpointNamingScheme checkpointNamingScheme;
        private readonly SemaphoreSlim semaphore;
        private readonly bool removeOutdated;

        /// <summary>
        /// Next commit number
        /// </summary>
        private long commitNum;

        // Only used if removeOutdated
        private IDevice[] devicePair;

        /// <summary>
        /// Create new instance of log commit manager
        /// </summary>
        /// <param name="deviceFactory">Factory for getting devices</param>
        /// <param name="checkpointNamingScheme">Checkpoint naming helper</param>
        /// <param name="removeOutdated">Remote outdated commits</param>
        public DeviceLogCommitCheckpointManager(INamedDeviceFactory deviceFactory, ICheckpointNamingScheme checkpointNamingScheme, bool removeOutdated = false)
        {
            this.deviceFactory = deviceFactory;
            this.checkpointNamingScheme = checkpointNamingScheme;

            this.commitNum = 0;
            this.semaphore = new SemaphoreSlim(0);

            this.removeOutdated = removeOutdated;

            deviceFactory.Initialize(checkpointNamingScheme.BaseName());

            if (removeOutdated)
            {
                // Initialize two devices if removeOutdated
                devicePair[0] = deviceFactory.Get(checkpointNamingScheme.FasterLogCommitMetadata(0));
                devicePair[1] = deviceFactory.Get(checkpointNamingScheme.FasterLogCommitMetadata(1));
            }
        }

        #region ILogCommitManager

        /// <inheritdoc />
        public unsafe void Commit(long beginAddress, long untilAddress, byte[] commitMetadata)
        {
            var device = NextCommitDevice();

            // Two phase to ensure we write metadata in single Write operation
            using var ms = new MemoryStream();
            using (var writer = new BinaryWriter(ms))
            {
                writer.Write(commitMetadata.Length);
                writer.Write(commitMetadata);
            }
            var numBytesToWrite = ms.Position;
            numBytesToWrite = ((numBytesToWrite + (device.SectorSize - 1)) & ~(device.SectorSize - 1));

            fixed (byte* commit = ms.ToArray())
            {
                device.WriteAsync((IntPtr)commit, 0, (uint)numBytesToWrite, IOCallback, null);
            }
            semaphore.Wait();
        }

        /// <inheritdoc />
        public IEnumerable<long> ListCommits()
        {
            return deviceFactory.ListContents(checkpointNamingScheme.FasterLogCommitBasePath()).Select(e => checkpointNamingScheme.CommitNumber(e));
        }

        /// <inheritdoc />
        public byte[] GetCommitMetadata(long commitNum)
        {
            this.commitNum = commitNum + 1;

            byte[] writePad = new byte[sizeof(int)];
            var fd = checkpointNamingScheme.FasterLogCommitMetadata(commitNum);
            var device = deviceFactory.Get(fd);
            ReadInto(device, 0, writePad, sizeof(int));
            int size = BitConverter.ToInt32(writePad, 0);
            byte[] body = new byte[size];
            ReadInto(device, sizeof(int), body, size);
            device.Close();
            return body;
        }

        private IDevice NextCommitDevice()
        {
            if (!removeOutdated)
            {
                return deviceFactory.Get(checkpointNamingScheme.FasterLogCommitMetadata(commitNum++));
            }

            return devicePair[commitNum++ % 2];
        }
        #endregion


        #region ICheckpointManager
        /// <inheritdoc />
        public unsafe void CommitIndexCheckpoint(Guid indexToken, byte[] commitMetadata)
        {
            var device = NextIndexCheckpointDevice(indexToken);

            // Two phase to ensure we write metadata in single Write operation
            using var ms = new MemoryStream();
            using (var writer = new BinaryWriter(ms))
            {
                writer.Write(commitMetadata.Length);
                writer.Write(commitMetadata);
            }
            var numBytesToWrite = ms.Position;
            numBytesToWrite = ((numBytesToWrite + (device.SectorSize - 1)) & ~(device.SectorSize - 1));

            fixed (byte* commit = ms.ToArray())
            {
                device.WriteAsync((IntPtr)commit, 0, (uint)numBytesToWrite, IOCallback, null);
            }
            semaphore.Wait();
        }

        /// <inheritdoc />
        public IEnumerable<Guid> GetIndexCheckpointTokens()
        {
            return deviceFactory.ListContents(checkpointNamingScheme.IndexCheckpointBasePath()).Select(e => checkpointNamingScheme.Token(e));
        }

        /// <inheritdoc />
        public byte[] GetIndexCheckpointMetadata(Guid indexToken)
        {
            var device = deviceFactory.Get(checkpointNamingScheme.IndexCheckpointMetadata(indexToken));

            byte[] writePad = new byte[sizeof(int)];
            ReadInto(device, 0, writePad, sizeof(int));
            int size = BitConverter.ToInt32(writePad, 0);
            byte[] body = new byte[size];
            ReadInto(device, sizeof(int), body, size);
            return body;
        }

        /// <inheritdoc />
        public unsafe void CommitLogCheckpoint(Guid logToken, byte[] commitMetadata)
        {
            var device = NextLogCheckpointDevice(logToken);

            // Two phase to ensure we write metadata in single Write operation
            using var ms = new MemoryStream();
            using (var writer = new BinaryWriter(ms))
            {
                writer.Write(commitMetadata.Length);
                writer.Write(commitMetadata);
            }
            var numBytesToWrite = ms.Position;
            numBytesToWrite = ((numBytesToWrite + (device.SectorSize - 1)) & ~(device.SectorSize - 1));

            fixed (byte* commit = ms.ToArray())
            {
                device.WriteAsync((IntPtr)commit, 0, (uint)numBytesToWrite, IOCallback, null);
            }
            semaphore.Wait();
        }

        /// <inheritdoc />
        public IEnumerable<Guid> GetLogCheckpointTokens()
        {
            return deviceFactory.ListContents(checkpointNamingScheme.LogCheckpointBasePath()).Select(e => checkpointNamingScheme.Token(e));
        }

        /// <inheritdoc />
        public byte[] GetLogCheckpointMetadata(Guid logToken)
        {
            var device = deviceFactory.Get(checkpointNamingScheme.LogCheckpointMetadata(logToken));

            byte[] writePad = new byte[sizeof(int)];
            ReadInto(device, 0, writePad, sizeof(int));
            int size = BitConverter.ToInt32(writePad, 0);
            byte[] body = new byte[size];
            ReadInto(device, sizeof(int), body, size);
            return body;
        }

        /// <inheritdoc />
        public IDevice GetIndexDevice(Guid indexToken)
        {
            return deviceFactory.Get(checkpointNamingScheme.HashTable(indexToken));
        }

        /// <inheritdoc />
        public IDevice GetSnapshotLogDevice(Guid token)
        {
            return deviceFactory.Get(checkpointNamingScheme.LogSnapshot(token));
        }

        /// <inheritdoc />
        public IDevice GetSnapshotObjectLogDevice(Guid token)
        {
            return deviceFactory.Get(checkpointNamingScheme.ObjectLogSnapshot(token));
        }

        /// <inheritdoc />
        public void InitializeIndexCheckpoint(Guid indexToken)
        {
        }

        /// <inheritdoc />
        public void InitializeLogCheckpoint(Guid logToken)
        {
        }

        private IDevice NextIndexCheckpointDevice(Guid token)
        {
            if (!removeOutdated)
            {
                return deviceFactory.Get(checkpointNamingScheme.IndexCheckpointMetadata(token));
            }
            throw new NotImplementedException();
        }

        private IDevice NextLogCheckpointDevice(Guid token)
        {
            if (!removeOutdated)
            {
                return deviceFactory.Get(checkpointNamingScheme.LogCheckpointMetadata(token));
            }
            throw new NotImplementedException();
        }
        #endregion

        private unsafe void IOCallback(uint errorCode, uint numBytes, NativeOverlapped* overlapped)
        {
            try
            {
                if (errorCode != 0)
                {
                    Trace.TraceError("OverlappedStream GetQueuedCompletionStatus error: {0}", errorCode);
                }
                semaphore.Release();
            }
            finally
            {
                Overlapped.Free(overlapped);
            }
        }

        private unsafe void ReadInto(IDevice commitDevice, ulong address, byte[] buffer, int size)
        {
            Debug.Assert(buffer.Length >= size);
            fixed (byte* bufferRaw = buffer)
            {
                CountdownEvent countdown = new CountdownEvent(1);
                commitDevice.ReadAsync(address, (IntPtr)bufferRaw,
                    (uint)size, IOCallback, null);
                countdown.Wait();
            }
        }
    }
}