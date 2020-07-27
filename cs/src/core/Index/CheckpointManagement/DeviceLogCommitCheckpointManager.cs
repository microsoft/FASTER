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

        private readonly bool overwriteLogCommits;
        private readonly bool removeOutdated;
        private SectorAlignedBufferPool bufferPool;

        private IDevice singleLogCommitDevice;

        /// <summary>
        /// Next commit number
        /// </summary>
        private long commitNum;


        /// <summary>
        /// Create new instance of log commit manager
        /// </summary>
        /// <param name="deviceFactory">Factory for getting devices</param>
        /// <param name="checkpointNamingScheme">Checkpoint naming helper</param>
        /// <param name="overwriteLogCommits">Overwrite same FASTER log commits each time</param>
        /// <param name="removeOutdated">Remote older FASTER log commits</param>
        public DeviceLogCommitCheckpointManager(INamedDeviceFactory deviceFactory, ICheckpointNamingScheme checkpointNamingScheme, bool overwriteLogCommits = true, bool removeOutdated = false)
        {
            this.deviceFactory = deviceFactory;
            this.checkpointNamingScheme = checkpointNamingScheme;

            this.commitNum = 0;
            this.semaphore = new SemaphoreSlim(0);

            this.overwriteLogCommits = overwriteLogCommits;
            this.removeOutdated = removeOutdated;

            deviceFactory.Initialize(checkpointNamingScheme.BaseName());
        }

        /// <inheritdoc />
        public void PurgeAll()
        {
            deviceFactory.Delete(new FileDescriptor { directoryName = "" });
        }

        /// <summary>
        /// Create new instance of log commit manager
        /// </summary>
        /// <param name="deviceFactory">Factory for getting devices</param>
        /// <param name="baseName">Overall location specifier (e.g., local path or cloud container name)</param>
        /// <param name="overwriteLogCommits">Overwrite same FASTER log commits each time</param>
        /// <param name="removeOutdated">Remote older FASTER log commits</param>
        public DeviceLogCommitCheckpointManager(INamedDeviceFactory deviceFactory, string baseName, bool overwriteLogCommits = true, bool removeOutdated = false)
            : this(deviceFactory, new DefaultCheckpointNamingScheme(baseName), overwriteLogCommits, removeOutdated)
                  { }

        #region ILogCommitManager

        /// <inheritdoc />
        public unsafe void Commit(long beginAddress, long untilAddress, byte[] commitMetadata)
        {
            var device = NextCommitDevice();

            // Two phase to ensure we write metadata in single Write operation
            using var ms = new MemoryStream();
            using var writer = new BinaryWriter(ms);
            writer.Write(commitMetadata.Length);
            writer.Write(commitMetadata);

            WriteInto(device, 0, ms.ToArray(), (int)ms.Position);

            if (!overwriteLogCommits)
                device.Close();

            RemoveOutdated();
        }

        /// <inheritdoc />
        public void Dispose()
        {
            singleLogCommitDevice?.Close();
            singleLogCommitDevice = null;
        }

        /// <inheritdoc />
        public IEnumerable<long> ListCommits()
        {
            return deviceFactory.ListContents(checkpointNamingScheme.FasterLogCommitBasePath()).Select(e => checkpointNamingScheme.CommitNumber(e)).OrderByDescending(e => e);
        }

        /// <inheritdoc />
        public byte[] GetCommitMetadata(long commitNum)
        {
            IDevice device;
            if (overwriteLogCommits)
            {
                if (singleLogCommitDevice == null)
                    singleLogCommitDevice = deviceFactory.Get(checkpointNamingScheme.FasterLogCommitMetadata(commitNum));
                device = singleLogCommitDevice;
            }
            else
            {
                device = deviceFactory.Get(checkpointNamingScheme.FasterLogCommitMetadata(commitNum));
                this.commitNum = commitNum + 1;
            }
            

            ReadInto(device, 0, out byte[] writePad, sizeof(int));
            int size = BitConverter.ToInt32(writePad, 0);

            byte[] body;
            if (writePad.Length >= size + sizeof(int))
                body = writePad;
            else
                ReadInto(device, 0, out body, size + sizeof(int));
            
            if (!overwriteLogCommits)
                device.Close();

            return new Span<byte>(body).Slice(sizeof(int)).ToArray();
        }

        private IDevice NextCommitDevice()
        {
            if (overwriteLogCommits)
            {
                if (singleLogCommitDevice == null)
                    singleLogCommitDevice = deviceFactory.Get(checkpointNamingScheme.FasterLogCommitMetadata(commitNum));
                return singleLogCommitDevice;
            }

            return deviceFactory.Get(checkpointNamingScheme.FasterLogCommitMetadata(commitNum++));
        }

        private void RemoveOutdated()
        {
            if (removeOutdated && commitNum > 1)
                deviceFactory.Delete(checkpointNamingScheme.FasterLogCommitMetadata(commitNum - 2));
        }
        #endregion


        #region ICheckpointManager
        /// <inheritdoc />
        public unsafe void CommitIndexCheckpoint(Guid indexToken, byte[] commitMetadata)
        {
            var device = NextIndexCheckpointDevice(indexToken);

            // Two phase to ensure we write metadata in single Write operation
            using var ms = new MemoryStream();
            using var writer = new BinaryWriter(ms);
            writer.Write(commitMetadata.Length);
            writer.Write(commitMetadata);

            WriteInto(device, 0, ms.ToArray(), (int)ms.Position);
            device.Close();
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

            ReadInto(device, 0, out byte[] writePad, sizeof(int));
            int size = BitConverter.ToInt32(writePad, 0);

            byte[] body;
            if (writePad.Length >= size + sizeof(int))
                body = writePad;
            else
                ReadInto(device, 0, out body, size + sizeof(int));
            device.Close();
            return new Span<byte>(body).Slice(sizeof(int)).ToArray();
        }

        /// <inheritdoc />
        public unsafe void CommitLogCheckpoint(Guid logToken, byte[] commitMetadata)
        {
            var device = NextLogCheckpointDevice(logToken);

            // Two phase to ensure we write metadata in single Write operation
            using var ms = new MemoryStream();
            using var writer = new BinaryWriter(ms);
            writer.Write(commitMetadata.Length);
            writer.Write(commitMetadata);

            WriteInto(device, 0, ms.ToArray(), (int)ms.Position);
            device.Close();
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

            ReadInto(device, 0, out byte[] writePad, sizeof(int));
            int size = BitConverter.ToInt32(writePad, 0);

            byte[] body;
            if (writePad.Length >= size + sizeof(int))
                body = writePad;
            else
                ReadInto(device, 0, out body, size + sizeof(int));
            device.Close();
            return new Span<byte>(body).Slice(sizeof(int)).ToArray();
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

        /// <summary>
        /// Note: will read potentially more data (based on sector alignment)
        /// </summary>
        /// <param name="device"></param>
        /// <param name="address"></param>
        /// <param name="buffer"></param>
        /// <param name="size"></param>
        private unsafe void ReadInto(IDevice device, ulong address, out byte[] buffer, int size)
        {
            if (bufferPool == null)
                bufferPool = new SectorAlignedBufferPool(1, (int)device.SectorSize);

            long numBytesToRead = size;
            numBytesToRead = ((numBytesToRead + (device.SectorSize - 1)) & ~(device.SectorSize - 1));

            var pbuffer = bufferPool.Get((int)numBytesToRead);
            device.ReadAsync(address, (IntPtr)pbuffer.aligned_pointer,
                (uint)numBytesToRead, IOCallback, null);
            semaphore.Wait();

            buffer = new byte[numBytesToRead];
            fixed (byte* bufferRaw = buffer)
                Buffer.MemoryCopy(pbuffer.aligned_pointer, bufferRaw, numBytesToRead, numBytesToRead);
            pbuffer.Return();
        }

        /// <summary>
        /// Note: pads the bytes with zeros to achieve sector alignment
        /// </summary>
        /// <param name="device"></param>
        /// <param name="address"></param>
        /// <param name="buffer"></param>
        /// <param name="size"></param>
        private unsafe void WriteInto(IDevice device, ulong address, byte[] buffer, int size)
        {
            if (bufferPool == null)
                bufferPool = new SectorAlignedBufferPool(1, (int)device.SectorSize);

            long numBytesToWrite = size;
            numBytesToWrite = ((numBytesToWrite + (device.SectorSize - 1)) & ~(device.SectorSize - 1));

            var pbuffer = bufferPool.Get((int)numBytesToWrite);
            fixed (byte* bufferRaw = buffer)
            {
                Buffer.MemoryCopy(bufferRaw, pbuffer.aligned_pointer, size, size);
            }
            
            device.WriteAsync((IntPtr)pbuffer.aligned_pointer, address, (uint)numBytesToWrite, IOCallback, null);
            semaphore.Wait();

            pbuffer.Return();
        }
    }
}