// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;

namespace FASTER.core
{
    /// <summary>
    /// Local storage device factory
    /// </summary>
    public class LocalStorageNamedDeviceFactory : INamedDeviceFactory
    {
        string baseName;
        readonly bool deleteOnClose;
        readonly int? throttleLimit;
        readonly bool preallocateFile;
        readonly bool disableFileBuffering;

        /// <summary>
        /// Create instance of factory
        /// </summary>
        /// <param name="preallocateFile">Whether files should be preallocated</param>
        /// <param name="deleteOnClose">Whether file should be deleted on close</param>
        /// <param name="disableFileBuffering">Whether file buffering (during write) is disabled (default of true requires aligned writes)</param>
        /// <param name="throttleLimit">Throttle limit (max number of pending I/Os) for this device instance</param>
        public LocalStorageNamedDeviceFactory(bool preallocateFile = false, bool deleteOnClose = false, bool disableFileBuffering = true, int? throttleLimit = null)
        {
            this.preallocateFile = preallocateFile;
            this.deleteOnClose = deleteOnClose;
            this.disableFileBuffering = disableFileBuffering;
            this.throttleLimit = throttleLimit;
        }

        /// <inheritdoc />
        public void Initialize(string baseName)
        {
            this.baseName = baseName;
        }

        /// <inheritdoc />
        public IDevice Get(FileDescriptor fileInfo)
        {
            var device = Devices.CreateLogDevice(Path.Combine(baseName, fileInfo.directoryName, fileInfo.fileName), preallocateFile: preallocateFile, deleteOnClose: deleteOnClose, disableFileBuffering: disableFileBuffering);
            if (this.throttleLimit.HasValue)
            {
                device.ThrottleLimit = this.throttleLimit.Value;
            }
            return device;
        }


        /// <inheritdoc />
        public IEnumerable<FileDescriptor> ListContents(string path)
        {
            var pathInfo = new DirectoryInfo(Path.Combine(baseName, path));

            if (pathInfo.Exists)
            {
                foreach (var folder in pathInfo.GetDirectories().OrderByDescending(f => f.LastWriteTime))
                {
                    yield return new FileDescriptor(folder.Name, "");
                }

                foreach (var file in pathInfo.GetFiles().OrderByDescending(f => f.LastWriteTime))
                {
                    yield return new FileDescriptor("", file.Name);
                }
            }
        }

        /// <inheritdoc />
        public void Delete(FileDescriptor fileInfo)
        {
            long startTime = DateTimeOffset.UtcNow.Ticks;
            if (fileInfo.fileName != null)
            {
                var file = new FileInfo(Path.Combine(baseName, fileInfo.directoryName, fileInfo.fileName + ".0"));
                while (true)
                {
                    try
                    {
                        if (file.Exists) file.Delete();
                        break;
                    }
                    catch { }
                    Thread.Yield();
                    // Retry until timeout
                    if (DateTimeOffset.UtcNow.Ticks - startTime > TimeSpan.FromSeconds(5).Ticks) break;
                }
            }
            else
            {
                var dir = new DirectoryInfo(Path.Combine(baseName, fileInfo.directoryName));
                while (true)
                {
                    try
                    {
                        if (dir.Exists) dir.Delete(true);
                        break;
                    }
                    catch { }
                    Thread.Yield();
                    // Retry until timeout
                    if (DateTimeOffset.UtcNow.Ticks - startTime > TimeSpan.FromSeconds(5).Ticks) break;
                }
            }
        }
    }
}