// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace FASTER.core
{
    /// <summary>
    /// Local storage device factory
    /// </summary>
    public class LocalStorageNamedDeviceFactory : INamedDeviceFactory
    {
        private string baseName;

        /// <inheritdoc />
        public LocalStorageNamedDeviceFactory() { }

        /// <inheritdoc />
        public void Initialize(string baseName)
        {
            this.baseName = baseName;
        }

        /// <inheritdoc />
        public IDevice Get(FileDescriptor fileInfo)
        {
            Directory.CreateDirectory(fileInfo.directoryName);
            return new LocalStorageDevice(Path.Combine(baseName, fileInfo.directoryName, fileInfo.fileName), initialLogFileHandles: null);
        }


        /// <inheritdoc />
        public IEnumerable<FileDescriptor> ListContents(string path)
        {
            var pathInfo = new DirectoryInfo(Path.Combine(baseName, path));

            bool found = false;
            foreach (var folder in pathInfo.GetDirectories().OrderByDescending(f => f.LastWriteTime))
            {
                found = true;
                yield return new FileDescriptor(folder.Name, "");
            }

            foreach (var file in pathInfo.GetFiles().OrderByDescending(f => f.LastWriteTime))
            {
                found = true;
                yield return new FileDescriptor("", file.Name);
            }

            if (!found)
                throw new FasterException($"No contents in path {path}");
        }
    }
}