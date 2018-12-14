// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.IO;
using System.Linq.Expressions;
using System.Runtime.InteropServices;

namespace FASTER.core
{


    /// <summary>
    /// Factory to create FASTER objects
    /// </summary>
    public static class Devices
    {
        /// <summary>
        /// Create a storage device for the log
        /// </summary>
        /// <param name="logPath">Path to file that will store the log (empty for null device)</param>
        /// <param name="preallocateFile">Whether we try to preallocate the file on creation</param>
        /// <param name="deleteOnClose">Delete files on close</param>
        /// <returns>Device instance</returns>
        public static IDevice CreateLogDevice(string logPath, bool preallocateFile = true, bool deleteOnClose = false)
        {
            IDevice logDevice = new NullDevice();
            if (string.IsNullOrWhiteSpace(logPath))
                return logDevice;
#if DOTNETCORE
            if (!RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                logDevice = new ManagedLocalStorageDevice(logPath + ".log", preallocateFile, deleteOnClose);
            }
            else
#endif
            {
                logDevice = new LocalStorageDevice(logPath + ".log", preallocateFile, deleteOnClose);
            }
            return logDevice;
        }

        /// <summary>
        /// Create a storage device for the object log (for non-blittable objects)
        /// </summary>
        /// <param name="logPath">Path to file that will store the log (empty for null device)</param>
        /// <param name="preallocateFile">Whether we try to preallocate the file on creation</param>
        /// <param name="deleteOnClose">Delete files on close</param>
        /// <returns>Device instance</returns>
        public static IDevice CreateObjectLogDevice(string logPath, bool preallocateFile = true, bool deleteOnClose = false)
        {
            IDevice logDevice = new NullDevice();
            if (String.IsNullOrWhiteSpace(logPath))
                return logDevice;
#if DOTNETCORE
            if (!RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                logDevice = new ManagedLocalStorageDevice(logPath + ".obj.log", preallocateFile, deleteOnClose);
            }
            else
#endif
            {
                logDevice = new LocalStorageDevice(logPath + ".obj.log", preallocateFile, deleteOnClose);
            }
            return logDevice;
        }
    }
}
