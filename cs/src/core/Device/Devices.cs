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
        public const string EMULATED_STORAGE_STRING = "UseDevelopmentStorage=true;";
        public const string TEST_CONTAINER = "test";

        /// <summary>
        /// Create a storage device for the log
        /// </summary>
        /// <param name="logPath">Path to file that will store the log (empty for null device)</param>
        /// <param name="preallocateFile">Whether we try to preallocate the file on creation</param>
        /// <param name="deleteOnClose">Delete files on close</param>
        /// <returns>Device instance</returns>
        public static IDevice CreateLogDevice(string logPath, bool preallocateFile = true, bool deleteOnClose = false)
        {
            
            if (string.IsNullOrWhiteSpace(logPath))
                return new NullDevice();

            IDevice logDevice;

#if DOTNETCORE
            if (!RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                logDevice = new ManagedLocalStorageDevice(logPath, preallocateFile, deleteOnClose);
            }
            else
#endif
            {
                logDevice = new LocalStorageDevice(logPath, preallocateFile, deleteOnClose);
            }
            return logDevice;
        }

        // TODO(Tianyu): How do we want to integrate the option of using AzurePageBlobDevice into the original static factory class? We can either follow the original pattern and somehow encode this in the string path argument,
        // or use concrete factories that are initialized per instance to only create one type. 
        public static IDevice CreateAzurePageBlobDevice(string blobName, string storageString = EMULATED_STORAGE_STRING, string containerName = TEST_CONTAINER, bool deleteOnClose = false)
        {
            return new AzurePageBlobDevice(storageString, containerName, blobName, deleteOnClose);
        }
    }


}
