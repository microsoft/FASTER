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
        /// This value is supplied for capacity when the device does not have a specified limit.
        /// </summary>
        public const long CAPACITY_UNSPECIFIED = -1;
        private const string EMULATED_STORAGE_STRING = "UseDevelopmentStorage=true;";
        private const string TEST_CONTAINER = "test";

        /// <summary>
        /// Create a storage device for the log
        /// </summary>
        /// <param name="logPath">Path to file that will store the log (empty for null device)</param>
        /// <param name="preallocateFile">Whether we try to preallocate the file on creation</param>
        /// <param name="deleteOnClose">Delete files on close</param>
        /// <param name="capacity"></param>
        /// <returns>Device instance</returns>
        public static IDevice CreateLogDevice(string logPath, bool preallocateFile = true, bool deleteOnClose = false, long capacity = CAPACITY_UNSPECIFIED)
        {
            
            if (string.IsNullOrWhiteSpace(logPath))
                return new NullDevice();

            IDevice logDevice;

#if DOTNETCORE
            if (!RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                logDevice = new ManagedLocalStorageDevice(logPath, preallocateFile, deleteOnClose, capacity);
            }
            else
#endif
            {
                logDevice = new LocalStorageDevice(logPath, preallocateFile, deleteOnClose, capacity);
            }
            return logDevice;
        }

        // TODO(Tianyu): How do we want to integrate the option of using AzurePageBlobDevice into the original static factory class? We can either follow the original pattern and somehow encode this in the string path argument,
        // or use concrete factories that are initialized per instance to only create one type. 
        /// <summary>
        /// Creates a log device backed by <see href="https://docs.microsoft.com/en-us/azure/storage/blobs/storage-blob-pageblob-overview">Azure Page Blob</see>.
        /// </summary>
        /// <param name="blobName">A descriptive name that will be the prefix of all blobs created with this device</param>
        /// <param name="connectionString"> The connection string to use when estblishing connection to Azure Blobs</param>
        /// <param name="containerName">Name of the Azure Blob container to use. If there does not exist a container with the supplied name, one is created</param>
        /// <param name = "deleteOnClose" >
        /// True if the program should delete all blobs created on call to <see cref="IDevice.Close">Close</see>. False otherwise. 
        /// The container is not deleted even if it was created in this constructor
        /// </param>
        /// <returns>The constructed Device instance</returns>
        public static IDevice CreateAzurePageBlobDevice(string blobName, string connectionString = EMULATED_STORAGE_STRING, string containerName = TEST_CONTAINER, bool deleteOnClose = false)
        {
            return new AzurePageBlobDevice(connectionString, containerName, blobName, deleteOnClose);
        }
    }


}
