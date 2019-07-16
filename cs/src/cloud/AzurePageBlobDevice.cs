// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO;
using System.Threading;
using FASTER.core;
using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Blob;

namespace FASTER.cloud
{
    /// <summary>
    /// A IDevice Implementation that is backed by<see href="https://docs.microsoft.com/en-us/azure/storage/blobs/storage-blob-pageblob-overview">Azure Page Blob</see>.
    /// This device is expected to be an order of magnitude slower than local SSD or HDD, but provide scalability and shared access in the cloud.
    /// </summary>
    public class AzurePageBlobDevice : StorageDeviceBase
    {
        private CloudBlobContainer container;
        private readonly ConcurrentDictionary<int, CloudPageBlob> blobs;
        private readonly string blobName;
        private readonly bool deleteOnClose;

        // Page Blobs permit blobs of max size 8 TB, but the emulator permits only 2 GB
        private const long MAX_BLOB_SIZE = (long)(2 * 10e8);
        // Azure Page Blobs have a fixed sector size of 512 bytes.
        private const uint PAGE_BLOB_SECTOR_SIZE = 512;

        /// <summary>
        /// Constructs a new AzurePageBlobDevice instance
        /// </summary>
        /// <param name="connectionString"> The connection string to use when estblishing connection to Azure Blobs</param>
        /// <param name="containerName">Name of the Azure Blob container to use. If there does not exist a container with the supplied name, one is created</param>
        /// <param name="blobName">A descriptive name that will be the prefix of all blobs created with this device</param>
        /// <param name="deleteOnClose">
        /// True if the program should delete all blobs created on call to <see cref="Close">Close</see>. False otherwise. 
        /// The container is not deleted even if it was created in this constructor
        /// </param>
        public AzurePageBlobDevice(string connectionString, string containerName, string blobName, bool deleteOnClose = false)
            : base(connectionString + "/" + containerName + "/" + blobName, PAGE_BLOB_SECTOR_SIZE)
        {
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(connectionString);
            CloudBlobClient client = storageAccount.CreateCloudBlobClient();
            container = client.GetContainerReference(containerName);
            container.CreateIfNotExists();
            blobs = new ConcurrentDictionary<int, CloudPageBlob>();
            this.blobName = blobName;
            this.deleteOnClose = deleteOnClose;
        }

        /// <summary>
        /// <see cref="IDevice.Close">Inherited</see>
        /// </summary>
        public override void Close()
        {
            // Unlike in LocalStorageDevice, we explicitly remove all page blobs if the deleteOnClose flag is set, instead of relying on the operating system
            // to delete files after the end of our process. This leads to potential problems if multiple instances are sharing the same underlying page blobs.
            //
            // Since this flag is presumably only used for testing though, it is probably fine.
            if (deleteOnClose)
            {
                foreach (var entry in blobs)
                {
                    entry.Value.Delete();
                }
            }
        }

        /// <summary>
        /// <see cref="IDevice.Close">Inherited</see>
        /// </summary>
        public override void DeleteSegmentRange(int fromSegment, int toSegment)
        {
            for (int i = fromSegment; i < toSegment; i++)
            {
                if (blobs.TryRemove(i, out CloudPageBlob blob))
                {
                    blob.Delete();
                }
            }
        }

        /// <summary>
        /// <see cref="IDevice.ReadAsync(int, ulong, IntPtr, uint, IOCompletionCallback, IAsyncResult)">Inherited</see>
        /// </summary>
        public override unsafe void ReadAsync(int segmentId, ulong sourceAddress, IntPtr destinationAddress, uint readLength, IOCompletionCallback callback, IAsyncResult asyncResult)
        {
            // It is up to the allocator to make sure no reads are issued to segments before they are written
            if (!blobs.TryGetValue(segmentId, out CloudPageBlob pageBlob)) throw new InvalidOperationException("Attempting to read non-existent segments");

            // Even though Azure Page Blob does not make use of Overlapped, we populate one to conform to the callback API
            Overlapped ov = new Overlapped(0, 0, IntPtr.Zero, asyncResult);
            NativeOverlapped* ovNative = ov.UnsafePack(callback, IntPtr.Zero);

            UnmanagedMemoryStream stream = new UnmanagedMemoryStream((byte*)destinationAddress, readLength, readLength, FileAccess.Write);
            pageBlob.BeginDownloadRangeToStream(stream, (Int64)sourceAddress, readLength, ar => {
                try
                {
                    pageBlob.EndDownloadRangeToStream(ar);
                }
                // I don't think I can be more specific in catch here because no documentation on exception behavior is provided
                catch (Exception e)
                {
                    // Is there any documentation on the meaning of error codes here? The handler suggests that any non-zero value is an error
                    // but does not distinguish between them.
                    callback(2, readLength, ovNative);
                }
                callback(0, readLength, ovNative);
            }, asyncResult);
        }

        /// <summary>
        /// <see cref="IDevice.WriteAsync(IntPtr, int, ulong, uint, IOCompletionCallback, IAsyncResult)">Inherited</see>
        /// </summary>
        public override void WriteAsync(IntPtr sourceAddress, int segmentId, ulong destinationAddress, uint numBytesToWrite, IOCompletionCallback callback, IAsyncResult asyncResult)
        {
            if (!blobs.TryGetValue(segmentId, out CloudPageBlob pageBlob))
            {
                // If no blob exists for the segment, we must first create the segment asynchronouly. (Create call takes ~70 ms by measurement)
                pageBlob = container.GetPageBlobReference(blobName + segmentId);

                // If segment size is -1, which denotes absence, we request the largest possible blob. This is okay because
                // page blobs are not backed by real pages on creation, and the given size is only a the physical limit of 
                // how large it can grow to.
                var size = segmentSize == -1 ? MAX_BLOB_SIZE : segmentSize;
                // It is up to allocator to ensure that no reads happen before the callback of this function is invoked.
                pageBlob.BeginCreate(size, ar =>
                {
                    pageBlob.EndCreate(ar);
                }, null);
                WriteToBlobAsync(pageBlob, sourceAddress, destinationAddress, numBytesToWrite, callback, asyncResult);
                blobs.TryAdd(segmentId, pageBlob);
            }
            else
            {
                // Write directly to the existing blob otherwise
                WriteToBlobAsync(pageBlob, sourceAddress, destinationAddress, numBytesToWrite, callback, asyncResult);
            }
        }

        private static unsafe void WriteToBlobAsync(CloudPageBlob blob, IntPtr sourceAddress, ulong destinationAddress, uint numBytesToWrite, IOCompletionCallback callback, IAsyncResult asyncResult)
        {
            // Even though Azure Page Blob does not make use of Overlapped, we populate one to conform to the callback API
            Overlapped ov = new Overlapped(0, 0, IntPtr.Zero, asyncResult);
            NativeOverlapped* ovNative = ov.UnsafePack(callback, IntPtr.Zero);
            UnmanagedMemoryStream stream = new UnmanagedMemoryStream((byte*)sourceAddress, numBytesToWrite);
            blob.BeginWritePages(stream, (long)destinationAddress, null, ar =>
            {
                try
                {
                    blob.EndWritePages(ar);
                }
                // I don't think I can be more specific in catch here because no documentation on exception behavior is provided
                catch (Exception e)
                {
                    Console.WriteLine(e.Message);
                    // Is there any documentation on the meaning of error codes here? The handler suggests that any non-zero value is an error
                    // but does not distinguish between them.
                    callback(1, numBytesToWrite, ovNative);
                }
                callback(0, numBytesToWrite, ovNative);
            }, asyncResult);
        }
    }


}
