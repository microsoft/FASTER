// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Storage.Blob;
using FASTER.core;
using Microsoft.Azure.Storage;

namespace FASTER.devices
{
    /// <summary>
    /// A IDevice Implementation that is backed by<see href="https://docs.microsoft.com/en-us/azure/storage/blobs/storage-blob-pageblob-overview">Azure Page Blob</see>.
    /// This device is slower than a local SSD or HDD, but provides scalability and shared access in the cloud.
    /// </summary>
    public class AzureStorageDevice : StorageDeviceBase
    {
        private readonly ConcurrentDictionary<int, BlobEntry> blobs;
        private readonly CloudBlobDirectory blobDirectory;
        private readonly string blobName;
        private readonly bool underLease;

        internal BlobRequestOptions BlobRequestOptions { get; private set; }

        // Page Blobs permit blobs of max size 8 TB, but the emulator permits only 2 GB
        private const long MAX_BLOB_SIZE = (long)(2 * 10e8);
        // Azure Page Blobs have a fixed sector size of 512 bytes.
        private const uint PAGE_BLOB_SECTOR_SIZE = 512;
        // Whether blob files are deleted on close
        private readonly bool deleteOnClose;

        /// <summary>
        /// Constructs a new AzureStorageDevice instance, backed by Azure Page Blobs
        /// </summary>
        /// <param name="cloudBlobDirectory">Cloud blob directory containing the blobs</param>
        /// <param name="blobName">A descriptive name that will be the prefix of all blobs created with this device</param>
        /// <param name="blobManager">Blob manager instance</param>
        /// <param name="underLease">Whether we use leases</param>
        /// <param name="deleteOnClose">
        /// True if the program should delete all blobs created on call to <see cref="Close">Close</see>. False otherwise. 
        /// The container is not deleted even if it was created in this constructor
        /// </param>
        /// <param name="capacity">The maximum number of bytes this storage device can accommondate, or CAPACITY_UNSPECIFIED if there is no such limit </param>
        public AzureStorageDevice(CloudBlobDirectory cloudBlobDirectory, string blobName, IBlobManager blobManager = null, bool underLease = false, bool deleteOnClose = false, long capacity = Devices.CAPACITY_UNSPECIFIED)
            : base($"{cloudBlobDirectory}\\{blobName}", PAGE_BLOB_SECTOR_SIZE, capacity)
        {
            this.blobs = new ConcurrentDictionary<int, BlobEntry>();
            this.blobDirectory = cloudBlobDirectory;
            this.blobName = blobName;
            this.underLease = underLease;
            this.deleteOnClose = deleteOnClose;

            this.BlobManager = blobManager ?? new DefaultBlobManager(this.underLease, this.blobDirectory);
            this.BlobRequestOptions = BlobManager.GetBlobRequestOptions();

            StartAsync().Wait();
        }

        /// <summary>
        /// Constructs a new AzureStorageDevice instance, backed by Azure Page Blobs
        /// </summary>
        /// <param name="connectionString"> The connection string to use when estblishing connection to Azure Blobs</param>
        /// <param name="containerName">Name of the Azure Blob container to use. If there does not exist a container with the supplied name, one is created</param>
        /// <param name="directoryName">Directory within blob container to use.</param>
        /// <param name="blobName">A descriptive name that will be the prefix of all blobs created with this device</param>
        /// <param name="blobManager">Blob manager instance</param>
        /// <param name="underLease">Whether we use leases</param>
        /// <param name="deleteOnClose">
        /// True if the program should delete all blobs created on call to <see cref="Close">Close</see>. False otherwise. 
        /// The container is not deleted even if it was created in this constructor
        /// </param>
        /// <param name="capacity">The maximum number of bytes this storage device can accommondate, or CAPACITY_UNSPECIFIED if there is no such limit </param>
        public AzureStorageDevice(string connectionString, string containerName, string directoryName, string blobName, IBlobManager blobManager = null, bool underLease = false, bool deleteOnClose = false, long capacity = Devices.CAPACITY_UNSPECIFIED)
            : base($"{connectionString}\\{containerName}\\{directoryName}\\{blobName}", PAGE_BLOB_SECTOR_SIZE, capacity)
        {
            var storageAccount = CloudStorageAccount.Parse(connectionString);
            var client = storageAccount.CreateCloudBlobClient();
            var container = client.GetContainerReference(containerName);
            container.CreateIfNotExists();

            this.blobs = new ConcurrentDictionary<int, BlobEntry>();
            this.blobDirectory = container.GetDirectoryReference(directoryName);
            this.blobName = blobName;
            this.underLease = underLease;
            this.deleteOnClose = deleteOnClose;

            this.BlobManager = blobManager ?? new DefaultBlobManager(this.underLease, this.blobDirectory);
            this.BlobRequestOptions = BlobManager.GetBlobRequestOptions();

            StartAsync().Wait();
        }

        private async Task StartAsync()
        {
            // list all the blobs representing the segments

            int prevSegmentId = -1;
            var prefix = $"{blobDirectory.Prefix}{blobName}.";

            BlobContinuationToken continuationToken = null;
            do
            {
                if (this.underLease)
                {
                    await this.BlobManager.ConfirmLeaseAsync().ConfigureAwait(false);
                }
                var response = await this.blobDirectory.ListBlobsSegmentedAsync(useFlatBlobListing: false, blobListingDetails: BlobListingDetails.None, maxResults: 1000,
                    currentToken: continuationToken, options: this.BlobRequestOptions, operationContext: null).ConfigureAwait(false);

                foreach (IListBlobItem item in response.Results)
                {
                    if (item is CloudPageBlob pageBlob)
                    {
                        if (Int32.TryParse(pageBlob.Name.Replace(prefix, ""), out int segmentId))
                        {
                            if (segmentId != prevSegmentId + 1)
                            {
                                startSegment = segmentId;
                            }
                            else
                            {
                                endSegment = segmentId;
                            }
                            prevSegmentId = segmentId;
                        }
                    }
                }
                continuationToken = response.ContinuationToken;
            }
            while (continuationToken != null);

            for (int i = startSegment; i <= endSegment; i++)
            {
                bool ret = this.blobs.TryAdd(i, new BlobEntry(this.blobDirectory.GetPageBlobReference(GetSegmentBlobName(i)), this.BlobManager));

                if (!ret)
                {
                    throw new InvalidOperationException("Recovery of blobs is single-threaded and should not yield any failure due to concurrency");
                }
            }
        }

        /// <summary>
        /// Is called on exceptions, if non-null; can be set by application
        /// </summary>
        private IBlobManager BlobManager { get; set; }

        private string GetSegmentBlobName(int segmentId)
        {
            return $"{blobName}.{segmentId}";
        }

        /// <summary>
        /// <see cref="IDevice.Close">Inherited</see>
        /// </summary>
        public override void Close()
        {
            // Unlike in LocalStorageDevice, we explicitly remove all page blobs if the deleteOnClose flag is set, instead of relying on the operating system
            // to delete files after the end of our process. This leads to potential problems if multiple instances are sharing the same underlying page blobs.
            // Since this flag is only used for testing, it is probably fine.
            if (deleteOnClose)
            {
                foreach (var entry in blobs)
                {
                    entry.Value.PageBlob.Delete();
                }
            }
        }

        /// <summary>
        /// <see cref="IDevice.RemoveSegmentAsync(int, AsyncCallback, IAsyncResult)"/>
        /// </summary>
        /// <param name="segment"></param>
        /// <param name="callback"></param>
        /// <param name="result"></param>
        public override void RemoveSegmentAsync(int segment, AsyncCallback callback, IAsyncResult result)
        {
            if (this.blobs.TryRemove(segment, out BlobEntry blob))
            {
                CloudPageBlob pageBlob = blob.PageBlob;

                if (this.underLease)
                {
                    this.BlobManager.ConfirmLeaseAsync().GetAwaiter().GetResult();
                }

                if (!this.BlobManager.CancellationToken.IsCancellationRequested)
                {
                    pageBlob.DeleteAsync(cancellationToken: this.BlobManager.CancellationToken)
                       .ContinueWith((Task t) =>
                       {
                           if (t.IsFaulted)
                           {
                               this.BlobManager?.HandleBlobError(nameof(RemoveSegmentAsync), "could not remove page blob for segment", pageBlob?.Name, t.Exception, false);
                           }
                           callback(result);
                       });
                }
            }
        }

        //---- The actual read and write accesses to the page blobs

        private unsafe Task WritePortionToBlobUnsafeAsync(CloudPageBlob blob, IntPtr sourceAddress, long destinationAddress, long offset, uint length)
        {
            return this.WritePortionToBlobAsync(new UnmanagedMemoryStream((byte*)sourceAddress + offset, length), blob, sourceAddress, destinationAddress, offset, length);
        }

        private async Task WritePortionToBlobAsync(UnmanagedMemoryStream stream, CloudPageBlob blob, IntPtr sourceAddress, long destinationAddress, long offset, uint length)
        {
            try
            {
                if (this.underLease)
                {
                    await this.BlobManager.ConfirmLeaseAsync().ConfigureAwait(false);
                }

                await blob.WritePagesAsync(stream, destinationAddress + offset,
                    contentChecksum: null, accessCondition: null, options: this.BlobRequestOptions, operationContext: null, cancellationToken: this.BlobManager.CancellationToken).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                this.BlobManager?.HandleBlobError(nameof(WritePortionToBlobAsync), "could not write to page blob", blob?.Name, exception, true);
                throw;
            }
            finally
            {
                stream.Dispose();
            }
        }

        private unsafe Task ReadFromBlobUnsafeAsync(CloudPageBlob blob, long sourceAddress, long destinationAddress, uint readLength)
        {
            return this.ReadFromBlobAsync(new UnmanagedMemoryStream((byte*)destinationAddress, readLength, readLength, FileAccess.Write), blob, sourceAddress, destinationAddress, readLength);
        }

        private async Task ReadFromBlobAsync(UnmanagedMemoryStream stream, CloudPageBlob blob, long sourceAddress, long destinationAddress, uint readLength)
        {
            Debug.WriteLine($"AzureStorageDevice.ReadFromBlobAsync Called target={blob.Name}");

            try
            {
                if (this.underLease)
                {
                    Debug.WriteLine($"confirm lease");
                    await this.BlobManager.ConfirmLeaseAsync().ConfigureAwait(false);
                    Debug.WriteLine($"confirm lease done");
                }

                Debug.WriteLine($"starting download target={blob.Name} readLength={readLength} sourceAddress={sourceAddress}");

                await blob.DownloadRangeToStreamAsync(stream, sourceAddress, readLength,
                         accessCondition: null, options: this.BlobRequestOptions, operationContext: null, cancellationToken: this.BlobManager.CancellationToken);

                Debug.WriteLine($"finished download target={blob.Name} readLength={readLength} sourceAddress={sourceAddress}");

                if (stream.Position != readLength)
                {
                    throw new InvalidDataException($"wrong amount of data received from page blob, expected={readLength}, actual={stream.Position}");
                }
            }
            catch (Exception exception)
            {
                this.BlobManager?.HandleBlobError(nameof(ReadFromBlobAsync), "could not read from page blob", blob?.Name, exception, true);
                throw new FasterException(nameof(ReadFromBlobAsync) + "could not read from page blob " + blob?.Name, exception);
            }
            finally
            {
                stream.Dispose();
            }
        }

        //---- the overridden methods represent the interface for a generic storage device

        /// <summary>
        /// <see cref="IDevice.ReadAsync(int, ulong, IntPtr, uint, IOCompletionCallback, IAsyncResult)">Inherited</see>
        /// </summary>
        public override unsafe void ReadAsync(int segmentId, ulong sourceAddress, IntPtr destinationAddress, uint readLength, IOCompletionCallback callback, IAsyncResult asyncResult)
        {
            Debug.WriteLine($"AzureStorageDevice.ReadAsync Called segmentId={segmentId} sourceAddress={sourceAddress} readLength={readLength}");

            // It is up to the allocator to make sure no reads are issued to segments before they are written
            if (!blobs.TryGetValue(segmentId, out BlobEntry blobEntry))
            {
                var nonLoadedBlob = this.blobDirectory.GetPageBlobReference(GetSegmentBlobName(segmentId));
                var exception = new InvalidOperationException("Attempt to read a non-loaded segment");
                this.BlobManager?.HandleBlobError(nameof(ReadAsync), exception.Message, nonLoadedBlob?.Name, exception, true);
                throw exception;
            }

            // Even though Azure Page Blob does not make use of Overlapped, we populate one to conform to the callback API
            Overlapped ov = new Overlapped(0, 0, IntPtr.Zero, asyncResult);
            NativeOverlapped* ovNative = ov.UnsafePack(callback, IntPtr.Zero);

            this.ReadFromBlobUnsafeAsync(blobEntry.PageBlob, (long)sourceAddress, (long)destinationAddress, readLength)
                  .ContinueWith((Task t) =>
                  {
                      if (t.IsFaulted)
                      {
                          Debug.WriteLine("AzureStorageDevice.ReadAsync Returned (Failure)");
                          callback(uint.MaxValue, readLength, ovNative);
                      }
                      else
                      {
                          Debug.WriteLine("AzureStorageDevice.ReadAsync Returned");
                          callback(0, readLength, ovNative);
                      }
                  });
        }

        /// <summary>
        /// <see cref="IDevice.WriteAsync(IntPtr, int, ulong, uint, IOCompletionCallback, IAsyncResult)">Inherited</see>
        /// </summary>
        public override void WriteAsync(IntPtr sourceAddress, int segmentId, ulong destinationAddress, uint numBytesToWrite, IOCompletionCallback callback, IAsyncResult asyncResult)
        {
            Debug.WriteLine($"AzureStorageDevice.WriteAsync Called segmentId={segmentId} destinationAddress={destinationAddress} numBytesToWrite={numBytesToWrite}");

            if (!blobs.TryGetValue(segmentId, out BlobEntry blobEntry))
            {
                BlobEntry entry = new BlobEntry(this.BlobManager);
                if (blobs.TryAdd(segmentId, entry))
                {
                    CloudPageBlob pageBlob = this.blobDirectory.GetPageBlobReference(GetSegmentBlobName(segmentId));

                    // If segment size is -1, which denotes absence, we request the largest possible blob. This is okay because
                    // page blobs are not backed by real pages on creation, and the given size is only a the physical limit of 
                    // how large it can grow to.
                    var size = segmentSize == -1 ? MAX_BLOB_SIZE : segmentSize;

                    // If no blob exists for the segment, we must first create the segment asynchronouly. (Create call takes ~70 ms by measurement)
                    // After creation is done, we can call write.
                    var _ = entry.CreateAsync(size, pageBlob);
                }
                // Otherwise, some other thread beat us to it. Okay to use their blobs.
                blobEntry = blobs[segmentId];
            }
            this.TryWriteAsync(blobEntry, sourceAddress, destinationAddress, numBytesToWrite, callback, asyncResult);
        }

        private void TryWriteAsync(BlobEntry blobEntry, IntPtr sourceAddress, ulong destinationAddress, uint numBytesToWrite, IOCompletionCallback callback, IAsyncResult asyncResult)
        {
            // If pageBlob is null, it is being created. Attempt to queue the write for the creator to complete after it is done
            if (blobEntry.PageBlob == null
                && blobEntry.TryQueueAction(p => this.WriteToBlobAsync(p, sourceAddress, destinationAddress, numBytesToWrite, callback, asyncResult)))
            {
                return;
            }
            // Otherwise, invoke directly.
            this.WriteToBlobAsync(blobEntry.PageBlob, sourceAddress, destinationAddress, numBytesToWrite, callback, asyncResult);
        }

        private unsafe void WriteToBlobAsync(CloudPageBlob blob, IntPtr sourceAddress, ulong destinationAddress, uint numBytesToWrite, IOCompletionCallback callback, IAsyncResult asyncResult)
        {
            // Even though Azure Page Blob does not make use of Overlapped, we populate one to conform to the callback API
            Overlapped ov = new Overlapped(0, 0, IntPtr.Zero, asyncResult);
            NativeOverlapped* ovNative = ov.UnsafePack(callback, IntPtr.Zero);

            this.WriteToBlobAsync(blob, sourceAddress, (long)destinationAddress, numBytesToWrite)
                .ContinueWith((Task t) =>
                {
                    if (t.IsFaulted)
                    {
                        Debug.WriteLine("AzureStorageDevice.WriteAsync Returned (Failure)");
                        callback(uint.MaxValue, numBytesToWrite, ovNative);
                    }
                    else
                    {
                        Debug.WriteLine("AzureStorageDevice.WriteAsync Returned");
                        callback(0, numBytesToWrite, ovNative);
                    }
                });
        }

        const int maxPortionSizeForPageBlobWrites = 4 * 1024 * 1024; // 4 MB is a limit on page blob write portions, apparently

        private async Task WriteToBlobAsync(CloudPageBlob blob, IntPtr sourceAddress, long destinationAddress, uint numBytesToWrite)
        {
            long offset = 0;
            while (numBytesToWrite > 0)
            {
                var length = Math.Min(numBytesToWrite, maxPortionSizeForPageBlobWrites);
                await this.WritePortionToBlobUnsafeAsync(blob, sourceAddress, destinationAddress, offset, length).ConfigureAwait(false);
                numBytesToWrite -= length;
                offset += length;
            }
        }
    }
}