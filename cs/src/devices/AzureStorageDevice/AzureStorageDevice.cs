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

        internal BlobRequestOptions BlobRequestOptionsWithoutRetry { get; private set; }
        internal BlobRequestOptions BlobRequestOptionsWithRetry { get; private set; }

        // Azure Page Blobs have a fixed sector size of 512 bytes.
        private const uint PAGE_BLOB_SECTOR_SIZE = 512;

        // Max upload size must be at most 4MB
        // we use an even smaller value to improve retry/timeout behavior in highly contended situations
        private const uint MAX_UPLOAD_SIZE = 1024 * 1024;

        // Max Azure Page Blob size (used when segment size is not specified): we set this at 8 GB
        private const long MAX_PAGEBLOB_SIZE = 8L * 1024 * 1024 * 1024;

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
        /// True if the program should delete all blobs created on call to <see cref="Dispose">Close</see>. False otherwise. 
        /// The container is not deleted even if it was created in this constructor
        /// </param>
        /// <param name="capacity">The maximum number of bytes this storage device can accommondate, or CAPACITY_UNSPECIFIED if there is no such limit </param>
        public AzureStorageDevice(CloudBlobDirectory cloudBlobDirectory, string blobName, IBlobManager blobManager = null, bool underLease = false, bool deleteOnClose = false, long capacity = Devices.CAPACITY_UNSPECIFIED)
            : base($"{cloudBlobDirectory}/{blobName}", PAGE_BLOB_SECTOR_SIZE, capacity)
        {
            this.blobs = new ConcurrentDictionary<int, BlobEntry>();
            this.blobDirectory = cloudBlobDirectory;
            this.blobName = blobName;
            this.underLease = underLease;
            this.deleteOnClose = deleteOnClose;

            this.BlobManager = blobManager ?? new DefaultBlobManager(this.underLease, this.blobDirectory);
            this.BlobRequestOptionsWithoutRetry = BlobManager.GetBlobRequestOptionsWithoutRetry();
            this.BlobRequestOptionsWithRetry = BlobManager.GetBlobRequestOptionsWithRetry();

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
        /// True if the program should delete all blobs created on call to <see cref="Dispose">Close</see>. False otherwise. 
        /// The container is not deleted even if it was created in this constructor
        /// </param>
        /// <param name="capacity">The maximum number of bytes this storage device can accommondate, or CAPACITY_UNSPECIFIED if there is no such limit </param>
        public AzureStorageDevice(string connectionString, string containerName, string directoryName, string blobName, IBlobManager blobManager = null, bool underLease = false, bool deleteOnClose = false, long capacity = Devices.CAPACITY_UNSPECIFIED)
            : base($"{connectionString}/{containerName}/{directoryName}/{blobName}", PAGE_BLOB_SECTOR_SIZE, capacity)
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
            this.BlobRequestOptionsWithoutRetry = BlobManager.GetBlobRequestOptionsWithoutRetry();
            this.BlobRequestOptionsWithRetry = BlobManager.GetBlobRequestOptionsWithRetry();

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
                    currentToken: continuationToken, options: this.BlobRequestOptionsWithRetry, operationContext: null)
                    .ConfigureAwait(BlobManager.ConfigureAwaitForStorage);

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

        /// <inheritdoc />
        public override void Dispose()
        {
            // Unlike in LocalStorageDevice, we explicitly remove all page blobs if the deleteOnClose flag is set, instead of relying on the operating system
            // to delete files after the end of our process. This leads to potential problems if multiple instances are sharing the same underlying page blobs.
            // Since this flag is only used for testing, it is probably fine.
            if (deleteOnClose)
                PurgeAll();
        }

        /// <summary>
        /// Purge all blobs related to this device. Do not use if 
        /// multiple instances are sharing the same underlying page blobs.
        /// </summary>
        public void PurgeAll()
        {
            foreach (var entry in blobs)
            {
                entry.Value.PageBlob.Delete();
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
                    this.BlobManager.ConfirmLeaseAsync().GetAwaiter().GetResult();  // REVIEW: this method cannot avoid GetAwaiter
                }

                if (!this.BlobManager.CancellationToken.IsCancellationRequested)
                {
                    var t = pageBlob.DeleteAsync(cancellationToken: this.BlobManager.CancellationToken);
                    t.GetAwaiter().GetResult();                                     // REVIEW: this method cannot avoid GetAwaiter
                    if (t.IsFaulted)
                    {
                        this.BlobManager?.HandleBlobError(nameof(RemoveSegmentAsync), "could not remove page blob for segment", pageBlob?.Name, t.Exception, false);
                    }
                    callback(result);
                }
            }
        }

        /// <inheritdoc/>
        public override long GetFileSize(int segmentId)
        {
            // We didn't find segment in blob cache
            if (!blobs.TryGetValue(segmentId, out _))
                return 0;
            return segmentSize == -1 ? MAX_PAGEBLOB_SIZE : segmentSize;
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
                await BlobManager.AsyncStorageWriteMaxConcurrency.WaitAsync().ConfigureAwait(false);

                int numAttempts = 0;
                long streamPosition = stream.Position;

                while (true) // retry loop
                {
                    numAttempts++;
                    try
                    {
                        if (this.underLease)
                        {
                            await this.BlobManager.ConfirmLeaseAsync().ConfigureAwait(false);
                        }

                        if (length > 0)
                        {
                            await blob.WritePagesAsync(stream, destinationAddress + offset,
                                contentChecksum: null, accessCondition: null, options: this.BlobRequestOptionsWithoutRetry, operationContext: null, cancellationToken: this.BlobManager.CancellationToken)
                                .ConfigureAwait(BlobManager.ConfigureAwaitForStorage);
                        }
                        break;
                    }
                    catch (StorageException e) when (this.underLease && IsTransientStorageError(e) && numAttempts < BlobManager.MaxRetries)
                    {
                        TimeSpan nextRetryIn = TimeSpan.FromSeconds(1 + Math.Pow(2, (numAttempts - 1)));
                        this.BlobManager?.HandleBlobError(nameof(WritePortionToBlobAsync), $"could not write to page blob, will retry in {nextRetryIn}s", blob?.Name, e, false);
                        await Task.Delay(nextRetryIn).ConfigureAwait(false);
                        stream.Seek(streamPosition, SeekOrigin.Begin);  // must go back to original position before retrying
                        continue;
                    }
                    catch (Exception exception) when (!IsFatal(exception))
                    {
                        this.BlobManager?.HandleBlobError(nameof(WritePortionToBlobAsync), "could not write to page blob", blob?.Name, exception, false);
                        throw;
                    }
                };
            }
            finally
            {
                BlobManager.AsyncStorageWriteMaxConcurrency.Release();
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
                await BlobManager.AsyncStorageReadMaxConcurrency.WaitAsync().ConfigureAwait(false);

                int numAttempts = 0;

                while (true) // retry loop
                {
                    numAttempts++;
                    try
                    {
                        if (this.underLease)
                        {
                            await this.BlobManager.ConfirmLeaseAsync().ConfigureAwait(false);
                        }

                        Debug.WriteLine($"starting download target={blob.Name} readLength={readLength} sourceAddress={sourceAddress}");

                        if (readLength > 0)
                        {
                            await blob.DownloadRangeToStreamAsync(stream, sourceAddress, readLength,
                                 accessCondition: null, options: this.BlobRequestOptionsWithoutRetry, operationContext: null, cancellationToken: this.BlobManager.CancellationToken)
                                .ConfigureAwait(BlobManager.ConfigureAwaitForStorage);
                        }

                        Debug.WriteLine($"finished download target={blob.Name} readLength={readLength} sourceAddress={sourceAddress}");

                        if (stream.Position != readLength)
                        {
                            throw new InvalidDataException($"wrong amount of data received from page blob, expected={readLength}, actual={stream.Position}");
                        }
                        break;
                    }
                    catch (StorageException e) when (this.underLease && IsTransientStorageError(e) && numAttempts < BlobManager.MaxRetries)
                    {
                        TimeSpan nextRetryIn = TimeSpan.FromSeconds(1 + Math.Pow(2, (numAttempts - 1)));
                        this.BlobManager?.HandleBlobError(nameof(ReadFromBlobAsync), $"could not write to page blob, will retry in {nextRetryIn}s", blob?.Name, e, false);
                        await Task.Delay(nextRetryIn).ConfigureAwait(false);
                        stream.Seek(0, SeekOrigin.Begin); // must go back to original position before retrying
                        continue;
                    }
                    catch (Exception exception) when (!IsFatal(exception))
                    {
                        this.BlobManager?.HandleBlobError(nameof(ReadFromBlobAsync), "could not read from page blob", blob?.Name, exception, false);
                        throw;
                    }
                }
            }
            finally
            {
                BlobManager.AsyncStorageReadMaxConcurrency.Release();
                stream.Dispose();
            }
        }

        //---- the overridden methods represent the interface for a generic storage device

        /// <summary>
        /// <see cref="IDevice.ReadAsync(int, ulong, IntPtr, uint, DeviceIOCompletionCallback, object)">Inherited</see>
        /// </summary>
        public override unsafe void ReadAsync(int segmentId, ulong sourceAddress, IntPtr destinationAddress, uint readLength, DeviceIOCompletionCallback callback, object context)
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

            this.ReadFromBlobUnsafeAsync(blobEntry.PageBlob, (long)sourceAddress, (long)destinationAddress, readLength)
                  .ContinueWith((Task t) =>
                  {
                      if (t.IsFaulted)
                      {
                          Debug.WriteLine("AzureStorageDevice.ReadAsync Returned (Failure)");
                          callback(uint.MaxValue, readLength, context);
                      }
                      else
                      {
                          Debug.WriteLine("AzureStorageDevice.ReadAsync Returned");
                          callback(0, readLength, context);
                      }
                  });
        }

        /// <summary>
        /// <see cref="IDevice.WriteAsync(IntPtr, int, ulong, uint, DeviceIOCompletionCallback, object)">Inherited</see>
        /// </summary>
        public override void WriteAsync(IntPtr sourceAddress, int segmentId, ulong destinationAddress, uint numBytesToWrite, DeviceIOCompletionCallback callback, object context)
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
                    var size = segmentSize == -1 ? MAX_PAGEBLOB_SIZE : segmentSize;

                    // If no blob exists for the segment, we must first create the segment asynchronouly. (Create call takes ~70 ms by measurement)
                    // After creation is done, we can call write.
                    var _ = entry.CreateAsync(size, pageBlob);
                }
                // Otherwise, some other thread beat us to it. Okay to use their blobs.
                blobEntry = blobs[segmentId];
            }
            this.TryWriteAsync(blobEntry, sourceAddress, destinationAddress, numBytesToWrite, callback, context);
        }

        private void TryWriteAsync(BlobEntry blobEntry, IntPtr sourceAddress, ulong destinationAddress, uint numBytesToWrite, DeviceIOCompletionCallback callback, object context)
        {
            // If pageBlob is null, it is being created. Attempt to queue the write for the creator to complete after it is done
            if (blobEntry.PageBlob == null
                && blobEntry.TryQueueAction(p => this.WriteToBlobAsync(p, sourceAddress, destinationAddress, numBytesToWrite, callback, context)))
            {
                return;
            }
            // Otherwise, invoke directly.
            this.WriteToBlobAsync(blobEntry.PageBlob, sourceAddress, destinationAddress, numBytesToWrite, callback, context);
        }

        private unsafe void WriteToBlobAsync(CloudPageBlob blob, IntPtr sourceAddress, ulong destinationAddress, uint numBytesToWrite, DeviceIOCompletionCallback callback, object context)
        {
            this.WriteToBlobAsync(blob, sourceAddress, (long)destinationAddress, numBytesToWrite)
                .ContinueWith((Task t) =>
                {
                    if (t.IsFaulted)
                    {
                        Debug.WriteLine("AzureStorageDevice.WriteAsync Returned (Failure)");
                        callback(uint.MaxValue, numBytesToWrite, context);
                    }
                    else
                    {
                        Debug.WriteLine("AzureStorageDevice.WriteAsync Returned");
                        callback(0, numBytesToWrite, context);
                    }
                });
        }

        private async Task WriteToBlobAsync(CloudPageBlob blob, IntPtr sourceAddress, long destinationAddress, uint numBytesToWrite)
        {
            long offset = 0;
            while (numBytesToWrite > 0)
            {
                var length = Math.Min(numBytesToWrite, MAX_UPLOAD_SIZE);
                await this.WritePortionToBlobUnsafeAsync(blob, sourceAddress, destinationAddress, offset, length).ConfigureAwait(false);
                numBytesToWrite -= length;
                offset += length;
            }
        }

        private static bool IsTransientStorageError(StorageException e)
        {
            return (e.RequestInformation.HttpStatusCode == 408)  //408 Request Timeout
                || (e.RequestInformation.HttpStatusCode == 429)  //429 Too Many Requests
                || (e.RequestInformation.HttpStatusCode == 500)  //500 Internal Server Error
                || (e.RequestInformation.HttpStatusCode == 502)  //502 Bad Gateway
                || (e.RequestInformation.HttpStatusCode == 503)  //503 Service Unavailable
                || (e.RequestInformation.HttpStatusCode == 504); //504 Gateway Timeout
        }

        private static bool IsFatal(Exception exception)
        {
            if (exception is OutOfMemoryException || exception is StackOverflowException)
            {
                return true;
            }
            return false;
        }
    }
}
