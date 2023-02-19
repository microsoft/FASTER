// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace FASTER.devices
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Azure.Storage.Blobs.Models;
    using FASTER.core;
    using Microsoft.Extensions.Logging;

    /// <summary>
    /// A IDevice Implementation that is backed by<see href="https://docs.microsoft.com/en-us/azure/storage/blobs/storage-blob-pageblob-overview">Azure Page Blob</see>.
    /// This device is slower than a local SSD or HDD, but provides scalability and shared access in the cloud.
    /// </summary>
    public class AzureStorageDevice : StorageDeviceBase
    {
        readonly ConcurrentDictionary<int, BlobEntry> blobs;
        readonly BlobUtilsV12.BlobDirectory pageBlobDirectory;
        readonly string blobName;
        readonly bool underLease;
        readonly ConcurrentDictionary<long, ReadWriteRequestInfo> pendingReadWriteOperations;
        readonly ConcurrentDictionary<long, RemoveRequestInfo> pendingRemoveOperations;
        readonly Timer hangCheckTimer;
        readonly SemaphoreSlim singleWriterSemaphore;
        readonly TimeSpan limit;
        readonly bool localBlobManager;

        // Whether blob files are deleted on close
        private readonly bool deleteOnClose;

        static long sequenceNumber;

        struct ReadWriteRequestInfo
        {
            public bool IsRead;
            public DeviceIOCompletionCallback Callback;
            public uint NumBytes;
            public object Context;
            public DateTime TimeStamp;
        }

        struct RemoveRequestInfo
        {
            public AsyncCallback Callback;
            public IAsyncResult Result;
            public DateTime TimeStamp;
        }

        SemaphoreSlim SingleWriterSemaphore => this.singleWriterSemaphore;

        internal IStorageErrorHandler StorageErrorHandler { get; private set; }

        // Azure Page Blobs have a fixed sector size of 512 bytes.
        const uint PAGE_BLOB_SECTOR_SIZE = 512;
        // Max upload size must be at most 4MB
        // we use an even smaller value to improve retry/timeout behavior in highly contended situations
        // Also, this allows us to use aggressive timeouts to kill stragglers
        const uint MAX_UPLOAD_SIZE = 1024 * 1024;
        const uint MAX_DOWNLOAD_SIZE = 1024 * 1024;

        const long MAX_PAGEBLOB_SIZE = 512L * 1024 * 1024 * 1024; // set this at 512 GB for now TODO consider implications

        /// <summary>
        /// Constructs a new AzureStorageDevice instance, backed by Azure Page Blobs
        /// </summary>
        /// <param name="connectionString"> The connection string to use when establishing connection to Azure Blobs</param>
        /// <param name="containerName">Name of the Azure Blob container to use. If there does not exist a container with the supplied name, one is created</param>
        /// <param name="directoryName">Directory within blob container to use.</param>
        /// <param name="blobName">A descriptive name that will be the prefix of all blobs created with this device</param>
        /// <param name="blobManager">Blob manager instance</param>
        /// <param name="underLease">Whether we use leases</param>
        /// <param name="deleteOnClose">
        /// True if the program should delete all blobs created on call to <see cref="Dispose">Close</see>. False otherwise. 
        /// The container is not deleted even if it was created in this constructor
        /// </param>
        /// <param name="capacity">The maximum number of bytes this storage device can accommodate, or CAPACITY_UNSPECIFIED if there is no such limit </param>
        /// <param name="logger">Logger</param>
        public AzureStorageDevice(string connectionString, string containerName, string directoryName, string blobName, IBlobManager blobManager = null, bool underLease = false, bool deleteOnClose = false, long capacity = Devices.CAPACITY_UNSPECIFIED, ILogger logger = null)
            : base($"{connectionString}/{containerName}/{directoryName}/{blobName}", PAGE_BLOB_SECTOR_SIZE, capacity)
        {
            var pageBlobAccount = BlobUtilsV12.GetServiceClients(connectionString);
            var pageBlobContainer = BlobUtilsV12.GetContainerClients(pageBlobAccount, containerName);
            if (!pageBlobContainer.WithRetries.Exists())
                pageBlobContainer.WithRetries.CreateIfNotExists();
            var pageBlobDirectory = new BlobUtilsV12.BlobDirectory(pageBlobContainer, directoryName);

            this.deleteOnClose = deleteOnClose;
            this.blobs = new ConcurrentDictionary<int, BlobEntry>();
            this.pendingReadWriteOperations = new ConcurrentDictionary<long, ReadWriteRequestInfo>();
            this.pendingRemoveOperations = new ConcurrentDictionary<long, RemoveRequestInfo>();
            this.pageBlobDirectory = pageBlobDirectory;
            this.blobName = blobName;

            if (blobManager == null) localBlobManager = true;
            this.BlobManager = blobManager ?? new BlobManager(logger, logger, LogLevel.Information, null, underLease, pageBlobDirectory, blobName);

            this.StorageErrorHandler = BlobManager.StorageErrorHandler;
            this.StorageErrorHandler.Token.Register(this.CancelAllRequests);
            this.underLease = underLease;
            this.hangCheckTimer = new Timer(this.DetectHangs, null, 0, 20000);
            this.singleWriterSemaphore = underLease ? new SemaphoreSlim(1) : null;
            this.limit = TimeSpan.FromSeconds(90);

            StartAsync().Wait();
        }

        /// <summary>
        /// Constructs a new AzureStorageDevice instance, backed by Azure Page Blobs
        /// </summary>
        /// <param name="blobName">A descriptive name that will be the prefix of all segments created</param>
        /// <param name="pageBlobDirectory">the directory containing the page blobs</param>
        /// <param name="blobManager">the blob manager handling the leases</param>
        /// <param name="underLease">whether this device needs to be protected by the lease</param>
        /// <param name="deleteOnClose">
        /// True if the program should delete all blobs created on call to <see cref="Dispose">Close</see>. False otherwise. 
        /// The container is not deleted even if it was created in this constructor
        /// </param>
        /// <param name="logger">Logger</param>
        internal AzureStorageDevice(string blobName, BlobUtilsV12.BlobDirectory pageBlobDirectory, BlobManager blobManager = null, bool underLease = false, bool deleteOnClose = false, ILogger logger = null)
        : base($"{pageBlobDirectory}/{blobName}", PAGE_BLOB_SECTOR_SIZE, Devices.CAPACITY_UNSPECIFIED)
        {
            this.deleteOnClose = deleteOnClose;
            this.blobs = new ConcurrentDictionary<int, BlobEntry>();
            this.pendingReadWriteOperations = new ConcurrentDictionary<long, ReadWriteRequestInfo>();
            this.pendingRemoveOperations = new ConcurrentDictionary<long, RemoveRequestInfo>();
            this.pageBlobDirectory = pageBlobDirectory;
            this.blobName = blobName;

            if (blobManager == null) localBlobManager = true;
            this.BlobManager = blobManager ?? new BlobManager(logger, logger, LogLevel.Information, null, underLease, pageBlobDirectory, blobName);

            this.StorageErrorHandler = BlobManager.StorageErrorHandler;
            this.StorageErrorHandler.Token.Register(this.CancelAllRequests);
            this.underLease = underLease;
            this.hangCheckTimer = new Timer(this.DetectHangs, null, 0, 20000);
            this.singleWriterSemaphore = underLease ? new SemaphoreSlim(1) : null;
            this.limit = TimeSpan.FromSeconds(90);

            StartAsync().Wait();
        }

        /// <inheritdoc/>
        public override string ToString()
        {
            return $"AzureStorageDevice {this.pageBlobDirectory}{this.blobName}";
        }

        async Task StartAsync()
        {
            try
            {
                this.BlobManager?.StorageTracer?.FasterStorageProgress($"StorageOpCalled AzureStorageDevice.StartAsync target={this.pageBlobDirectory}{this.blobName}");

                // list all the blobs representing the segments
                var prefix = $"{this.pageBlobDirectory}{this.blobName}.";

                string continuationToken = null;
                IReadOnlyList<BlobItem> pageResults = null;

                do
                {
                    await this.BlobManager.PerformWithRetriesAsync(
                        BlobManager.AsynchronousStorageReadMaxConcurrency,
                        this.underLease,
                        "BlobContainerClient.GetBlobsAsync",
                        "RecoverDevice",
                        $"continuationToken={continuationToken}",
                        this.pageBlobDirectory.ToString(),
                        2000,
                        true,
                        async (numAttempts) =>
                        {
                            var client = this.pageBlobDirectory.Client.WithRetries;

                            var page = await client.GetBlobsAsync(
                                prefix: prefix,
                                cancellationToken: this.StorageErrorHandler.Token)
                                .AsPages(continuationToken, 100)
                                .FirstAsync();

                            pageResults = page.Values;
                            continuationToken = page.ContinuationToken;
                            return page.Values.Count; // not accurate, in terms of bytes, but still useful for tracing purposes
                        });

                    foreach (var item in pageResults)
                    {
                        if (Int32.TryParse(item.Name.Replace(prefix, ""), out int segmentId))
                        {
                            this.BlobManager?.StorageTracer?.FasterStorageProgress($"AzureStorageDevice.StartAsync found segment={item.Name}");

                            bool ret = this.blobs.TryAdd(segmentId, new BlobEntry(BlobUtilsV12.GetPageBlobClients(this.pageBlobDirectory.Client, item.Name), item.Properties.ETag.Value, this));

                            if (!ret)
                            {
                                throw new InvalidOperationException("Recovery of blobs is single-threaded and should not yield any failure due to concurrency");
                            }
                        }
                    }
                }
                while (!string.IsNullOrEmpty(continuationToken));

                // make sure we did not lose the lease while iterating to find the blobs
                await this.BlobManager.ConfirmLeaseIsGoodForAWhileAsync();
                this.StorageErrorHandler.Token.ThrowIfCancellationRequested();


                // find longest contiguous sequence at end
                var keys = this.blobs.Keys.ToList();
                if (keys.Count == 0)
                {
                    // nothing has been written to this device so far.
                    this.startSegment = 0;
                    this.endSegment = -1;
                }
                else
                {
                    keys.Sort();
                    this.endSegment = this.startSegment = keys[keys.Count - 1];
                    for (int i = keys.Count - 2; i >= 0; i--)
                    {
                        if (keys[i] == keys[i + 1] - 1)
                        {
                            this.startSegment = i;
                        }
                    }
                }

                this.BlobManager?.StorageTracer?.FasterStorageProgress($"StorageOpReturned AzureStorageDevice.StartAsync, determined segment range for {this.pageBlobDirectory.Prefix}{this.blobName}: start={this.startSegment} end={this.endSegment}");
            }
            catch
            {
                this.BlobManager?.StorageTracer?.FasterStorageProgress($"StorageOpReturned AzureStorageDevice.StartAsync failed");
                throw;
            }
        }


        /// <summary>
        /// Is called on exceptions, if non-null; can be set by application
        /// </summary>
        internal IBlobManager BlobManager { get; set; }

        string GetSegmentBlobName(int segmentId)
        {
            return $"{this.blobName}.{segmentId}";
        }

        internal void DetectHangs(object _)
        {
            DateTime threshold = DateTime.UtcNow - (Debugger.IsAttached ? TimeSpan.FromMinutes(30) : this.limit);

            foreach (var kvp in this.pendingReadWriteOperations)
            {
                if (kvp.Value.TimeStamp < threshold)
                {
                    this.BlobManager.StorageErrorHandler.HandleError("DetectHangs", $"storage operation id={kvp.Key} has exceeded the time limit {this.limit}", null, true, false);
                    return;
                }
            }
            foreach (var kvp in this.pendingRemoveOperations)
            {
                if (kvp.Value.TimeStamp < threshold)
                {
                    this.BlobManager.StorageErrorHandler.HandleError("DetectHangs", $"storage operation id={kvp.Key} has exceeded the time limit {this.limit}", null, true, false);
                    return;
                }
            }
        }

        void CancelAllRequests()
        {
            foreach (var id in this.pendingReadWriteOperations.Keys.ToList())
            {
                if (this.pendingReadWriteOperations.TryRemove(id, out var request))
                {
                    if (request.IsRead)
                    {
                        this.BlobManager?.StorageTracer?.FasterStorageProgress($"StorageOpReturned AzureStorageDevice.ReadAsync id={id} (Canceled)");
                    }
                    else
                    {
                        this.BlobManager?.StorageTracer?.FasterStorageProgress($"StorageOpReturned AzureStorageDevice.WriteAsync id={id} (Canceled)");
                    }
                    request.Callback(uint.MaxValue, request.NumBytes, request.Context);
                }
            }
            foreach (var id in this.pendingRemoveOperations.Keys.ToList())
            {
                if (this.pendingRemoveOperations.TryRemove(id, out var request))
                {
                    this.BlobManager?.StorageTracer?.FasterStorageProgress($"StorageOpReturned AzureStorageDevice.RemoveSegmentAsync id={id} (Canceled)");
                    request.Callback(request.Result);
                }
            }
        }

        //---- the overridden methods represent the interface for a generic storage device

        /// <summary>
        /// <see cref="StorageDeviceBase.Dispose">Inherited</see>
        /// </summary>
        public override void Dispose()
        {
            if (localBlobManager)
                BlobManager.StopAsync().Wait();

            this.hangCheckTimer.Dispose();
            this.singleWriterSemaphore?.Dispose();

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
                entry.Value.PageBlob.Default?.Delete();
            }
        }

        /// <inheritdoc/>
        public override long GetFileSize(int segment)
        {
            if (!this.blobs.TryGetValue(segment, out BlobEntry blobEntry))
                return 0;
            long size = blobEntry.PageBlob.Default.GetProperties().Value.ContentLength;
            return size;
        }

        /// <summary>
        /// <see cref="IDevice.RemoveSegmentAsync(int, AsyncCallback, IAsyncResult)"/>
        /// </summary>
        /// <param name="segment"></param>
        /// <param name="callback"></param>
        /// <param name="result"></param>
        public override void RemoveSegmentAsync(int segment, AsyncCallback callback, IAsyncResult result)
        {
            long id = Interlocked.Increment(ref AzureStorageDevice.sequenceNumber);

            this.BlobManager?.StorageTracer?.FasterStorageProgress($"StorageOpCalled AzureStorageDevice.RemoveSegmentAsync id={id} segment={segment}");

            this.pendingRemoveOperations.TryAdd(id, new RemoveRequestInfo()
            {
                Callback = callback,
                Result = result,
                TimeStamp = DateTime.UtcNow
            });

            Task deletionTask = Task.CompletedTask;

            if (this.blobs.TryRemove(segment, out BlobEntry entry))
            {
                deletionTask = this.BlobManager.PerformWithRetriesAsync(
                    null,
                    this.underLease,
                    "BlobBaseClient.DeleteAsync",
                    "DeleteDeviceSegment",
                    "",
                    entry.PageBlob.Default.Name,
                    5000,
                    true,
                    async (numAttempts) =>
                    {
                        var client = (numAttempts > 1) ? entry.PageBlob.Default : entry.PageBlob.Aggressive;
                        await client.DeleteAsync(cancellationToken: this.StorageErrorHandler.Token);
                        return 1;
                    });
            }
                
            deletionTask.ContinueWith((Task t) =>
            {
                if (this.pendingRemoveOperations.TryRemove(id, out var request))
                {
                    this.BlobManager?.StorageTracer?.FasterStorageProgress($"StorageOpReturned AzureStorageDevice.RemoveSegmentAsync id={id}");
                    request.Callback(request.Result);
                }
            }, TaskContinuationOptions.ExecuteSynchronously);
        }

        /// <summary>
        /// Delete the device blobs in storage.
        /// </summary>
        /// <returns></returns>
        Task DeleteAsync()
        {
            Task Delete(BlobEntry entry)
            {
                return this.BlobManager.PerformWithRetriesAsync(
                    BlobManager.AsynchronousStorageWriteMaxConcurrency,
                    this.underLease,
                    "BlobBaseClient.DeleteAsync",
                    "DeleteDevice",
                    "",
                    entry.PageBlob.Default.Name,
                    5000,
                    false,
                    async (numAttempts) =>
                    {
                        var client = (numAttempts > 1) ? entry.PageBlob.Default : entry.PageBlob.Aggressive;
                        await client.DeleteAsync(cancellationToken: this.StorageErrorHandler.Token);
                        return 1;
                    });
            }

            return Task.WhenAll(this.blobs.Values.Select(Delete).ToList());
        }

        /// <summary>
        /// <see cref="IDevice.ReadAsync(int, ulong, IntPtr, uint, DeviceIOCompletionCallback, object)">Inherited</see>
        /// </summary>
        public override unsafe void ReadAsync(int segmentId, ulong sourceAddress, IntPtr destinationAddress, uint readLength, DeviceIOCompletionCallback callback, object context)
        {
            long id = Interlocked.Increment(ref AzureStorageDevice.sequenceNumber);

            this.BlobManager?.StorageTracer?.FasterStorageProgress($"StorageOpCalled AzureStorageDevice.ReadAsync id={id} segmentId={segmentId} sourceAddress={sourceAddress} readLength={readLength}");

            this.pendingReadWriteOperations.TryAdd(id, new ReadWriteRequestInfo()
            {
                IsRead = true,
                Callback = callback,
                NumBytes = readLength,
                Context = context,
                TimeStamp = DateTime.UtcNow
            });

            // It is up to the allocator to make sure no reads are issued to segments before they are written
            if (!this.blobs.TryGetValue(segmentId, out BlobEntry blobEntry))
            {
                var nonLoadedBlob = this.pageBlobDirectory.GetPageBlobClient(this.GetSegmentBlobName(segmentId));
                var exception = new InvalidOperationException("Attempt to read a non-loaded segment");
                this.BlobManager?.HandleStorageError(nameof(ReadAsync), exception.Message, nonLoadedBlob.Default?.Name, exception, false, true);
                throw exception;
            }

            this.ReadFromBlobUnsafeAsync(blobEntry.PageBlob, (long)sourceAddress, (long)destinationAddress, readLength, id)
                  .ContinueWith((Task t) =>
                  {
                      if (this.pendingReadWriteOperations.TryRemove(id, out ReadWriteRequestInfo request))
                      {
                          if (t.IsFaulted)
                          {
                              this.BlobManager?.StorageTracer?.FasterStorageProgress($"StorageOpReturned AzureStorageDevice.ReadAsync id={id} (Failure)");
                              request.Callback(uint.MaxValue, request.NumBytes, request.Context);
                          }
                          else
                          {
                              this.BlobManager?.StorageTracer?.FasterStorageProgress($"StorageOpReturned AzureStorageDevice.ReadAsync id={id}");
                              request.Callback(0, request.NumBytes, request.Context);
                          }
                      }
                  }, TaskContinuationOptions.ExecuteSynchronously);
        }

        /// <summary>
        /// <see cref="IDevice.WriteAsync(IntPtr, int, ulong, uint, DeviceIOCompletionCallback, object)">Inherited</see>
        /// </summary>
        public override void WriteAsync(IntPtr sourceAddress, int segmentId, ulong destinationAddress, uint numBytesToWrite, DeviceIOCompletionCallback callback, object context)
        {
            long id = Interlocked.Increment(ref AzureStorageDevice.sequenceNumber);

            this.BlobManager?.StorageTracer?.FasterStorageProgress($"StorageOpCalled AzureStorageDevice.WriteAsync id={id} segmentId={segmentId} destinationAddress={destinationAddress} numBytesToWrite={numBytesToWrite}");

            this.pendingReadWriteOperations.TryAdd(id, new ReadWriteRequestInfo()
            {
                IsRead = false,
                Callback = callback,
                NumBytes = numBytesToWrite,
                Context = context,
                TimeStamp = DateTime.UtcNow
            });

            if (!this.blobs.TryGetValue(segmentId, out BlobEntry blobEntry))
            {
                BlobEntry entry = new(this);
                if (this.blobs.TryAdd(segmentId, entry))
                {
                    var pageBlob = this.pageBlobDirectory.GetPageBlobClient(this.GetSegmentBlobName(segmentId));

                    // If segment size is -1 we use a default
                    var size = this.segmentSize == -1 ? AzureStorageDevice.MAX_PAGEBLOB_SIZE : this.segmentSize;

                    // If no blob exists for the segment, we must first create the segment asynchronouly. (Create call takes ~70 ms by measurement)
                    // After creation is done, we can call write.
                    _ = entry.CreateAsync(size, pageBlob);
                }
                // Otherwise, some other thread beat us to it. Okay to use their blobs.
                blobEntry = this.blobs[segmentId];
            }
            this.TryWriteAsync(blobEntry, sourceAddress, destinationAddress, numBytesToWrite, id);
        }

        //---- The actual read and write accesses to the page blobs

        unsafe Task WritePortionToBlobUnsafeAsync(BlobEntry blobEntry, IntPtr sourceAddress, long destinationAddress, long offset, uint length, long id)
        {
            return this.WritePortionToBlobAsync(new UnmanagedMemoryStream((byte*)sourceAddress + offset, length), blobEntry, sourceAddress, destinationAddress, offset, length, id);
        }

        async Task WritePortionToBlobAsync(UnmanagedMemoryStream stream, BlobEntry blobEntry, IntPtr sourceAddress, long destinationAddress, long offset, uint length, long id)
        {
            using (stream)
            {
                long originalStreamPosition = stream.Position;
                await this.BlobManager.PerformWithRetriesAsync(
                    BlobManager.AsynchronousStorageWriteMaxConcurrency,
                    true,
                    "PageBlobClient.UploadPagesAsync",
                    "WriteToDevice",
                    $"id={id} length={length} destinationAddress={destinationAddress + offset}",
                    blobEntry.PageBlob.Default.Name,
                    1000 + (int)length / 1000,
                    true,
                    async (numAttempts) =>
                    {
                        if (numAttempts > 0)
                        {
                            stream.Seek(originalStreamPosition, SeekOrigin.Begin); // must go back to original position before retry
                        }

                        if (length > 0)
                        {
                            var client = numAttempts > 2 ? blobEntry.PageBlob.Default : blobEntry.PageBlob.Aggressive;

                            var response = await client.UploadPagesAsync(
                                 content: stream,
                                 offset: destinationAddress + offset,
                                 transactionalContentHash: null,
                                 conditions: this.underLease ? new PageBlobRequestConditions() { IfMatch = blobEntry.ETag } : null,
                                 progressHandler: null,
                                 cancellationToken: this.StorageErrorHandler.Token).ConfigureAwait(false);

                            blobEntry.ETag = response.Value.ETag;
                        }

                        return (long)length;
                    },
                    async () =>
                    {
                        var response = await blobEntry.PageBlob.Default.GetPropertiesAsync();
                        blobEntry.ETag = response.Value.ETag;

                    }).ConfigureAwait(false);
            }
        }

        unsafe Task ReadFromBlobUnsafeAsync(BlobUtilsV12.PageBlobClients blob, long sourceAddress, long destinationAddress, uint readLength, long id)
        {
            return this.ReadFromBlobAsync(new UnmanagedMemoryStream((byte*)destinationAddress, readLength, readLength, FileAccess.Write), blob, sourceAddress, readLength, id);
        }

        async Task ReadFromBlobAsync(UnmanagedMemoryStream stream, BlobUtilsV12.PageBlobClients blob, long sourceAddress, uint readLength, long id)
        {
            using (stream)
            {
                long offset = 0;
                while (readLength > 0)
                {
                    var length = Math.Min(readLength, MAX_DOWNLOAD_SIZE);

                    await this.BlobManager.PerformWithRetriesAsync(
                        BlobManager.AsynchronousStorageReadMaxConcurrency,
                        true,
                        "PageBlobClient.DownloadStreamingAsync",
                        "ReadFromDevice",
                        $"id={id} readLength={length} sourceAddress={sourceAddress + offset}",
                        blob.Default.Name,
                        1000 + (int)length / 1000,
                        true,
                        async (numAttempts) =>
                        {
                            if (numAttempts > 0)
                            {
                                stream.Seek(offset, SeekOrigin.Begin); // must go back to original position before retrying
                            }

                            if (length > 0)
                            {
                                var client = (numAttempts > 1 || length == MAX_DOWNLOAD_SIZE) ? blob.Default : blob.Aggressive;

                                var response = await client.DownloadStreamingAsync(
                                    range: new Azure.HttpRange(sourceAddress + offset, length),
                                    conditions: null,
                                    rangeGetContentHash: false,
                                    cancellationToken: this.StorageErrorHandler.Token)
                                    .ConfigureAwait(false);

                                await response.Value.Content.CopyToAsync(stream).ConfigureAwait(false);
                            }

                            if (stream.Position != offset + length)
                            {
                                throw new InvalidDataException($"wrong amount of data received from page blob, expected={length}, actual={stream.Position}");
                            }

                            return length;
                        });

                    readLength -= length;
                    offset += length;
                }
            }
        }

        void TryWriteAsync(BlobEntry blobEntry, IntPtr sourceAddress, ulong destinationAddress, uint numBytesToWrite, long id)
        {
            // If pageBlob is null, it is being created. Attempt to queue the write for the creator to complete after it is done
            if (blobEntry.PageBlob.Default == null
                && blobEntry.TryQueueAction(() => this.WriteToBlobAsync(blobEntry, sourceAddress, destinationAddress, numBytesToWrite, id)))
            {
                return;
            }
            // Otherwise, invoke directly.
            this.WriteToBlobAsync(blobEntry, sourceAddress, destinationAddress, numBytesToWrite, id);
        }

        unsafe void WriteToBlobAsync(BlobEntry blobEntry, IntPtr sourceAddress, ulong destinationAddress, uint numBytesToWrite, long id)
        {
            this.WriteToBlobAsync(blobEntry, sourceAddress, (long)destinationAddress, numBytesToWrite, id)
                .ContinueWith((Task t) =>
                    {
                        if (this.pendingReadWriteOperations.TryRemove(id, out ReadWriteRequestInfo request))
                        {
                            if (t.IsFaulted)
                            {
                                this.BlobManager?.StorageTracer?.FasterStorageProgress($"StorageOpReturned AzureStorageDevice.WriteAsync id={id} (Failure)");
                                request.Callback(uint.MaxValue, request.NumBytes, request.Context);
                            }
                            else
                            {
                                this.BlobManager?.StorageTracer?.FasterStorageProgress($"StorageOpReturned AzureStorageDevice.WriteAsync id={id}");
                                request.Callback(0, request.NumBytes, request.Context);
                            }
                        }

                        if (this.underLease)
                        {
                            this.SingleWriterSemaphore.Release();
                        }

                    }, TaskContinuationOptions.ExecuteSynchronously);
        }

        async Task WriteToBlobAsync(BlobEntry blobEntry, IntPtr sourceAddress, long destinationAddress, uint numBytesToWrite, long id)
        {
            if (this.underLease)
            {
                await this.SingleWriterSemaphore.WaitAsync();
            }

            long offset = 0;
            while (numBytesToWrite > 0)
            {
                var length = Math.Min(numBytesToWrite, MAX_UPLOAD_SIZE);
                await this.WritePortionToBlobUnsafeAsync(blobEntry, sourceAddress, destinationAddress, offset, length, id).ConfigureAwait(false);
                numBytesToWrite -= length;
                offset += length;
            }
        }
    }
}
