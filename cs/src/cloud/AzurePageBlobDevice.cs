// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
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
        // This class bundles a page blob object with a queue and a counter to ensure 
        // 1) BeginCreate is not called more than once
        // 2) No writes are issued before EndCreate
        // The creator of a BlobEntry is responsible for populating the object with an underlying Page Blob. Any subsequent callers
        // either directly write to the created page blob, or queues the write so the creator can clear it after creation is complete.
        // In-progress creation is denoted by a null value on the underlying page blob
        private class BlobEntry
        {
            private CloudPageBlob pageBlob;
            private ConcurrentQueue<Action<CloudPageBlob>> pendingWrites;
            private int waitingCount;

            /// <summary>
            /// Creates a new BlobEntry, does not initialize a page blob. Use <see cref="CreateAsync(long, CloudPageBlob)"/>
            /// for actual creation.
            /// </summary>
            public BlobEntry()
            {
                pageBlob = null;
                pendingWrites = new ConcurrentQueue<Action<CloudPageBlob>>();
                waitingCount = 0;
            }

            /// <summary>
            /// Getter for the underlying <see cref="CloudPageBlob"/>
            /// </summary>
            /// <returns>the underlying <see cref="CloudPageBlob"/>, or null if there is none</returns>
            public CloudPageBlob GetPageBlob()
            {
                return pageBlob;
            }

            /// <summary>
            /// Asynchronously invoke create on the given pageBlob.
            /// </summary>
            /// <param name="size">maximum size of the blob</param>
            /// <param name="pageBlob">The page blob to create</param>
            public void CreateAsync(long size, CloudPageBlob pageBlob)
            {
                Debug.Assert(waitingCount == 0, "Create should be called on blobs that don't already exist and exactly once");
                // Asynchronously create the blob
                pageBlob.BeginCreate(size, ar =>
                {
                    try
                    {
                        pageBlob.EndCreate(ar);
                    }
                    catch (Exception e)
                    {
                        // TODO(Tianyu): Can't really do better without knowing error behavior
                        Trace.TraceError(e.Message);
                    }
                    // At this point the blob is fully created. After this line all consequent writers will write immediately. We just
                    // need to clear the queue of pending writers.
                    this.pageBlob = pageBlob;
                    // Take a snapshot of the current waiting count. Exactly this many actions will be cleared.
                    // Swapping in -1 will inform any stragglers that we are not taking their actions and prompt them to retry (and call write directly)
                    int waitingCountSnapshot = Interlocked.Exchange(ref waitingCount, -1);
                    Action<CloudPageBlob> action;
                    // Clear actions
                    for (int i = 0; i < waitingCountSnapshot; i++)
                    {
                        // inserts into the queue may lag behind the creation thread. We have to wait until that happens.
                        // This is so rare, that we are probably okay with a busy wait.
                        while (!pendingWrites.TryDequeue(out action)) { }
                        action(pageBlob);
                    }
                    // Mark for deallocation for the GC
                    pendingWrites = null;
                }, null);
            }

            /// <summary>
            /// Attempts to enqueue an action to be invoked by the creator after creation is done. Should only be invoked when
            /// creation is in-flight. This call is allowed to fail (and return false) if concurrently the creation is complete.
            /// The caller should call the write action directly instead of queueing in this case.
            /// </summary>
            /// <param name="writeAction">The write action to perform</param>
            /// <returns>Whether the action was successfully enqueued</returns>
            public bool TryQueueAction(Action<CloudPageBlob> writeAction)
            {
                int currentCount;
                do
                {
                    currentCount = waitingCount;
                    // If current count became -1, creation is complete. New queue entries will not be processed and we must call the action ourselves.
                    if (currentCount == -1) return false;
                } while (Interlocked.CompareExchange(ref waitingCount, currentCount + 1, currentCount) != currentCount);
                // Enqueue last. The creation thread is obliged to wait until it has processed waitingCount many actions.
                // It is extremely unlikely that we will get scheduled out here anyways.
                pendingWrites.Enqueue(writeAction);
                return true;
            }
        }
        private CloudBlobContainer container;
        private readonly ConcurrentDictionary<int, BlobEntry> blobs;
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
            blobs = new ConcurrentDictionary<int, BlobEntry>();
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
                    entry.Value.GetPageBlob().Delete();
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
                if (blobs.TryRemove(i, out BlobEntry blob))
                {
                    blob.GetPageBlob().Delete();
                }
            }
        }

        /// <summary>
        /// <see cref="IDevice.ReadAsync(int, ulong, IntPtr, uint, IOCompletionCallback, IAsyncResult)">Inherited</see>
        /// </summary>
        public override unsafe void ReadAsync(int segmentId, ulong sourceAddress, IntPtr destinationAddress, uint readLength, IOCompletionCallback callback, IAsyncResult asyncResult)
        {
            // It is up to the allocator to make sure no reads are issued to segments before they are written
            if (!blobs.TryGetValue(segmentId, out BlobEntry blobEntry)) throw new InvalidOperationException("Attempting to read non-existent segments");

            // Even though Azure Page Blob does not make use of Overlapped, we populate one to conform to the callback API
            Overlapped ov = new Overlapped(0, 0, IntPtr.Zero, asyncResult);
            NativeOverlapped* ovNative = ov.UnsafePack(callback, IntPtr.Zero);

            UnmanagedMemoryStream stream = new UnmanagedMemoryStream((byte*)destinationAddress, readLength, readLength, FileAccess.Write);
            CloudPageBlob pageBlob = blobEntry.GetPageBlob();
            pageBlob.BeginDownloadRangeToStream(stream, (Int64)sourceAddress, readLength, ar => {
                try
                {
                    pageBlob.EndDownloadRangeToStream(ar);
                }
                // I don't think I can be more specific in catch here because no documentation on exception behavior is provided
                catch (Exception e)
                {
                    Trace.TraceError(e.Message);
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
            if (!blobs.TryGetValue(segmentId, out BlobEntry blobEntry))
            {
                BlobEntry entry = new BlobEntry();
                if (blobs.TryAdd(segmentId, entry))
                {
                    CloudPageBlob pageBlob = container.GetPageBlobReference(blobName + segmentId);
                    // If segment size is -1, which denotes absence, we request the largest possible blob. This is okay because
                    // page blobs are not backed by real pages on creation, and the given size is only a the physical limit of 
                    // how large it can grow to.
                    var size = segmentSize == -1 ? MAX_BLOB_SIZE : segmentSize;
                    // If no blob exists for the segment, we must first create the segment asynchronouly. (Create call takes ~70 ms by measurement)
                    // After creation is done, we can call write.
                    entry.CreateAsync(size, pageBlob);
                }
                // Otherwise, some other thread beat us to it. Okay to use their blobs.
                blobEntry = blobs[segmentId];
            }
            TryWriteAsync(blobEntry, sourceAddress, destinationAddress, numBytesToWrite, callback, asyncResult);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void TryWriteAsync(BlobEntry blobEntry, IntPtr sourceAddress, ulong destinationAddress, uint numBytesToWrite, IOCompletionCallback callback, IAsyncResult asyncResult)
        {
            CloudPageBlob pageBlob = blobEntry.GetPageBlob();
            // If pageBlob is null, it is being created. Attempt to queue the write for the creator to complete after it is done
            if (pageBlob == null
                && blobEntry.TryQueueAction(p => WriteToBlobAsync(p, sourceAddress, destinationAddress, numBytesToWrite, callback, asyncResult))) return;

            // Otherwise, invoke directly.
            WriteToBlobAsync(pageBlob, sourceAddress, destinationAddress, numBytesToWrite, callback, asyncResult);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
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
                    Trace.TraceError(e.Message);
                    // Is there any documentation on the meaning of error codes here? The handler suggests that any non-zero value is an error
                    // but does not distinguish between them.
                    callback(1, numBytesToWrite, ovNative);
                }
                callback(0, numBytesToWrite, ovNative);
            }, asyncResult);
        }
    }
}
