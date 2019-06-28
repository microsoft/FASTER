// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.IO;
using System.Threading;
using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Blob;

namespace FASTER.core
{
    public class AzurePageBlobDevice : StorageDeviceBase
    {
        private CloudBlobContainer container;
        private readonly ConcurrentDictionary<int, CloudPageBlob> blobs;

        // Azure Page Blobs permit blobs of max size 8 TB
        const long MAX_BLOB_SIZE = (long)(8 * 10e12);

        // I don't believe the FileName attribute on the base class is meaningful here. As no external operation depends on its return value.
        // Therefore, I am using just the connectionString even though it is not a "file name".
        public AzurePageBlobDevice(string connectionString, string containerName, uint sectorSize = 512) : base(connectionString, sectorSize)
        {
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(connectionString);
            CloudBlobClient client = storageAccount.CreateCloudBlobClient();
            container = client.GetContainerReference(containerName);
            // TODO(Tianyu): WTF does this even do
            container.CreateIfNotExists();
            blobs = new ConcurrentDictionary<int, CloudPageBlob>();
        } 

        public override void Close()
        {
            // From what I can tell from the (nonexistent) documentation, no close operation is requried of page blobs
        }

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

        public override unsafe void ReadAsync(int segmentId, ulong sourceAddress, IntPtr destinationAddress, uint readLength, IOCompletionCallback callback, IAsyncResult asyncResult)
        {
            CloudPageBlob pageBlob = GetOrAddPageBlob(segmentId);

            // Even though Azure Page Blob does not make use of Overlapped, we populate one to conform to the callback API
            Overlapped ov = new Overlapped(0, 0, IntPtr.Zero, asyncResult);
            NativeOverlapped* ovNative = ov.UnsafePack(callback, IntPtr.Zero);

            UnmanagedMemoryStream stream = new UnmanagedMemoryStream((byte*)destinationAddress, readLength, readLength, FileAccess.Write);

            // TODO(Tianyu): This implementation seems to swallow exceptions that would otherwise be thrown from the synchronous version of this
            // function. I wasn't able to find any good documentaiton on how exceptions are propagated or handled in this scenario. 
            pageBlob.BeginDownloadRangeToStream(stream, (Int64)sourceAddress, readLength, ar => callback(0, readLength, ovNative), asyncResult);
        }

        public override unsafe void WriteAsync(IntPtr sourceAddress, int segmentId, ulong destinationAddress, uint numBytesToWrite, IOCompletionCallback callback, IAsyncResult asyncResult)
        {
            CloudPageBlob pageBlob = GetOrAddPageBlob(segmentId);

            // Even though Azure Page Blob does not make use of Overlapped, we populate one to conform to the callback API
            Overlapped ov = new Overlapped(0, 0, IntPtr.Zero, asyncResult);
            NativeOverlapped* ovNative = ov.UnsafePack(callback, IntPtr.Zero);
            UnmanagedMemoryStream stream = new UnmanagedMemoryStream((byte*)sourceAddress, numBytesToWrite);
            pageBlob.BeginWritePages(stream, (long)destinationAddress, null, ar => callback(0, numBytesToWrite, ovNative), asyncResult);
        }

        private CloudPageBlob GetOrAddPageBlob(int segmentId)
        {
            return blobs.GetOrAdd(segmentId, id => CreatePageBlob(id));
        }

        private CloudPageBlob CreatePageBlob(int segmentId)
        {
            // TODO(Tianyu): Is this now blocking? How sould this work when multiple apps share the same backing blob store?
            // TODO(Tianyu): Need a better naming scheme?
            CloudPageBlob blob = container.GetPageBlobReference("segment." + segmentId);

            // If segment size is -1, which denotes absence, we request the largest possible blob. This is okay because
            // page blobs are not backed by real pages on creation, and the given size is only a the physical limit of 
            // how large it can grow to.
            var size = segmentSize == -1 ? MAX_BLOB_SIZE : segmentSize;

            // TODO(Tianyu): There is a race hidden here if multiple applications are interacting with the same underlying blob store.
            // How that should be fixed is dependent on our decision on the architecture.
            blob.Create(segmentSize);
            return blob;
        }
    }


}
