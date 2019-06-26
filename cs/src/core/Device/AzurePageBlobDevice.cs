// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.IO;
using System.Threading;
using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Blob;

namespace FASTER.core.Device
{
    public class AzurePageBlobDevice : StorageDeviceBase
    {
        private CloudBlobContainer container;
        private readonly ConcurrentDictionary<int, CloudPageBlob> blobs;
        // I don't believe the FileName attribute on the base class is meaningful here. As no external operation depends on its return value.
        // Therefore, I am using just the connectionString even though it is not a "file name".
        public AzurePageBlobDevice(string connectionString, string containerName, uint sectorSize = 512) : base(connectionString, sectorSize)
        {
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(connectionString);
            CloudBlobClient client = storageAccount.CreateCloudBlobClient();
            container = client.GetContainerReference(containerName);
            // TODO(Tianyu): WTF does this even do
            container.CreateIfNotExists();
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

            UnmanagedMemoryStream stream = new UnmanagedMemoryStream((byte*)destinationAddress, readLength);

            // What do with the return value, or do I just not care?
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
            // TODO(Tianyu): There does not seem to be an equivalent concept to preallocating in page blobs
            // TODO(Tianyu): Also, why the hell is there no CreateIfExists on this thing? This is race-prone if multiple apps are sharing access to an instance
            // Maybe I should fix this using leases, but the lease API is just absolute shit and has no documentation. 
            blob.Create(SectorSize);
            return blob;
        }
    }


}
