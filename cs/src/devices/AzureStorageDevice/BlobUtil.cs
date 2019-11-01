using System;
using System.Diagnostics;
using Microsoft.Azure.Storage.Blob;

namespace FASTER.devices
{
    /// <summary>
    /// Collection of utility functions used by classes of this package
    /// </summary>
    public static class BlobUtil
    {
        /// <summary>
        /// The maximum blob size to use for new page blobs. Page Blobs permit blobs of max size 8 TB,
        /// but the emulator permits only 2 GB
        /// </summary>
        public const long MAX_BLOB_SIZE = (long) (2 * 10e8);
        
        /// <summary>
        /// Azure Page Blobs have a fixed sector size of 512 bytes.
        /// </summary>
        public const uint PAGE_BLOB_SECTOR_SIZE = 512;
        
        /// <summary>
        /// Create a cloud page blob with the given name in the given container
        /// </summary>
        /// <param name="container">reference to the target cloud container</param>
        /// <param name="name">name of the intended page blob</param>
        /// <returns>reference to a created blob</returns>
        public static CloudPageBlob CreateCloudPageBlob(CloudBlobContainer container, string name)
        {
            CloudPageBlob blob = container.GetPageBlobReference(name);
            // TODO(Tianyu): Will there ever be a race on this?
            blob.Create(MAX_BLOB_SIZE);
            return blob;
        }

        private static int ReadInt32(CloudPageBlob blob, long offset)
        {
            byte[] result = new byte[sizeof(Int32)];
            var read = blob.DownloadRangeToByteArray(result, 0, offset, sizeof(Int32));
            // TODO(Tianyu): Can read bytes ever be smaller than requested like POSIX? There is certainly
            // no documentation about the behavior...
            Debug.Assert(read == sizeof(Int32), "Underfilled read buffer");
            return BitConverter.ToInt32(result, 0);
        }

        /// <summary>
        /// Read the metadata out of a blob file (stored in the system as length + bytes)
        /// </summary>
        /// <param name="blob">source of the metadata</param>
        /// <returns>metadata bytes</returns>
        public static byte[] ReadMetadataFile(CloudPageBlob blob)
        {
            // Assuming that the page blob already exists
            int length = ReadInt32(blob, 0);
            byte[] result = new byte[length];
            var downloaded = blob.DownloadRangeToByteArray(result, 0, sizeof(Int32), length);
            Debug.Assert(downloaded == length, "Underfilled read buffer");
            return result;
        }

    }
}