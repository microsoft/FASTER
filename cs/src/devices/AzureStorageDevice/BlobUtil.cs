using System;
using System.Diagnostics;
using System.Linq;
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
        public const int PAGE_BLOB_SECTOR_SIZE = 512;

        private const int OneShotReadMaxFileSize = 1024;
        
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
        
        /// <summary>
        /// Read the metadata out of a blob file (stored in the system as length + bytes). It is assumed that the blob
        /// exists. 
        /// </summary>
        /// <param name="blob">source of the metadata</param>
        /// <returns>metadata bytes</returns>
        public static byte[] ReadMetadataFile(CloudPageBlob blob)
        {
            // Optimization for small metadata file size: we read some maximum length bytes, and if the file size is
            // smaller than that, we do not have to issue a second read request after reading the length of metadata.
            byte[] firstRead = new byte[OneShotReadMaxFileSize];
            var downloaded = blob.DownloadRangeToByteArray(firstRead, 0, 0, OneShotReadMaxFileSize);
            Debug.Assert(downloaded == OneShotReadMaxFileSize);
            
            
            int length = BitConverter.ToInt32(firstRead, 0);
            // If file length is less than what we have read, we can return from memory immediately
            if (length < OneShotReadMaxFileSize - sizeof(int))
            {
                // This still copies the array. But since the cost will be dominated by read request to Azure, I am
                // guessing it does not matter
                return firstRead.Skip(sizeof(int)).Take(length).ToArray();
            }
            
            // Otherwise, copy over what we have read and read the remaining bytes.
            byte[] result = new byte[length];
            int numBytesRead = OneShotReadMaxFileSize - sizeof(int);
            Array.Copy(firstRead, sizeof(int), result, 0, numBytesRead);
            downloaded = blob.DownloadRangeToByteArray(result, numBytesRead, OneShotReadMaxFileSize, length - numBytesRead);
            Debug.Assert(downloaded == length - numBytesRead, "Underfilled read buffer");
            return result;
        }

    }
}