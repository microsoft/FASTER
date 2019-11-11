using System;
using System.Diagnostics;
using System.IO;
using FASTER.core;
using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Blob;

namespace FASTER.devices
{
    /// <summary>
    /// A CommitManager backed by Azure cloud page blobs
    /// </summary>
    public class AzureStorageCommitManager : ILogCommitManager
    {
        private CloudPageBlob blob;
        
        /// <summary>
        /// Construct a new AzureStorageCommitManager backed by the given page blob
        /// </summary>
        /// <param name="blob">
        /// backing page blob for the commit manager. It is assumed that the blob has already been created
        /// </param>
        public AzureStorageCommitManager(CloudPageBlob blob)
        {
            this.blob = blob;
        }
        
        /// <summary>
        /// Construct a new AzureStorageCommitManager backed by the given page blob
        /// </summary>
        /// <param name="connectionString"> connection string for the storage account </param>
        /// <param name="containerName"> name of the container the blob is in </param>
        /// <param name="commitFileName">
        /// name of the blob. It is assumed that the blob has already been created
        /// </param>
        public AzureStorageCommitManager(string connectionString, string containerName, string commitFileName)
        {
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(connectionString);
            CloudBlobClient client = storageAccount.CreateCloudBlobClient();
            CloudBlobContainer container = client.GetContainerReference(containerName);
            container.CreateIfNotExists();
            blob = container.GetPageBlobReference(commitFileName);
        }

        /// <inheritdoc />
        public void Commit(long beginAddress, long untilAddress, byte[] commitMetadata)
        {
            int writeSize = sizeof(int) + commitMetadata.Length;
            // Writes to PageBlob must be aligned at 512 boundaries, we need to therefore pad up to the closest
            // multiple of 512 for the write buffer size.
            int mask = BlobUtil.PAGE_BLOB_SECTOR_SIZE - 1;
            byte[] alignedByteChunk = new byte[(writeSize + mask) & ~mask]; 
            
            Array.Copy(BitConverter.GetBytes(commitMetadata.Length), alignedByteChunk, sizeof(int));
            Array.Copy(commitMetadata, 0, alignedByteChunk, sizeof(int), commitMetadata.Length);
            
            // TODO(Tianyu): We assume this operation is atomic I guess?
            blob.WritePages(new MemoryStream(alignedByteChunk), 0);
        }

        /// <inheritdoc />
        public byte[] GetCommitMetadata()
        {
            return BlobUtil.ReadMetadataFile(blob);
        }
    }
}