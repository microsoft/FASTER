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
            using (var ms = new MemoryStream())
            {
                using (var writer = new BinaryWriter(ms))
                {
                    writer.Write(commitMetadata.Length);
                    writer.Write(commitMetadata);
                }
                blob.WritePages(ms, 0);
            }
        }

        /// <inheritdoc />
        public byte[] GetCommitMetadata()
        {
            return BlobUtil.ReadMetadataFile(blob);
        }
    }
}