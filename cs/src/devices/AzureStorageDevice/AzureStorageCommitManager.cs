using System;
using System.Diagnostics;
using System.IO;
using FASTER.core;
using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Blob;

namespace FASTER.devices
{
    public class AzureStorageCommitManager : ILogCommitManager
    {
        private CloudPageBlob blob;

        // TODO(Tianyu): We have several instances of this, need to put this into a global constant file ore something..
        private const long MAX_BLOB_SIZE = (long) (2 * 10e8);
        
        public AzureStorageCommitManager(CloudPageBlob blob)
        {
            this.blob = blob;
        }
        
        public AzureStorageCommitManager(string connectionString, string containerName, string commitFileName)
        {
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(connectionString);
            CloudBlobClient client = storageAccount.CreateCloudBlobClient();
            CloudBlobContainer container = client.GetContainerReference(containerName);
            container.CreateIfNotExists();
            blob = container.GetPageBlobReference(commitFileName);
            blob.Create(MAX_BLOB_SIZE);
        }

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

        // TODO(Tianyu): This is duplicate from CheckpointManager, should reuse when possible
        public byte[] GetCommitMetadata()
        {
            return BlobUtil.ReadMetadataFile(blob);
        }
    }
}