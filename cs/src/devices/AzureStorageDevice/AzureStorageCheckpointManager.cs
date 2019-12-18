using System;
using System.Collections.Generic;
using System.ComponentModel.Design;
using System.Diagnostics;
using System.IO;
using System.Linq;
using FASTER.core;
using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Blob;

namespace FASTER.devices
{
    public class AzureStorageCheckpointManager : ICheckpointManager
    {
        private CloudBlobContainer container;

        public AzureStorageCheckpointManager(string connectionString, string containerName)
        {
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(connectionString);
            CloudBlobClient client = storageAccount.CreateCloudBlobClient();
            container = client.GetContainerReference(containerName);
            container.CreateIfNotExists();
        }

        public void InitializeIndexCheckpoint(Guid indexToken)
        {
            // Nothing to do
        }

        public void InitializeLogCheckpoint(Guid logToken)
        {
            // Nothing to do
        }

        public void CommitIndexCheckpoint(Guid indexToken, byte[] commitMetadata)
        {
            CloudPageBlob metadataBlob =
                BlobUtil.CreateCloudPageBlob(container,
                    AzureStorageCheckpointBlobNamingScheme.GetIndexCheckpointMetadataBlobName(indexToken));
            using (var ms = new MemoryStream())
            {
                using (var writer = new BinaryWriter(ms))
                {
                    // TODO(Tianyu): Endianness a concern?
                    writer.Write(commitMetadata.Length);
                    writer.Write(commitMetadata);
                }
                metadataBlob.WritePages(ms, 0);
            }

            // Use the existence of an empty blob as indication that checkpoint has completed.
            // TODO(Tianyu): Is this efficient?
            BlobUtil.CreateCloudPageBlob(container,
                AzureStorageCheckpointBlobNamingScheme.GetIndexCheckpointCompletionBlobName(indexToken));
        }

        public void CommitLogCheckpoint(Guid logToken, byte[] commitMetadata)
        {
            CloudPageBlob metadataBlob =
                BlobUtil.CreateCloudPageBlob(container,
                    AzureStorageCheckpointBlobNamingScheme.GetHybridLogCheckpointMetadataBlobName(logToken));
            using (var ms = new MemoryStream())
            {
                using (var writer = new BinaryWriter(ms))
                {
                    // TODO(Tianyu): Endianness a concern?
                    writer.Write(commitMetadata.Length);
                    writer.Write(commitMetadata);
                }
                metadataBlob.WritePages(ms, 0);
            }

            // Use the existence of an empty blob as indication that checkpoint has completed.
            // TODO(Tianyu): Is this efficient?
            BlobUtil.CreateCloudPageBlob(container,
                AzureStorageCheckpointBlobNamingScheme.GetHybridLogCheckpointCompletionBlobName(logToken));
        }

        public byte[] GetIndexCommitMetadata(Guid indexToken)
        {
            if (container.GetPageBlobReference(
                AzureStorageCheckpointBlobNamingScheme.GetIndexCheckpointCompletionBlobName(indexToken)).Exists())
                return null;
            return BlobUtil.ReadMetadataFile(container.GetPageBlobReference(
                AzureStorageCheckpointBlobNamingScheme.GetIndexCheckpointMetadataBlobName(indexToken)));
        }

        public byte[] GetLogCommitMetadata(Guid logToken)
        {
            if (container.GetPageBlobReference(
                AzureStorageCheckpointBlobNamingScheme.GetHybridLogCheckpointCompletionBlobName(logToken)).Exists())
                return null;
            return BlobUtil.ReadMetadataFile(container.GetPageBlobReference(
                AzureStorageCheckpointBlobNamingScheme.GetHybridLogCheckpointMetadataBlobName(logToken)));
        }

        public IDevice GetIndexDevice(Guid indexToken)
        {
            // TODO(Tianyu): Specify capacity?
            return new AzureStorageDevice(container,
                AzureStorageCheckpointBlobNamingScheme.GetIndexCheckpointMetadataBlobName(indexToken));
        }

        public IDevice GetSnapshotLogDevice(Guid token)
        {
            // TODO(Tianyu): Specify capacity?
            return new AzureStorageDevice(container,
                AzureStorageCheckpointBlobNamingScheme.GetLogSnapshotBlobName(token));
        }

        public IDevice GetSnapshotObjectLogDevice(Guid token)
        {
            // TODO(Tianyu): Specify capacity?
            return new AzureStorageDevice(container,
                AzureStorageCheckpointBlobNamingScheme.GetObjectLogSnapshotBlobName(token));
        }

        public bool GetLatestCheckpoint(out Guid indexToken, out Guid logToken)
        {
            indexToken = default;
            logToken = default;
            // Scan through all blobs to remove incompleted checkpoints and obtain a list of all
            // completed checkpoints
            IList<Guid> indexCheckpointMetadataBlobs = new List<Guid>(),
                          hybridLogCheckpointMetadataBlobs = new List<Guid>();
            foreach (IListBlobItem blob in container.ListBlobs())
            {
                string name = blob.ToString();
                // TODO(Tianyu): Is there going to be an issue with concurrent modification of container while traversing?
                if (AzureStorageCheckpointBlobNamingScheme.IsIndexCheckpointMetadataBlob(name))
                {
                    Guid guid = AzureStorageCheckpointBlobNamingScheme.ExtractGuid(name);
                    if (!IsIndexCheckpointCompleted(guid))
                    {
                        EraseIndexCheckpoint(guid);
                    }
                    else
                    {
                        indexCheckpointMetadataBlobs.Add(guid);
                    }
                }
                else if (AzureStorageCheckpointBlobNamingScheme.IsHybridLogCheckpointMetadataBlob(name))
                {
                    Guid guid = AzureStorageCheckpointBlobNamingScheme.ExtractGuid(name);
                    if (!IsHybridLogCheckpointCompleted(guid))
                    {
                        EraseHybridLogCheckpoint(guid);
                    }
                    else
                    {
                        hybridLogCheckpointMetadataBlobs.Add(guid);
                    }
                }
            }

            // Find latest index checkpoint
            if (indexCheckpointMetadataBlobs.Count == 0) return false;
            indexToken = indexCheckpointMetadataBlobs.OrderByDescending(IndexCheckpointCompletionTime).First();
            
            // Find latest hlog checkpoint 
            if (hybridLogCheckpointMetadataBlobs.Count == 0) return false;
            logToken = hybridLogCheckpointMetadataBlobs.OrderByDescending(IndexCheckpointCompletionTime).First();
            
            return true;
        }

        private bool IsIndexCheckpointCompleted(Guid guid)
        {
            return container.GetPageBlobReference(
                AzureStorageCheckpointBlobNamingScheme.GetIndexCheckpointCompletionBlobName(guid)).Exists();
        }

        private bool IsHybridLogCheckpointCompleted(Guid guid)
        {
            return container.GetPageBlobReference(
                AzureStorageCheckpointBlobNamingScheme.GetHybridLogCheckpointCompletionBlobName(guid)).Exists();
        }

        private void EraseIndexCheckpoint(Guid guid)
        {
            container.GetPageBlobReference(
                AzureStorageCheckpointBlobNamingScheme.GetIndexCheckpointMetadataBlobName(guid)).DeleteIfExists();
            container.GetPageBlobReference(
                AzureStorageCheckpointBlobNamingScheme.GetIndexCheckpointCompletionBlobName(guid)).DeleteIfExists();
            container.GetPageBlobReference(
                AzureStorageCheckpointBlobNamingScheme.GetPrimaryHashTableBlobName(guid)).DeleteIfExists();
            // TODO(Tianyu): This never seems used?
//            container.GetPageBlobReference(
//                AzureStorageCheckpointBlobNamingScheme.GetOverflowBucketsBlobName(guid)).DeleteIfExists();
        }

        private void EraseHybridLogCheckpoint(Guid guid)
        {
            container.GetPageBlobReference(
                AzureStorageCheckpointBlobNamingScheme.GetHybridLogCheckpointMetadataBlobName(guid)).DeleteIfExists();
            // TODO(Tianyu): This never seem used?
//            container.GetPageBlobReference(
//                AzureStorageCheckpointBlobNamingScheme.GetHybridLogCheckpointContextBlobName(guid)).DeleteIfExists();
            container.GetPageBlobReference(
                AzureStorageCheckpointBlobNamingScheme.GetHybridLogCheckpointCompletionBlobName(guid)).DeleteIfExists();
            container.GetPageBlobReference(
                AzureStorageCheckpointBlobNamingScheme.GetLogSnapshotBlobName(guid)).DeleteIfExists();
            container.GetPageBlobReference(
                AzureStorageCheckpointBlobNamingScheme.GetObjectLogSnapshotBlobName(guid)).DeleteIfExists();
        }

        private DateTimeOffset IndexCheckpointCompletionTime(Guid guid)
        {
            // TODO(Tianyu): When will the operation return null? Is error propagation the correct behavior?
            return (DateTimeOffset) container
                .GetPageBlobReference(AzureStorageCheckpointBlobNamingScheme.GetIndexCheckpointCompletionBlobName(guid))
                .Properties.LastModified;
        }

        private DateTimeOffset HybridLogCheckpointCompletionTime(Guid guid)
        {
            // TODO(Tianyu): When will the operation return null? Is error propagation the correct behavior?
            return (DateTimeOffset) container
                .GetPageBlobReference(AzureStorageCheckpointBlobNamingScheme.GetHybridLogCheckpointCompletionBlobName(guid))
                .Properties.LastModified;
        }
    }
}