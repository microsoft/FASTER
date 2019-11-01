using System;
using Microsoft.Azure.Storage.Blob;

namespace FASTER.devices
{
    /// <summary>
    /// Centralized utility class for orchestrating blob naming of checkpointing in the cloud
    /// </summary>
    public static class AzureStorageCheckpointBlobNamingScheme
    {
        private const string IndexBaseName = "index-checkpoints";
        private const string IndexMetadataName = "info";
        private const string HashTableName = "ht";
        private const string OverflowBucketsName = "ofb";
        private const string SnapshotName = "snapshot";
        private const string CprBaseName = "cpr-checkpoints";
        private const string CprMetadataName = "info";
        
        /// <summary>
        /// Get name of the index checkpoint metadata file
        /// </summary>
        /// <param name="token">Guid of the checkpoint</param>
        /// <returns>name of the index checkpoint metadata file</returns>
        public static string GetIndexCheckpointMetadataBlobName(Guid token)
        {
            return IndexBaseName + "-" + token + "-" + IndexMetadataName;
        }
        
        /// <summary>
        /// Get name of the index checkpoint completion file. The completion file is an empty file, the
        /// existence of which indicates that an entire checkpoint is complete.
        /// </summary>
        /// <param name="token">Guid of the checkpoint</param>
        /// <returns>name of the index checkpoint completion file</returns>
        public static string GetIndexCheckpointCompletionBlobName(Guid token)
        {
            return IndexBaseName + "-" + token + "-completed";
        }

        /// <summary>
        /// Get name of the primary hash table file
        /// </summary>
        /// <param name="token">Guid of the checkpoint</param>
        /// <returns>name of the primary hash table file</returns>
        public static string GetPrimaryHashTableBlobName(Guid token)
        {
            return IndexBaseName + "-" + token + "-" + HashTableName;
        }
        
        
        /// <summary>
        /// Get name of the overflow buckets file
        /// </summary>
        /// <param name="token">Guid of the checkpoint</param>
        /// <returns>name of the overflow buckets file</returns>
        public static string GetOverflowBucketsBlobName(Guid token)
        {
            return IndexBaseName + "-" + token + "-" + OverflowBucketsName;
        }

        /// <summary>
        /// Get name of the hybrid log checkpoint metadata file
        /// </summary>
        /// <param name="token">Guid of the checkpoint</param>
        /// <returns>name of the hybrid log checkpoint metadata file</returns>
        public static string GetHybridLogCheckpointMetadataBlobName(Guid token)
        {
            return CprBaseName + "-" + token + "-" + CprMetadataName;
        }

        /// <summary>
        /// Get name of the hybrid log checkpoint completion file. The completion file is an
        /// empty file, the existence of which indicates that an entire checkpoint is complete.
        /// </summary>
        /// <param name="token">Guid of the checkpoint</param>
        /// <returns>name of the hybrid log checkpoint completion file</returns>
        public static string GetHybridLogCheckpointCompletionBlobName(Guid token)
        {
            return CprBaseName + "-" + token + "-completed";
        }


        /// <summary>
        /// Get name of the hybrid log checkpoint context file
        /// </summary>
        /// <param name="checkpointToken">guid of the checkpoint</param>
        /// <param name="sessionToken">guid of the session </param>
        /// <returns>name of the hybrid log checkpoint context file</returns>
        public static string GetHybridLogCheckpointContextBlobName(Guid checkpointToken, Guid sessionToken)
        {
            return CprBaseName + "-" + checkpointToken + "-" + sessionToken;
        }

        /// <summary>
        /// Get name of the log snapshot file
        /// </summary>
        /// <param name="token">Guid of the checkpoint</param>
        /// <returns>name of the log snapshot file</returns>
        public static string GetLogSnapshotBlobName(Guid token)
        {
            return CprBaseName + "-" + token + "-" + SnapshotName;
        }

        /// <summary>
        /// Get name of the object log snapshot file
        /// </summary>
        /// <param name="token">Guid of the checkpoint</param>
        /// <returns>name of the object log snapshot file</returns>
        public static string GetObjectLogSnapshotBlobName(Guid token)
        {
            return CprBaseName + "-" + token + "-" + SnapshotName + "-obj";
        }

        /// <summary>
        /// Test whether a name is one of the index checkpoint metadata according to the naming scheme
        /// </summary>
        /// <param name="name">name of the file to check</param>
        /// <returns>whether a name is one of the index checkpoint metadata according to the naming scheme</returns>
        public static bool IsIndexCheckpointMetadataBlob(string name)
        {
            return name.StartsWith(IndexBaseName) && name.EndsWith(IndexMetadataName);
        }

        /// <summary>
        /// Test whether a name is one of the hybrid log checkpoint metadata according to the naming scheme
        /// </summary>
        /// <param name="name">name of the file to check</param>
        /// <returns>
        /// whether a name is one of the hybrid log checkpoint metadata according to the naming scheme
        /// </returns>
        public static bool IsHybridLogCheckpointMetadataBlob(string name)
        {
            return name.StartsWith(CprBaseName) && name.EndsWith(CprMetadataName);
        }

        /// <summary>
        /// Extract the Guid from the name of a checkpoint metadata file.
        /// </summary>
        /// <param name="metadataName">name of the checkpoint metadata file</param>
        /// <returns>Guid of the checkpoint</returns>
        public static Guid ExtractGuid(string metadataName)
        {
            return Guid.Parse(metadataName.Split('-')[1]);
        }
    }
}