using System;
using Microsoft.Azure.Storage.Blob;

namespace FASTER.devices
{
    public static class AzureStorageCheckpointBlobNamingScheme
    {
        private const string index_base_name = "index-checkpoints";
        private const string index_metadata_name = "info";
        private const string hash_table_name = "ht";
        private const string overflow_buckets_name = "ofb";
        private const string snapshot_name = "snapshot";
        private const string cpr_base_name = "cpr-checkpoints";
        private const string cpr_metadata_name = "info";
        
        public static string GetIndexCheckpointMetadataBlobName(Guid token)
        {
            return index_base_name + "-" + token + "-" + index_metadata_name;
        }
        
        public static string GetIndexCheckpointCompletionBlobName(Guid token)
        {
            return index_base_name + "-" + token + "-completed";
        }

        public static string GetPrimaryHashTableBlobName(Guid token)
        {
            return index_base_name + "-" + token + "-" + hash_table_name;
        }
        

        public static string GetOverflowBucketsBlobName(Guid token)
        {
            return index_base_name + "-" + token + "-" + overflow_buckets_name;
        }

        public static string GetHybridLogCheckpointMetadataBlobName(Guid token)
        {
            return cpr_base_name + "-" + token + "-" + cpr_metadata_name;
        }

        public static string GetHybridLogCheckpointCompletionBlobName(Guid token)
        {
            return cpr_base_name + "-" + token + "-completed";
        }

        public static string GetHybridLogCheckpointContextBlobName(Guid checkpointToken, Guid sessionToken)
        {
            return cpr_base_name + "-" + checkpointToken + "-" + sessionToken;
        }

        public static string GetLogSnapshotBlobName(Guid token)
        {
            return cpr_base_name + "-" + token + "-" + snapshot_name;
        }

        public static string GetObjectLogSnapshotBlobName(Guid token)
        {
            return cpr_base_name + "-" + token + "-" + snapshot_name + "-obj";
        }

        public static bool IsIndexCheckpointMetadataBlob(string name)
        {
            return name.StartsWith(index_base_name) && name.EndsWith(index_metadata_name);
        }

        public static bool IsHybridLogCheckpointMetadataBlob(string name)
        {
            return name.StartsWith(cpr_base_name) && name.EndsWith(cpr_metadata_name);
        }

        public static Guid ExtractGuid(string metadataName)
        {
            return Guid.Parse(metadataName.Split('-')[1]);
        }
    }
}