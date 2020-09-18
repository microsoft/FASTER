using FASTER.core;
using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace FASTER.devices
{
    /// <summary>
    /// Device factory for Azure
    /// </summary>
    public class AzureStorageNamedDeviceFactory : INamedDeviceFactory
    {
        private readonly CloudBlobClient client;
        private CloudBlobDirectory baseRef;

        /// <summary>
        /// Create instance of factory for Azure devices
        /// </summary>
        /// <param name="client"></param>
        public AzureStorageNamedDeviceFactory(CloudBlobClient client)
        {
            this.client = client;
        }

        /// <summary>
        /// Create instance of factory for Azure devices
        /// </summary>
        /// <param name="connectionString"></param>
        public AzureStorageNamedDeviceFactory(string connectionString)
            : this(CloudStorageAccount.Parse(connectionString).CreateCloudBlobClient())
        {
        }

        /// <inheritdoc />
        public void Delete(FileDescriptor fileInfo)
        {
            if (fileInfo.fileName != null)
            {
                var dir = fileInfo.directoryName == "" ? baseRef : baseRef.GetDirectoryReference(fileInfo.directoryName);

                // We only delete shard 0
                dir.GetBlobReference(fileInfo.fileName + ".0").DeleteIfExists();
            }
            else
            {
                var dir = fileInfo.directoryName == "" ? baseRef : baseRef.GetDirectoryReference(fileInfo.directoryName);
                foreach (IListBlobItem blob in dir.ListBlobs(true))
                {
                    if (blob.GetType() == typeof(CloudBlob) || blob.GetType().BaseType == typeof(CloudBlob))
                    {
                        ((CloudBlob)blob).DeleteIfExists();
                    }
                }
            }
        }

        /// <inheritdoc />
        public IDevice Get(FileDescriptor fileInfo)
        {
            return new AzureStorageDevice(baseRef.GetDirectoryReference(fileInfo.directoryName), fileInfo.fileName);
        }

        /// <inheritdoc />
        public void Initialize(string baseName)
        {
            var path = baseName.Split('/');
            var containerName = path[0];
            var dirName = string.Join("/", path.Skip(1));

            var containerRef = client.GetContainerReference(containerName);
            containerRef.CreateIfNotExists();
            baseRef = containerRef.GetDirectoryReference(dirName);
        }

        /// <inheritdoc />
        public IEnumerable<FileDescriptor> ListContents(string path)
        {
            foreach (var entry in baseRef.GetDirectoryReference(path).ListBlobs().Where(b => b as CloudBlobDirectory != null)
                .OrderByDescending(f => GetLastModified((CloudBlobDirectory)f)))
            {
                yield return new FileDescriptor
                {
                    directoryName = entry.Uri.LocalPath,
                    fileName = ""
                };
            }

            foreach (var entry in baseRef.ListBlobs().Where(b => b as CloudPageBlob != null)
                .OrderByDescending(f => ((CloudPageBlob)f).Properties.LastModified))
            {
                yield return new FileDescriptor
                {
                    directoryName = "",
                    fileName = ((CloudPageBlob)entry).Name
                };
            }
        }

        private DateTimeOffset? GetLastModified(CloudBlobDirectory cloudBlobDirectory)
        {
            return cloudBlobDirectory.ListBlobs().Select(e => ((CloudPageBlob)e).Properties.LastModified).OrderByDescending(e => e).First();
        }
    }
}
