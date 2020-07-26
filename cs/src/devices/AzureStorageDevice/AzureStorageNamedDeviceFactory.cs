using FASTER.core;
using Microsoft.Azure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
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
        private CloudBlobContainer containerRef;

        /// <summary>
        /// Create instance of factory for Azure devices
        /// </summary>
        /// <param name="client"></param>
        public AzureStorageNamedDeviceFactory(CloudBlobClient client)
        {
            this.client = client;
        }

        /// <inheritdoc />
        public void Delete(FileDescriptor fileInfo)
        {
            Debug.WriteLine("Delete of Azure blobs not yet supported");
        }

        /// <inheritdoc />
        public IDevice Get(FileDescriptor fileInfo)
        {
            return new AzureStorageDevice(containerRef.GetDirectoryReference(fileInfo.directoryName), fileInfo.fileName);
        }

        /// <inheritdoc />
        public void Initialize(string baseName)
        {
            containerRef = client.GetContainerReference(baseName);
            containerRef.CreateIfNotExists();
        }

        /// <inheritdoc />
        public IEnumerable<FileDescriptor> ListContents(string path)
        {
            foreach (var entry in containerRef.ListBlobs().Where(b => b as CloudBlobDirectory != null)
                .OrderByDescending(f => GetLastModified((CloudBlobDirectory)f)))
            {
                yield return new FileDescriptor
                {
                    directoryName = entry.Uri.LocalPath,
                    fileName = ""
                };
            }

            foreach (var entry in containerRef.ListBlobs().Where(b => b as CloudPageBlob != null)
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
