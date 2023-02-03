using FASTER.core;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace DurableTask.Netherite.Faster
{
    /// <summary>
    /// Device factory for Azure
    /// </summary>
    public class AzureStorageNamedDeviceFactory : INamedDeviceFactory
    {
        readonly BlobUtilsV12.ServiceClients pageBlobAccount;
        BlobUtilsV12.ContainerClients pageBlobContainer;
        BlobUtilsV12.BlobDirectory pageBlobDirectory;

        /// <summary>
        /// Create instance of factory for Azure devices
        /// </summary>
        /// <param name="connectionString"></param>
        public AzureStorageNamedDeviceFactory(string connectionString)
            : this(BlobUtilsV12.GetServiceClients(connectionString))
        {
        }

        /// <summary>
        /// Create instance of factory for Azure devices
        /// </summary>
        /// <param name="pageBlobAccount"></param>
        AzureStorageNamedDeviceFactory(BlobUtilsV12.ServiceClients pageBlobAccount)
        {
            this.pageBlobAccount = pageBlobAccount;
        }

        /// <inheritdoc />
        public void Initialize(string baseName)
            => InitializeAsync(baseName).GetAwaiter().GetResult();
            
        
        async Task InitializeAsync(string baseName)
        {
            var path = baseName.Split('/');
            var containerName = path[0];
            var dirName = string.Join("/", path.Skip(1));

            this.pageBlobContainer = BlobUtilsV12.GetContainerClients(this.pageBlobAccount, containerName);
            await this.pageBlobContainer.WithRetries.CreateIfNotExistsAsync();

            pageBlobDirectory = new BlobUtilsV12.BlobDirectory(pageBlobContainer, dirName);
        }


        /// <inheritdoc />
        public void Delete(FileDescriptor fileInfo)
        {
            var dir = fileInfo.directoryName == "" ? pageBlobDirectory : pageBlobDirectory.GetSubDirectory(fileInfo.directoryName);

            if (fileInfo.fileName != null)
            {
                // We only delete shard 0
                dir.GetPageBlobClient(fileInfo.fileName + ".0").Default.DeleteIfExists();
            }
            else
            {
                foreach (var blob in dir.Client.WithRetries.GetBlobs())
                {
                    dir.GetPageBlobClient(blob.Name).Default.DeleteIfExists();
                }
            }
        }

        /// <inheritdoc />
        public IDevice Get(FileDescriptor fileInfo)
        {
            return new AzureStorageDevice(fileInfo.fileName, default, pageBlobDirectory.GetSubDirectory(fileInfo.directoryName), null, false);
        }

        /// <inheritdoc />
        public IEnumerable<FileDescriptor> ListContents(string path)
        {
            var dir = pageBlobDirectory.GetSubDirectory(path);
            var client = dir.Client.WithRetries;
            foreach (var item in client.GetBlobsByHierarchy()
                .OrderByDescending(f => dir.GetPageBlobClient(f.Blob.Name).Default.GetProperties().Value.LastModified))
            {
                if (item.IsPrefix)
                {
                    yield return new FileDescriptor
                    {
                        directoryName = item.Prefix,
                        fileName = ""
                    };
                }
                else
                {
                    yield return new FileDescriptor
                    {
                        directoryName = item.Blob.Name,
                        fileName = ""
                    };
                }
            }
        }
    }
}
