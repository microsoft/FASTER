// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace DurableTask.Netherite.Faster
{
    using DurableTask.Core.Common;
    using FASTER.core;
    using Azure.Storage.Blobs;
    using Microsoft.Extensions.Logging;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Azure.Storage.Blobs.Specialized;
    using Azure.Storage.Blobs.Models;
    using System.Net;
    using System.Text;

    /// <summary>
    /// Provides management of blobs and blob names associated with a partition, and logic for partition lease maintenance and termination.
    /// </summary>
    partial class BlobManager : ICheckpointManager, ILogCommitManager
    {
        readonly NetheriteOrchestrationServiceSettings settings;
        readonly uint partitionId;
        readonly CancellationTokenSource shutDownOrTermination;
        readonly string taskHubPrefix;

        BlobUtilsV12.ServiceClients blockBlobAccount;
        BlobUtilsV12.ServiceClients pageBlobAccount;

        BlobUtilsV12.ContainerClients blockBlobContainer;
        BlobUtilsV12.ContainerClients pageBlobContainer;

        BlobUtilsV12.BlockBlobClients eventLogCommitBlob;
        BlobLeaseClient leaseClient;

        BlobUtilsV12.BlobDirectory pageBlobPartitionDirectory;
        BlobUtilsV12.BlobDirectory blockBlobPartitionDirectory;

        readonly TimeSpan LeaseDuration = TimeSpan.FromSeconds(45); // max time the lease stays after unclean shutdown
        readonly TimeSpan LeaseRenewal = TimeSpan.FromSeconds(30); // how often we renew the lease
        readonly TimeSpan LeaseSafetyBuffer = TimeSpan.FromSeconds(10); // how much time we want left on the lease before issuing a protected access

        internal CheckpointInfo CheckpointInfo { get; }
        Azure.ETag? CheckpointInfoETag { get; set; }

        internal FasterTraceHelper TraceHelper { get; private set; }
        internal FasterTraceHelper StorageTracer => this.TraceHelper.IsTracingAtMostDetailedLevel ? this.TraceHelper : null;

        public IDevice EventLogDevice { get; private set; }
        public IDevice HybridLogDevice { get; private set; }
        public IDevice ObjectLogDevice { get; private set; }

        public DateTime IncarnationTimestamp { get; private set; }

        public string ContainerName { get; }

        internal BlobUtilsV12.ContainerClients BlockBlobContainer => this.blockBlobContainer;
        internal BlobUtilsV12.ContainerClients PageBlobContainer => this.pageBlobContainer;

        public int PartitionId => (int)this.partitionId;

        public IPartitionErrorHandler PartitionErrorHandler { get; private set; }

        internal static SemaphoreSlim AsynchronousStorageReadMaxConcurrency = new SemaphoreSlim(Math.Min(100, Environment.ProcessorCount * 10));
        internal static SemaphoreSlim AsynchronousStorageWriteMaxConcurrency = new SemaphoreSlim(Math.Min(50, Environment.ProcessorCount * 7));

        internal volatile int LeaseUsers;

        volatile System.Diagnostics.Stopwatch leaseTimer;

        internal const long HashTableSize = 1L << 14; // 16 k buckets, 1 MB
        internal const long HashTableSizeBytes = HashTableSize * 64;

        public class FasterTuningParameters
        {
            public int? EventLogPageSizeBits;
            public int? EventLogSegmentSizeBits;
            public int? EventLogMemorySizeBits;
            public int? StoreLogPageSizeBits;
            public int? StoreLogSegmentSizeBits;
            public int? StoreLogMemorySizeBits;
            public double? StoreLogMutableFraction;
            public int? EstimatedAverageObjectSize;
            public int? NumPagesToPreload;
        }

        public FasterLogSettings GetDefaultEventLogSettings(bool useSeparatePageBlobStorage, FasterTuningParameters tuningParameters) => new FasterLogSettings
        {
            LogDevice = this.EventLogDevice,
            LogCommitManager = this.UseLocalFiles
                ? null // TODO: fix this: new LocalLogCommitManager($"{this.LocalDirectoryPath}\\{this.PartitionFolderName}\\{CommitBlobName}")
                : (ILogCommitManager)this,
            PageSizeBits = tuningParameters?.EventLogPageSizeBits ?? 21, // 2MB
            SegmentSizeBits = tuningParameters?.EventLogSegmentSizeBits ??
                (useSeparatePageBlobStorage ? 35  // 32 GB
                                            : 26), // 64 MB
            MemorySizeBits = tuningParameters?.EventLogMemorySizeBits ?? 22, // 2MB
        };

        public LogSettings GetDefaultStoreLogSettings(
            bool useSeparatePageBlobStorage, 
            long upperBoundOnAvailable, 
            FasterTuningParameters tuningParameters)
        {
            int pageSizeBits = tuningParameters?.StoreLogPageSizeBits ?? 10; // default page size is 1k

            // compute a reasonable memory size for the log considering maximally available memory, and expansion factor
            int memorybits = 0;
            if (tuningParameters?.StoreLogMemorySizeBits != null)
            {
                memorybits = tuningParameters.StoreLogMemorySizeBits.Value;
            }
            else
            {
                double expansionFactor = (24 + ((double)(tuningParameters?.EstimatedAverageObjectSize ?? 216))) / 24;
                long estimate = (long)(upperBoundOnAvailable / expansionFactor);
                
                while (estimate > 0)
                {
                    memorybits++;
                    estimate >>= 1;
                }
                memorybits = Math.Max(pageSizeBits + 2, memorybits); // never use less than 4 pages
            }

            return new LogSettings
            {
                LogDevice = this.HybridLogDevice,
                ObjectLogDevice = this.ObjectLogDevice,
                PageSizeBits = pageSizeBits,  
                MutableFraction = tuningParameters?.StoreLogMutableFraction ?? 0.9,
                SegmentSizeBits = tuningParameters?.StoreLogSegmentSizeBits ??
                    (useSeparatePageBlobStorage ? 35   // 32 GB
                                                : 32), // 4 GB
                PreallocateLog = false,
                ReadFlags = ReadFlags.None,
                ReadCacheSettings = null, // no read cache
                MemorySizeBits = memorybits,
            };
        }


        static readonly int[] StorageFormatVersion = new int[] {
            1, //initial version
            2, //0.7.0-beta changed singleton storage, and adds dequeue count
            3, //changed organization of files
            4, //use Faster v2, reduced page size
            5, //support EventHub recovery
        }; 

        public static string GetStorageFormat(NetheriteOrchestrationServiceSettings settings)
        {
            return JsonConvert.SerializeObject(new StorageFormatSettings()
                {
                    UseAlternateObjectStore = settings.UseAlternateObjectStore,
                    FormatVersion = StorageFormatVersion.Last(),
                },
                serializerSettings);
        }

        [JsonObject]
        class StorageFormatSettings
        {
            // this must stay the same

            [JsonProperty("FormatVersion")]
            public int FormatVersion { get; set; }

            // the following can be changed between versions

            [JsonProperty("UseAlternateObjectStore", DefaultValueHandling = DefaultValueHandling.Ignore)]
            public bool? UseAlternateObjectStore { get; set; }
        }

        static readonly JsonSerializerSettings serializerSettings = new JsonSerializerSettings()
        {
            TypeNameHandling = TypeNameHandling.None,
            MissingMemberHandling = MissingMemberHandling.Ignore,
            CheckAdditionalContent = false,
            Formatting = Formatting.None,
        };

        public static void CheckStorageFormat(string format, NetheriteOrchestrationServiceSettings settings)
        {
            try
            {
                var taskhubFormat = JsonConvert.DeserializeObject<StorageFormatSettings>(format, serializerSettings);

                if (taskhubFormat.UseAlternateObjectStore != settings.UseAlternateObjectStore)
                {
                    throw new NetheriteConfigurationException("The Netherite configuration setting 'UseAlternateObjectStore' is incompatible with the existing taskhub.");
                }
                if (taskhubFormat.FormatVersion != StorageFormatVersion.Last())
                {
                    throw new NetheriteConfigurationException($"The current storage format version (={StorageFormatVersion.Last()}) is incompatible with the existing taskhub (={taskhubFormat.FormatVersion}).");
                }
            }
            catch (Exception e)
            {
                throw new NetheriteConfigurationException("The taskhub has an incompatible storage format", e);
            }
        }

        public void Dispose()
        {
            // we do not need to dispose any resources for the commit manager, because any such resources are deleted together with the taskhub
        }

        public void Purge(Guid token)
        {
            throw new NotImplementedException("Purges are handled directly on recovery, not via FASTER");
        }

        public void PurgeAll()
        {
            throw new NotImplementedException("Purges are handled directly on recovery, not via FASTER");
        }

        public void OnRecovery(Guid indexToken, Guid logToken)
        {
            // we handle cleanup of old checkpoints somewhere else
        }

        public CheckpointSettings StoreCheckpointSettings => new CheckpointSettings
        {
            CheckpointManager = this.UseLocalFiles ? (ICheckpointManager)this.LocalCheckpointManager : (ICheckpointManager)this,
        };

        public const int MaxRetries = 10;

        public static TimeSpan GetDelayBetweenRetries(int numAttempts)
            => TimeSpan.FromSeconds(Math.Pow(2, (numAttempts - 1)));

        /// <summary>
        /// Create a blob manager.
        /// </summary>
        /// <param name="storageAccount">The cloud storage account, or null if using local file paths</param>
        /// <param name="pageBlobAccount">The storage account to use for page blobs</param>
        /// <param name="localFilePath">The local file path, or null if using cloud storage</param>
        /// <param name="taskHubName">The name of the taskhub</param>
        /// <param name="logger">A logger for logging</param>
        /// <param name="logLevelLimit">A limit on log event level emitted</param>
        /// <param name="partitionId">The partition id</param>
        /// <param name="errorHandler">A handler for errors encountered in this partition</param>
        public BlobManager(
            NetheriteOrchestrationServiceSettings settings,
            string taskHubName,
            string taskHubPrefix,
            FaultInjector faultInjector,
            ILogger logger,
            ILogger performanceLogger,
            Microsoft.Extensions.Logging.LogLevel logLevelLimit,
            uint partitionId, 
            IPartitionErrorHandler errorHandler)
        {
            this.settings = settings;
            this.ContainerName = GetContainerName(taskHubName);
            this.taskHubPrefix = taskHubPrefix;
            this.FaultInjector = faultInjector;
            this.partitionId = partitionId;
            this.CheckpointInfo = new CheckpointInfo();
            this.CheckpointInfoETag = default;

            if (!string.IsNullOrEmpty(settings.UseLocalDirectoryForPartitionStorage))
            {
                this.UseLocalFiles = true;
                this.LocalFileDirectoryForTestingAndDebugging = settings.UseLocalDirectoryForPartitionStorage;
                this.LocalCheckpointManager = new LocalFileCheckpointManager(
                   this.CheckpointInfo,
                   this.LocalCheckpointDirectoryPath,
                   this.GetCheckpointCompletedBlobName());
            }
            else
            {
                this.UseLocalFiles = false;
            }
            this.TraceHelper = new FasterTraceHelper(logger, logLevelLimit, performanceLogger, this.partitionId, this.UseLocalFiles ? "none" : this.settings.StorageAccountName, taskHubName);
            this.PartitionErrorHandler = errorHandler;
            this.shutDownOrTermination = CancellationTokenSource.CreateLinkedTokenSource(errorHandler.Token);
        }

        string PartitionFolderName => $"{this.taskHubPrefix}p{this.partitionId:D2}";

        // For testing and debugging with local files
        bool UseLocalFiles { get; }
        LocalFileCheckpointManager LocalCheckpointManager { get; set; }
        string LocalFileDirectoryForTestingAndDebugging { get; }
        string LocalDirectoryPath => $"{this.LocalFileDirectoryForTestingAndDebugging}\\{this.ContainerName}";
        string LocalCheckpointDirectoryPath => $"{this.LocalDirectoryPath}\\chkpts{this.partitionId:D2}";

        const string EventLogBlobName = "commit-log";
        const string CommitBlobName = "commit-lease";
        const string HybridLogBlobName = "store";
        const string ObjectLogBlobName = "store.obj";

        Task LeaseMaintenanceLoopTask = Task.CompletedTask;
        volatile Task NextLeaseRenewalTask = Task.CompletedTask;

        public static string GetContainerName(string taskHubName) => taskHubName.ToLowerInvariant() + "-storage";

        public async Task StartAsync()
        {
            if (this.UseLocalFiles)
            {
                this.LocalCheckpointManager = new LocalFileCheckpointManager(
                       this.CheckpointInfo,
                       this.LocalCheckpointDirectoryPath,
                       this.GetCheckpointCompletedBlobName());

                Directory.CreateDirectory($"{this.LocalDirectoryPath}\\{this.PartitionFolderName}");

                this.EventLogDevice = Devices.CreateLogDevice($"{this.LocalDirectoryPath}\\{this.PartitionFolderName}\\{EventLogBlobName}");
                this.HybridLogDevice = Devices.CreateLogDevice($"{this.LocalDirectoryPath}\\{this.PartitionFolderName}\\{HybridLogBlobName}");
                this.ObjectLogDevice = Devices.CreateLogDevice($"{this.LocalDirectoryPath}\\{this.PartitionFolderName}\\{ObjectLogBlobName}");

                // This does not acquire any blob ownership, but is needed for the lease maintenance loop which calls PartitionErrorHandler.TerminateNormally() when done.
                await this.AcquireOwnership();
            }
            else
            {
                this.blockBlobAccount = BlobUtilsV12.GetServiceClients(this.settings.BlobStorageConnection);
                this.blockBlobContainer = BlobUtilsV12.GetContainerClients(this.blockBlobAccount, this.ContainerName);
                await this.blockBlobContainer.WithRetries.CreateIfNotExistsAsync();
                this.blockBlobPartitionDirectory = new BlobUtilsV12.BlobDirectory(this.blockBlobContainer, this.PartitionFolderName);

                if (this.settings.PageBlobStorageConnection != null)
                {
                    this.pageBlobAccount = BlobUtilsV12.GetServiceClients(this.settings.PageBlobStorageConnection);
                    this.pageBlobContainer = BlobUtilsV12.GetContainerClients(this.pageBlobAccount, this.ContainerName);
                    await this.pageBlobContainer.WithRetries.CreateIfNotExistsAsync();
                    this.pageBlobPartitionDirectory = new BlobUtilsV12.BlobDirectory(this.pageBlobContainer, this.PartitionFolderName);
                }
                else
                {
                    this.pageBlobAccount = this.blockBlobAccount;
                    this.pageBlobContainer = this.BlockBlobContainer;
                    this.pageBlobPartitionDirectory = this.blockBlobPartitionDirectory;
                }

                this.eventLogCommitBlob = this.blockBlobPartitionDirectory.GetBlockBlobClient(CommitBlobName);
                this.leaseClient = this.eventLogCommitBlob.WithRetries.GetBlobLeaseClient();

                AzureStorageDevice createDevice(string name) =>
                     new AzureStorageDevice(name, this.blockBlobPartitionDirectory.GetSubDirectory(name), this.pageBlobPartitionDirectory.GetSubDirectory(name), this, true);

                var eventLogDevice = createDevice(EventLogBlobName);
                var hybridLogDevice = createDevice(HybridLogBlobName);
                var objectLogDevice = createDevice(ObjectLogBlobName);

                await this.AcquireOwnership();

                this.TraceHelper.FasterProgress("Starting Faster Devices");
                var startTasks = new List<Task>
                {
                    eventLogDevice.StartAsync(),
                    hybridLogDevice.StartAsync(),
                    objectLogDevice.StartAsync()
                };
                await Task.WhenAll(startTasks);
                this.TraceHelper.FasterProgress("Started Faster Devices");

                this.EventLogDevice = eventLogDevice;
                this.HybridLogDevice = hybridLogDevice;
                this.ObjectLogDevice = objectLogDevice;
            }
        }

        internal void DisposeDevices()
        {
            Dispose(this.HybridLogDevice);
            Dispose(this.ObjectLogDevice);

            void Dispose(IDevice device)
            {
                this.TraceHelper.FasterStorageProgress($"Disposing Device {device.FileName}");
                device.Dispose();
            }
        }

        public void HandleStorageError(string where, string message, string blobName, Exception e, bool isFatal, bool isWarning)
        {
            if (blobName == null)
            {
                this.PartitionErrorHandler.HandleError(where, message, e, isFatal, isWarning);
            }
            else
            {
                this.PartitionErrorHandler.HandleError(where, $"{message} blob={blobName}", e, isFatal, isWarning);
            }
        }

        // clean shutdown, wait for everything, then terminate
        public async Task StopAsync()
        {
            this.shutDownOrTermination.Cancel(); // has no effect if already cancelled

            await this.LeaseMaintenanceLoopTask; // wait for loop to terminate cleanly
        }

        public static async Task DeleteTaskhubStorageAsync(NetheriteOrchestrationServiceSettings settings, string pathPrefix)
        {
            var containerName = GetContainerName(settings.HubName);

            if (!string.IsNullOrEmpty(settings.UseLocalDirectoryForPartitionStorage))
            {
                DirectoryInfo di = new DirectoryInfo($"{settings.UseLocalDirectoryForPartitionStorage}\\{containerName}"); //TODO fine-grained deletion
                if (di.Exists)
                {
                    di.Delete(true);
                }
            }
            else
            {
                var blockBlobAccount = BlobUtilsV12.GetServiceClients(settings.BlobStorageConnection);
                await DeleteContainerContents(blockBlobAccount.WithRetries);

                if (settings.PageBlobStorageConnection != null)
                {
                    var pageBlobAccount = BlobUtilsV12.GetServiceClients(settings.PageBlobStorageConnection);
                    await DeleteContainerContents(pageBlobAccount.Default);
                }

                async Task DeleteContainerContents(BlobServiceClient account)
                {
                    var container = account.GetBlobContainerClient(containerName);
                    var deletionTasks = new List<Task>();
                    try
                    {
                        await foreach (BlobItem blob in container.GetBlobsAsync(BlobTraits.None, BlobStates.None, string.Empty))
                        {
                            deletionTasks.Add(BlobUtilsV12.ForceDeleteAsync(container, blob.Name));
                        }
                    }
                    catch (Azure.RequestFailedException e) when (e.Status == (int)HttpStatusCode.NotFound)
                    {
                    }
                    await Task.WhenAll(deletionTasks);
                }

                // We are not deleting the container itself because it creates problems when trying to recreate
                // the same container soon afterwards. So we prefer to leave an empty container behind. 
            }
        }

        public ValueTask ConfirmLeaseIsGoodForAWhileAsync()
        {
            if (this.leaseTimer?.Elapsed < this.LeaseDuration - this.LeaseSafetyBuffer && !this.shutDownOrTermination.IsCancellationRequested)
            {
                return default;
            }
            this.TraceHelper.LeaseProgress("Access is waiting for fresh lease");
            return new ValueTask(this.NextLeaseRenewalTask);
        }

        public void ConfirmLeaseIsGoodForAWhile()
        {
            if (this.leaseTimer?.Elapsed < this.LeaseDuration - this.LeaseSafetyBuffer && !this.shutDownOrTermination.IsCancellationRequested)
            {
                return;
            }
            this.TraceHelper.LeaseProgress("Access is waiting for fresh lease");
            this.NextLeaseRenewalTask.Wait();
        }

        async Task AcquireOwnership()
        {
            var newLeaseTimer = new System.Diagnostics.Stopwatch();
            int numAttempts = 0;

            while (true)
            {
                this.PartitionErrorHandler.Token.ThrowIfCancellationRequested();
                numAttempts++;

                try
                {
                    newLeaseTimer.Restart();

                    if (!this.UseLocalFiles)
                    {
                        this.FaultInjector?.StorageAccess(this, "AcquireLeaseAsync", "AcquireOwnership", this.eventLogCommitBlob.Name);
                        await this.leaseClient.AcquireAsync(
                            this.LeaseDuration,
                            null,
                            this.PartitionErrorHandler.Token)
                            .ConfigureAwait(false);
                        this.TraceHelper.LeaseAcquired();
                    }

                    this.IncarnationTimestamp = DateTime.UtcNow;
                    this.leaseTimer = newLeaseTimer;
                    this.LeaseMaintenanceLoopTask = Task.Run(() => this.MaintenanceLoopAsync());
                    return;
                }
                catch (Azure.RequestFailedException ex) when (BlobUtilsV12.LeaseConflictOrExpired(ex))
                {
                    this.TraceHelper.LeaseProgress("Waiting for lease");

                    this.FaultInjector?.BreakLease(this.eventLogCommitBlob); // during fault injection tests, we don't want to wait

                    // the previous owner has not released the lease yet, 
                    // try again until it becomes available, should be relatively soon
                    // as the transport layer is supposed to shut down the previous owner when starting this
                    await Task.Delay(TimeSpan.FromSeconds(1), this.PartitionErrorHandler.Token);

                    continue;
                }
                catch (Azure.RequestFailedException ex) when (BlobUtilsV12.BlobDoesNotExist(ex))
                {
                    // Create blob with empty content, then try again
                    await this.PerformWithRetriesAsync(
                        null,
                        false,
                        "CloudBlockBlob.UploadFromByteArrayAsync",
                        "CreateCommitLog",
                        "",
                        this.eventLogCommitBlob.Default.Name,
                        2000,
                        true,
                        async (numAttempts) =>
                        {
                            try
                            {
                                var client = numAttempts > 2 ? this.eventLogCommitBlob.Default : this.eventLogCommitBlob.Aggressive;
                                await client.UploadAsync(new MemoryStream());
                            }
                            catch (Azure.RequestFailedException ex2) when (BlobUtilsV12.LeaseConflictOrExpired(ex2))
                            {
                                // creation race, try from top
                                this.TraceHelper.LeaseProgress("Creation race observed, retrying");
                            }

                            return 1;
                        });

                    continue;
                }
                catch (OperationCanceledException) when (this.PartitionErrorHandler.IsTerminated)
                {
                    throw; // o.k. during termination or shutdown
                }
                catch (Exception e) when (this.PartitionErrorHandler.IsTerminated)
                {
                    string message = $"Lease acquisition was canceled";
                    this.TraceHelper.LeaseProgress(message);
                    throw new OperationCanceledException(message, e);
                }
                catch (Exception ex) when (numAttempts < BlobManager.MaxRetries
                    && !this.PartitionErrorHandler.IsTerminated && BlobUtils.IsTransientStorageError(ex))
                {
                    if (BlobUtils.IsTimeout(ex))
                    {
                        this.TraceHelper.FasterPerfWarning($"Lease acquisition timed out, retrying now");
                    }
                    else
                    {
                        TimeSpan nextRetryIn = BlobManager.GetDelayBetweenRetries(numAttempts);
                        this.TraceHelper.FasterPerfWarning($"Lease acquisition failed transiently, retrying in {nextRetryIn}");
                        await Task.Delay(nextRetryIn);
                    }
                    continue;
                }
                catch (Exception e) when (!Utils.IsFatal(e))
                {
                    this.PartitionErrorHandler.HandleError(nameof(AcquireOwnership), "Could not acquire partition lease", e, true, false);
                    throw;
                }
            }
        }

        public async Task RenewLeaseTask()
        {
            try
            {
                this.shutDownOrTermination.Token.ThrowIfCancellationRequested();

                var nextLeaseTimer = new System.Diagnostics.Stopwatch();
                nextLeaseTimer.Start();

                if (!this.UseLocalFiles)
                {
                    this.TraceHelper.LeaseProgress($"Renewing lease at {this.leaseTimer.Elapsed.TotalSeconds - this.LeaseDuration.TotalSeconds}s");
                    this.FaultInjector?.StorageAccess(this, "RenewLeaseAsync", "RenewLease", this.eventLogCommitBlob.Name);
                    await this.leaseClient.RenewAsync(null, this.PartitionErrorHandler.Token).ConfigureAwait(false);
                    this.TraceHelper.LeaseRenewed(this.leaseTimer.Elapsed.TotalSeconds, this.leaseTimer.Elapsed.TotalSeconds - this.LeaseDuration.TotalSeconds);

                    if (nextLeaseTimer.ElapsedMilliseconds > 2000)
                    {
                        this.TraceHelper.FasterPerfWarning($"RenewLeaseAsync took {nextLeaseTimer.Elapsed.TotalSeconds:F1}s, which is excessive; {this.leaseTimer.Elapsed.TotalSeconds - this.LeaseDuration.TotalSeconds}s past expiry");
                    }
                }

                this.leaseTimer = nextLeaseTimer;
            }
            catch (OperationCanceledException) when (this.PartitionErrorHandler.IsTerminated)
            {
                throw; // o.k. during termination or shutdown
            }
            catch (Exception e) when (!Utils.IsFatal(e))
            {
                this.TraceHelper.LeaseLost(this.leaseTimer.Elapsed.TotalSeconds, nameof(RenewLeaseTask));
                throw;
            }
        }

        public async Task MaintenanceLoopAsync()
        {
            this.TraceHelper.LeaseProgress("Started lease maintenance loop");
            try
            {
                while (true)
                {
                    int timeLeft = (int)(this.LeaseRenewal - this.leaseTimer.Elapsed).TotalMilliseconds;

                    if (timeLeft <= 0)
                    {
                        this.NextLeaseRenewalTask = this.RenewLeaseTask();
                    }
                    else
                    {
                        this.NextLeaseRenewalTask = LeaseTimer.Instance.Schedule(timeLeft, this.RenewLeaseTask, this.shutDownOrTermination.Token);
                    }

                    // wait for successful renewal, or exit the loop as this throws
                    await this.NextLeaseRenewalTask;
                }
            }
            catch (OperationCanceledException)
            {
                // it's o.k. to cancel while waiting
                this.TraceHelper.LeaseProgress("Lease renewal loop cleanly canceled");
            }
            catch (Azure.RequestFailedException e) when (e.InnerException != null && e.InnerException is OperationCanceledException)
            {
                // it's o.k. to cancel a lease renewal
                this.TraceHelper.LeaseProgress("Lease renewal storage operation canceled");
            }
            catch (Azure.RequestFailedException ex) when (BlobUtilsV12.LeaseConflict(ex))
            {
                // We lost the lease to someone else. Terminate ownership immediately.
                this.PartitionErrorHandler.HandleError(nameof(MaintenanceLoopAsync), "Lost partition lease", ex, true, true);
            }
            catch (Exception e) when (!Utils.IsFatal(e))
            {
                this.PartitionErrorHandler.HandleError(nameof(MaintenanceLoopAsync), "Could not maintain partition lease", e, true, false);
            }

            this.TraceHelper.LeaseProgress("Exited lease maintenance loop");

            while (this.LeaseUsers > 0
                && !this.PartitionErrorHandler.IsTerminated 
                && (this.leaseTimer?.Elapsed < this.LeaseDuration))
            {
                await Task.Delay(20); // give storage accesses that are in progress and require the lease a chance to complete
            }

            this.TraceHelper.LeaseProgress("Waited for lease users to complete");

            // release the lease
            if (!this.UseLocalFiles)
            {
                try
                {
                    this.TraceHelper.LeaseProgress("Releasing lease");

                    this.FaultInjector?.StorageAccess(this, "ReleaseLeaseAsync", "ReleaseLease", this.eventLogCommitBlob.Name);
                    await this.leaseClient.ReleaseAsync(null, this.PartitionErrorHandler.Token).ConfigureAwait(false);
                    this.TraceHelper.LeaseReleased(this.leaseTimer.Elapsed.TotalSeconds);
                }
                catch (OperationCanceledException)
                {
                    // it's o.k. if termination is triggered while waiting
                }
                catch (Azure.RequestFailedException e) when (e.InnerException != null && e.InnerException is OperationCanceledException)
                {
                    // it's o.k. if termination is triggered while we are releasing the lease
                }
                catch (Exception e)
                {
                    // we swallow, but still report exceptions when releasing a lease
                    this.PartitionErrorHandler.HandleError(nameof(MaintenanceLoopAsync), "Could not release partition lease during shutdown", e, false, true);
                }
            }

            this.PartitionErrorHandler.TerminateNormally();

            this.TraceHelper.LeaseProgress("Blob manager stopped");
        }

        public async Task RemoveObsoleteCheckpoints()
        {
            if (this.UseLocalFiles)
            {
                //TODO
                return;
            }
            else
            {
                string token1 = this.CheckpointInfo.LogToken.ToString();
                string token2 = this.CheckpointInfo.IndexToken.ToString();

                this.TraceHelper.FasterProgress($"Removing obsolete checkpoints, keeping only {token1} and {token2}");

                var tasks = new List<Task<(int, int)>>();

                tasks.Add(RemoveObsoleteCheckpoints(this.blockBlobPartitionDirectory.GetSubDirectory(cprCheckpointPrefix)));
                tasks.Add(RemoveObsoleteCheckpoints(this.blockBlobPartitionDirectory.GetSubDirectory(indexCheckpointPrefix)));

                if (this.settings.PageBlobStorageConnection != null)
                {
                    tasks.Add(RemoveObsoleteCheckpoints(this.pageBlobPartitionDirectory.GetSubDirectory(cprCheckpointPrefix)));
                    tasks.Add(RemoveObsoleteCheckpoints(this.pageBlobPartitionDirectory.GetSubDirectory(indexCheckpointPrefix)));
                }

                await Task.WhenAll(tasks);

                this.TraceHelper.FasterProgress($"Removed {tasks.Select(t => t.Result.Item1).Sum()} checkpoint directories containing {tasks.Select(t => t.Result.Item2).Sum()} blobs");

                async Task<(int, int)> RemoveObsoleteCheckpoints(BlobUtilsV12.BlobDirectory directory)
                {
                    List<string> results = null;

                    await this.PerformWithRetriesAsync(
                        BlobManager.AsynchronousStorageWriteMaxConcurrency,
                        true,
                        "BlobContainerClient.GetBlobsAsync",
                        "RemoveObsoleteCheckpoints",
                        "",
                        directory.Prefix,
                        1000,
                        false,
                        async (numAttempts) =>
                        {
                            results = await directory.GetBlobsAsync(this.shutDownOrTermination.Token);
                            return results.Count();
                        });


                    var checkpointFoldersToDelete = results
                        .GroupBy((s) => s.Split('/')[3])
                        .Where(g => g.Key != token1 && g.Key != token2)
                        .ToList();

                    var deletionTasks = new List<Task>();

                    foreach (var folder in checkpointFoldersToDelete)
                    {
                        deletionTasks.Add(DeleteCheckpointDirectory(folder));
                    }

                    await Task.WhenAll(deletionTasks);
                    return (checkpointFoldersToDelete.Count, results.Count);

                    async Task DeleteCheckpointDirectory(IEnumerable<string> blobsToDelete)
                    {
                        var deletionTasks = new List<Task>();
                        foreach (var blobName in blobsToDelete)
                        {
                            deletionTasks.Add(
                                this.PerformWithRetriesAsync(
                                    BlobManager.AsynchronousStorageWriteMaxConcurrency,
                                    false,
                                    "BlobUtils.ForceDeleteAsync",
                                    "DeleteCheckpointDirectory",
                                    "",
                                    blobName,
                                    1000,
                                    false,
                                    async (numAttempts) => (await BlobUtilsV12.ForceDeleteAsync(directory.Client.Default, blobName) ? 1 : 0)));
                        }
                        await Task.WhenAll(deletionTasks);
                    }
                }
            }
        }
    
        #region Blob Name Management

        string GetCheckpointCompletedBlobName() => "last-checkpoint.json";

        const string indexCheckpointPrefix = "index-checkpoints/";

        const string cprCheckpointPrefix = "cpr-checkpoints/";

        string GetIndexCheckpointMetaBlobName(Guid token) => $"{indexCheckpointPrefix}{token}/info.dat";

        (string, string) GetPrimaryHashTableBlobName(Guid token) => ($"{indexCheckpointPrefix}{token}", "ht.dat");

        string GetHybridLogCheckpointMetaBlobName(Guid token) => $"{cprCheckpointPrefix}{token}/info.dat";

        (string, string) GetLogSnapshotBlobName(Guid token) => ($"{cprCheckpointPrefix}{token}", "snapshot.dat");

        (string, string) GetObjectLogSnapshotBlobName(Guid token) => ($"{cprCheckpointPrefix}{token}", "snapshot.obj.dat");

        (string, string) GetDeltaLogSnapshotBlobName(Guid token) => ($"{cprCheckpointPrefix}{token}", "snapshot.delta.dat");

        string GetSingletonsSnapshotBlobName(Guid token) => $"{cprCheckpointPrefix}{token}/singletons.dat";

        #endregion

        #region ILogCommitManager

        void ILogCommitManager.Commit(long beginAddress, long untilAddress, byte[] commitMetadata, long commitNum)
        {
            try
            {
                this.StorageTracer?.FasterStorageProgress($"StorageOpCalled ILogCommitManager.Commit beginAddress={beginAddress} untilAddress={untilAddress}");

                this.PerformWithRetries(
                    false,
                    "BlockBlobClient.Upload",
                    "WriteCommitLogMetadata",
                    "",
                    this.eventLogCommitBlob.Default.Name,
                    1000,
                    true,
                    (int numAttempts) =>
                    {
                        try
                        {
                            var client = numAttempts > 2 ? this.eventLogCommitBlob.Default : this.eventLogCommitBlob.Aggressive;

                            client.Upload(
                                content: new MemoryStream(commitMetadata),
                                options: new BlobUploadOptions() { Conditions = new BlobRequestConditions() { LeaseId = this.leaseClient.LeaseId } },
                                cancellationToken: this.PartitionErrorHandler.Token);

                            return (commitMetadata.Length, true);
                        }
                        catch (Azure.RequestFailedException ex) when (BlobUtilsV12.LeaseConflict(ex))
                        {
                            // We lost the lease to someone else. Terminate ownership immediately.
                            this.TraceHelper.LeaseLost(this.leaseTimer.Elapsed.TotalSeconds, nameof(ILogCommitManager.Commit));
                            this.HandleStorageError(nameof(ILogCommitManager.Commit), "could not commit because of lost lease", this.eventLogCommitBlob.Name, ex, true, this.PartitionErrorHandler.IsTerminated);
                            throw;
                        }
                        catch (Azure.RequestFailedException ex) when (BlobUtilsV12.LeaseExpired(ex) && numAttempts < BlobManager.MaxRetries)
                        {
                            // if we get here, the lease renewal task did not complete in time
                            // give it another chance to complete
                            this.TraceHelper.LeaseProgress("ILogCommitManager.Commit: wait for next renewal");
                            this.NextLeaseRenewalTask.Wait();
                            this.TraceHelper.LeaseProgress("ILogCommitManager.Commit: renewal complete");
                            return (commitMetadata.Length, false);
                        }
                    });

                this.StorageTracer?.FasterStorageProgress($"StorageOpReturned ILogCommitManager.Commit");
            }
            catch
            {
                this.StorageTracer?.FasterStorageProgress($"StorageOpReturned ILogCommitManager.Commit failed");
                throw;
            }
        }


        IEnumerable<long> ILogCommitManager.ListCommits()
        {
            // we only use a single commit file in this implementation
            yield return 0;
        }

        void ILogCommitManager.OnRecovery(long commitNum) 
        { 
            // TODO: make sure our use of single commit is safe
        }

        void ILogCommitManager.RemoveAllCommits()
        {
            // TODO: make sure our use of single commit is safe
        }

        void ILogCommitManager.RemoveCommit(long commitNum) 
        {
            // TODO: make sure our use of single commit is safe
        }

        byte[] ILogCommitManager.GetCommitMetadata(long commitNum)
        {
            try
            {
                this.StorageTracer?.FasterStorageProgress($"StorageOpCalled ILogCommitManager.GetCommitMetadata (thread={Thread.CurrentThread.ManagedThreadId})");

                using var stream = new MemoryStream();

                this.PerformWithRetries(
                   false,
                   "BlobClient.DownloadTo",
                   "ReadCommitLogMetadata",
                   "",
                   this.eventLogCommitBlob.Name,
                   1000,
                   true,
                   (int numAttempts) =>
                   {
                       if (numAttempts > 0)
                       {
                           stream.Seek(0, SeekOrigin.Begin);
                       }

                       try
                       {
                           var client = numAttempts > 2 ? this.eventLogCommitBlob.Default : this.eventLogCommitBlob.Aggressive;

                           client.DownloadTo(
                               destination: stream,
                               conditions: new BlobRequestConditions() { LeaseId = this.leaseClient.LeaseId },
                               cancellationToken: this.PartitionErrorHandler.Token);

                           return (stream.Position, true);
                       }
                       catch (Azure.RequestFailedException ex) when (BlobUtilsV12.LeaseConflict(ex))
                       {
                           // We lost the lease to someone else. Terminate ownership immediately.
                           this.TraceHelper.LeaseLost(this.leaseTimer.Elapsed.TotalSeconds, nameof(ILogCommitManager.GetCommitMetadata));
                           this.HandleStorageError(nameof(ILogCommitManager.Commit), "could not read latest commit due to lost lease", this.eventLogCommitBlob.Name, ex, true, this.PartitionErrorHandler.IsTerminated);
                           throw;
                       }
                       catch (Azure.RequestFailedException ex) when (BlobUtilsV12.LeaseExpired(ex) && numAttempts < BlobManager.MaxRetries)
                       {
                           // if we get here, the lease renewal task did not complete in time
                           // give it another chance to complete
                           this.TraceHelper.LeaseProgress("ILogCommitManager.Commit: wait for next renewal");
                           this.NextLeaseRenewalTask.Wait();
                           this.TraceHelper.LeaseProgress("ILogCommitManager.Commit: renewal complete");
                           return (0, false);
                       }
                   });

                var bytes = stream.ToArray();
                this.StorageTracer?.FasterStorageProgress($"StorageOpReturned ILogCommitManager.GetCommitMetadata {bytes?.Length ?? null} bytes");
                return bytes.Length == 0 ? null : bytes;
            }
            catch (Exception e)
            {
                this.StorageTracer?.FasterStorageProgress($"StorageOpReturned ILogCommitManager.GetCommitMetadata failed with {e.GetType().Name}: {e.Message}");
                throw;
            }
        }

        #endregion

        #region ICheckpointManager

        void ICheckpointManager.InitializeIndexCheckpoint(Guid indexToken)
        {
            // there is no need to create empty directories in a blob container
        }

        void ICheckpointManager.InitializeLogCheckpoint(Guid logToken)
        {
            // there is no need to create empty directories in a blob container
        }

        IEnumerable<Guid> ICheckpointManager.GetIndexCheckpointTokens()
        {
            var indexToken = this.CheckpointInfo.IndexToken;
            this.StorageTracer?.FasterStorageProgress($"StorageOp ICheckpointManager.GetIndexCheckpointTokens indexToken={indexToken}");
            yield return indexToken;
        }

        IEnumerable<Guid> ICheckpointManager.GetLogCheckpointTokens()
        {
            var logToken = this.CheckpointInfo.LogToken;
            this.StorageTracer?.FasterStorageProgress($"StorageOp ICheckpointManager.GetLogCheckpointTokens logToken={logToken}");
            yield return logToken;
        }

        internal async Task<bool> FindCheckpointsAsync(bool logIsEmpty)
        {
            BlobUtilsV12.BlockBlobClients checkpointCompletedBlob = default;
            try
            {
                string jsonString = null;

                if (this.UseLocalFiles)
                {
                    try
                    {
                        jsonString = this.LocalCheckpointManager.GetLatestCheckpointJson();
                    }
                    catch (FileNotFoundException) when (logIsEmpty)
                    {
                        // ok to not have a checkpoint yet
                    }
                }
                else
                {
                    var partDir = this.blockBlobPartitionDirectory;
                    checkpointCompletedBlob = partDir.GetBlockBlobClient(this.GetCheckpointCompletedBlobName());

                    await this.PerformWithRetriesAsync(
                        semaphore: null,
                        requireLease: true,
                        "BlockBlobClient.DownloadContentAsync",
                        "FindCheckpointsAsync",
                        "",
                        checkpointCompletedBlob.Name,
                        1000,
                        true,
                        async (numAttempts) =>
                        {
                            try
                            {
                                Azure.Response<BlobDownloadResult> downloadResult = await checkpointCompletedBlob.WithRetries.DownloadContentAsync();
                                jsonString = downloadResult.Value.Content.ToString();
                                this.CheckpointInfoETag = downloadResult.Value.Details.ETag;
                                return 1;
                            }
                            catch (Azure.RequestFailedException e) when (BlobUtilsV12.BlobDoesNotExist(e) && logIsEmpty)
                            {
                                // ok to not have a checkpoint yet
                                return 0;
                            }
                        });
                }

                if (jsonString == null)
                {
                    return false;
                }
                else
                {
                    // read the fields from the json to update the checkpoint info
                    JsonConvert.PopulateObject(jsonString, this.CheckpointInfo);
                    return true;
                }
            }
            catch (Exception e)
            {
                this.HandleStorageError(nameof(FindCheckpointsAsync), "could not find any checkpoint", checkpointCompletedBlob.Name, e, true, this.PartitionErrorHandler.IsTerminated);
                throw;
            }
        }

        void ICheckpointManager.CommitIndexCheckpoint(Guid indexToken, byte[] commitMetadata)
        {
            try
            {
                this.StorageTracer?.FasterStorageProgress($"StorageOpCalled ICheckpointManager.CommitIndexCheckpoint, indexToken={indexToken}");
                var partDir = this.blockBlobPartitionDirectory;
                var metaFileBlob = partDir.GetBlockBlobClient(this.GetIndexCheckpointMetaBlobName(indexToken));

                this.PerformWithRetries(
                 false,
                 "BlockBlobClient.OpenWrite",
                 "WriteIndexCheckpointMetadata",
                 $"token={indexToken} size={commitMetadata.Length}",
                 metaFileBlob.Name,
                 1000,
                 true,
                 (numAttempts) =>
                 {
                     var client = metaFileBlob.WithRetries;
                     using var blobStream = client.OpenWrite(overwrite: true);
                     using var writer = new BinaryWriter(blobStream);
                     writer.Write(commitMetadata.Length);
                     writer.Write(commitMetadata);
                     writer.Flush();
                     return (commitMetadata.Length, true);
                 });

                this.CheckpointInfo.IndexToken = indexToken;
                this.StorageTracer?.FasterStorageProgress($"StorageOpReturned ICheckpointManager.CommitIndexCheckpoint, target={metaFileBlob.Name}");
            }
            catch
            {
                this.StorageTracer?.FasterStorageProgress($"StorageOpReturned ICheckpointManager.CommitIndexCheckpoint failed");
                throw;
            }
        }

        void ICheckpointManager.CommitLogCheckpoint(Guid logToken, byte[] commitMetadata)
        {
            try
            {
                this.StorageTracer?.FasterStorageProgress($"StorageOpCalled ICheckpointManager.CommitLogCheckpoint, logToken={logToken}");
                var partDir = this.blockBlobPartitionDirectory;
                var metaFileBlob = partDir.GetBlockBlobClient(this.GetHybridLogCheckpointMetaBlobName(logToken));

                this.PerformWithRetries(
                    false,
                    "BlockBlobClient.OpenWrite",
                    "WriteHybridLogCheckpointMetadata",
                    $"token={logToken}",
                    metaFileBlob.Name,
                    1000,
                    true,
                    (numAttempts) =>
                    {
                        var client = metaFileBlob.WithRetries;
                        using var blobStream = client.OpenWrite(overwrite: true);
                        using var writer = new BinaryWriter(blobStream);
                        writer.Write(commitMetadata.Length);
                        writer.Write(commitMetadata);
                        writer.Flush();
                        return (commitMetadata.Length + 4, true);
                    });

                this.CheckpointInfo.LogToken = logToken;
                this.StorageTracer?.FasterStorageProgress($"StorageOpReturned ICheckpointManager.CommitLogCheckpoint, target={metaFileBlob.Name}");
            }
            catch
            {
                this.StorageTracer?.FasterStorageProgress($"StorageOpReturned ICheckpointManager.CommitLogCheckpoint failed");
                throw;
            }
        }

        void ICheckpointManager.CommitLogIncrementalCheckpoint(Guid logToken, long version, byte[] commitMetadata, DeltaLog deltaLog)
        {
            throw new NotImplementedException("incremental checkpointing is not implemented");
        }

        byte[] ICheckpointManager.GetIndexCheckpointMetadata(Guid indexToken)
        {
            try
            {
                this.StorageTracer?.FasterStorageProgress($"StorageOpCalled ICheckpointManager.GetIndexCheckpointMetadata, indexToken={indexToken}");
                var partDir = this.blockBlobPartitionDirectory;
                var metaFileBlob = partDir.GetBlockBlobClient(this.GetIndexCheckpointMetaBlobName(indexToken));
                byte[] result = null;

                this.PerformWithRetries(
                   false,
                   "BlockBlobClient.OpenRead",
                   "ReadIndexCheckpointMetadata",
                   "",
                   metaFileBlob.Name,
                   1000,
                   true,
                   (numAttempts) =>
                   {
                       var client = metaFileBlob.WithRetries;
                       using var blobstream = client.OpenRead();
                       using var reader = new BinaryReader(blobstream);
                       var len = reader.ReadInt32();
                       result = reader.ReadBytes(len);
                       return (len + 4, true);
                   });

                this.StorageTracer?.FasterStorageProgress($"StorageOpReturned ICheckpointManager.GetIndexCheckpointMetadata {result?.Length ?? null} bytes, target={metaFileBlob.Name}");
                return result;
            }
            catch
            {
                this.StorageTracer?.FasterStorageProgress($"StorageOpReturned ICheckpointManager.GetIndexCheckpointMetadata failed");
                throw;
            }
        }

        byte[] ICheckpointManager.GetLogCheckpointMetadata(Guid logToken, DeltaLog deltaLog, bool scanDelta, long recoverTo)
        {
            try
            {
                this.StorageTracer?.FasterStorageProgress($"StorageOpCalled ICheckpointManager.GetLogCheckpointMetadata, logToken={logToken}");
                var partDir = this.blockBlobPartitionDirectory;
                var metaFileBlob = partDir.GetBlockBlobClient(this.GetHybridLogCheckpointMetaBlobName(logToken));
                byte[] result = null;

                this.PerformWithRetries(
                    false,
                    "BlockBlobClient.OpenRead",
                    "ReadLogCheckpointMetadata",
                    "",
                    metaFileBlob.Name,
                    1000,
                    true,
                    (numAttempts) =>
                    {
                        var client = metaFileBlob.WithRetries;
                        using var blobstream = client.OpenRead();
                        using var reader = new BinaryReader(blobstream);
                        var len = reader.ReadInt32();
                        result = reader.ReadBytes(len);
                        return (len + 4, true);
                    });

                this.StorageTracer?.FasterStorageProgress($"StorageOpReturned ICheckpointManager.GetLogCheckpointMetadata {result?.Length ?? null} bytes, target={metaFileBlob.Name}");
                return result;
            }
            catch
            {
                this.StorageTracer?.FasterStorageProgress($"StorageOpReturned ICheckpointManager.GetLogCheckpointMetadata failed");
                throw;
            }
        }

        void GetPartitionDirectories(string path, out BlobUtilsV12.BlobDirectory blockBlobDir, out BlobUtilsV12.BlobDirectory pageBlobDir)
        {
            var blockPartDir = this.blockBlobPartitionDirectory;
            blockBlobDir = blockPartDir.GetSubDirectory(path);
            var pagePartDir = this.pageBlobPartitionDirectory;
            pageBlobDir = pagePartDir.GetSubDirectory(path);
        }

        IDevice ICheckpointManager.GetIndexDevice(Guid indexToken)
        {
            try 
            {
                this.StorageTracer?.FasterStorageProgress($"StorageOpCalled ICheckpointManager.GetIndexDevice, indexToken={indexToken}");
                var (path, blobName) = this.GetPrimaryHashTableBlobName(indexToken);
                this.GetPartitionDirectories(path, out var blockBlobDir, out var pageBlobDir);
                var device = new AzureStorageDevice(blobName, blockBlobDir, pageBlobDir, this, false); // we don't need a lease since the token provides isolation
                device.StartAsync().Wait();
                this.StorageTracer?.FasterStorageProgress($"StorageOpReturned ICheckpointManager.GetIndexDevice, target={blockBlobDir}{blobName}");
                return device;
            }
            catch
            {
                this.StorageTracer?.FasterStorageProgress($"StorageOpReturned ICheckpointManager.GetIndexDevice failed");
                throw;
            }
        }

        IDevice ICheckpointManager.GetSnapshotLogDevice(Guid token)
        {
            try
            {
                this.StorageTracer?.FasterStorageProgress($"StorageOpCalled ICheckpointManager.GetSnapshotLogDevice, token={token}");
                var (path, blobName) = this.GetLogSnapshotBlobName(token);
                this.GetPartitionDirectories(path, out var blockBlobDir, out var pageBlobDir);
                var device = new AzureStorageDevice(blobName, blockBlobDir, pageBlobDir, this, false); // we don't need a lease since the token provides isolation
                device.StartAsync().Wait();
                this.StorageTracer?.FasterStorageProgress($"StorageOpReturned ICheckpointManager.GetSnapshotLogDevice, target={blockBlobDir}{blobName}");
                return device;
            }
            catch
            {
                this.StorageTracer?.FasterStorageProgress($"StorageOpReturned ICheckpointManager.GetSnapshotLogDevice failed");
                throw;
            }
        }

        IDevice ICheckpointManager.GetSnapshotObjectLogDevice(Guid token)
        {
            try
            {
                this.StorageTracer?.FasterStorageProgress($"StorageOpCalled ICheckpointManager.GetSnapshotObjectLogDevice, token={token}");
                var (path, blobName) = this.GetObjectLogSnapshotBlobName(token);
                this.GetPartitionDirectories(path, out var blockBlobDir, out var pageBlobDir);
                var device = new AzureStorageDevice(blobName, blockBlobDir, pageBlobDir, this, false); // we don't need a lease since the token provides isolation
                device.StartAsync().Wait();
                this.StorageTracer?.FasterStorageProgress($"StorageOpReturned ICheckpointManager.GetSnapshotObjectLogDevice, target={blockBlobDir}{blobName}");
                return device;
            }
            catch
            {
                this.StorageTracer?.FasterStorageProgress($"StorageOpReturned ICheckpointManager.GetSnapshotObjectLogDevice failed");
                throw;
            }
        }

        IDevice ICheckpointManager.GetDeltaLogDevice(Guid token)
        {
            try
            {
                this.StorageTracer?.FasterStorageProgress($"StorageOpCalled ICheckpointManager.GetDeltaLogDevice on, token={token}");
                var (path, blobName) = this.GetDeltaLogSnapshotBlobName(token);
                this.GetPartitionDirectories(path, out var blockBlobDir, out var pageBlobDir);
                var device = new AzureStorageDevice(blobName, blockBlobDir, pageBlobDir, this, false); // we don't need a lease since the token provides isolation
                device.StartAsync().Wait();
                this.StorageTracer?.FasterStorageProgress($"StorageOpReturned ICheckpointManager.GetDeltaLogDevice, target={blockBlobDir}{blobName}");
                return device;
            }
            catch
            {
                this.StorageTracer?.FasterStorageProgress($"StorageOpReturned ICheckpointManager.GetDeltaLogDevice failed");
                throw;
            }
        }

        #endregion

        internal async Task PersistSingletonsAsync(byte[] singletons, Guid guid)
        {
            if (this.UseLocalFiles)
            {
                var path = Path.Combine(this.LocalCheckpointDirectoryPath, this.GetSingletonsSnapshotBlobName(guid));
                using var filestream = File.OpenWrite(path);
                await filestream.WriteAsync(singletons, 0, singletons.Length);
                await filestream.FlushAsync();
            }
            else
            {
                var singletonsBlob = this.blockBlobPartitionDirectory.GetBlockBlobClient(this.GetSingletonsSnapshotBlobName(guid));
                await this.PerformWithRetriesAsync(
                   BlobManager.AsynchronousStorageWriteMaxConcurrency,
                   false,
                   "BlockBlobClient.UploadAsync",
                   "WriteSingletons",
                   "",
                   singletonsBlob.Name,
                   1000 + singletons.Length / 5000,
                   false,
                   async (numAttempts) =>
                   {
                       var client = singletonsBlob.WithRetries;
                       await client.UploadAsync(
                           new MemoryStream(singletons),
                           new BlobUploadOptions(),
                           this.PartitionErrorHandler.Token);
                       return singletons.Length;
                   });
            }
        }

        internal async Task<Stream> RecoverSingletonsAsync()
        {
            if (this.UseLocalFiles)
            {
                var path = Path.Combine(this.LocalCheckpointDirectoryPath, this.GetSingletonsSnapshotBlobName(this.CheckpointInfo.LogToken));
                var stream = File.OpenRead(path);
                return stream;
            }
            else
            {
                var singletonsBlob = this.blockBlobPartitionDirectory.GetBlockBlobClient(this.GetSingletonsSnapshotBlobName(this.CheckpointInfo.LogToken));
                var stream = new MemoryStream();
                await this.PerformWithRetriesAsync(
                    BlobManager.AsynchronousStorageReadMaxConcurrency,
                    true,
                    "BlobBaseClient.DownloadToAsync",
                    "ReadSingletons",
                    "",
                    singletonsBlob.Name,
                    20000,
                    true,
                    async (numAttempts) =>
                    {

                        var client = singletonsBlob.WithRetries;
                        var memoryStream = new MemoryStream();
                        await client.DownloadToAsync(stream);
                        return stream.Position;
                    });

                stream.Seek(0, SeekOrigin.Begin);
                return stream;
            }
        }

        internal async Task FinalizeCheckpointCompletedAsync()
        {
            var jsonText = JsonConvert.SerializeObject(this.CheckpointInfo, Formatting.Indented);
            if (this.UseLocalFiles)
            {
                File.WriteAllText(Path.Combine(this.LocalCheckpointDirectoryPath, this.GetCheckpointCompletedBlobName()), jsonText);
            }
            else
            {
                var checkpointCompletedBlob = this.blockBlobPartitionDirectory.GetBlockBlobClient(this.GetCheckpointCompletedBlobName());
                await this.PerformWithRetriesAsync(
                    BlobManager.AsynchronousStorageWriteMaxConcurrency,
                    true,
                    "BlockBlobClient.UploadAsync",
                    "WriteCheckpointMetadata",
                    "",
                    checkpointCompletedBlob.Name,
                    1000,
                    true,
                    async (numAttempts) =>
                    {
                        var client = numAttempts > 1 ? checkpointCompletedBlob.Default : checkpointCompletedBlob.Aggressive;

                        var azureResponse = await client.UploadAsync(
                            new MemoryStream(Encoding.UTF8.GetBytes(jsonText)),
                            new BlobUploadOptions()
                            {
                                Conditions = this.CheckpointInfoETag.HasValue ?
                                    new BlobRequestConditions() { IfMatch = this.CheckpointInfoETag.Value }
                                    : new BlobRequestConditions() { IfNoneMatch = Azure.ETag.All },
                                HttpHeaders = new BlobHttpHeaders() { ContentType = "application/json" },
                            },
                            this.PartitionErrorHandler.Token);

                        this.CheckpointInfoETag = azureResponse.Value.ETag;

                        return jsonText.Length;
                    },
                    async () =>
                    {
                        var response = await checkpointCompletedBlob.Default.GetPropertiesAsync();
                        this.CheckpointInfoETag = response.Value.ETag;
                    });
            }
        }
    }
}
