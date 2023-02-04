// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace FASTER.devices
{
    using FASTER.core;
    using Microsoft.Extensions.Logging;
    using System;
    using System.Diagnostics;
    using System.IO;
    using System.Threading;
    using System.Threading.Tasks;
    using Azure.Storage.Blobs.Specialized;

    /// <summary>
    /// Provides management of blobs and blob names associated with a partition, and logic for partition lease maintenance and termination.
    /// </summary>
    partial class BlobManager
    {
        readonly uint partitionId;
        readonly CancellationTokenSource shutDownOrTermination;
        readonly string taskHubPrefix;

        BlobUtilsV12.ServiceClients pageBlobAccount;
        BlobUtilsV12.ContainerClients pageBlobContainer;
        BlobUtilsV12.BlockBlobClients eventLogCommitBlob;
        BlobLeaseClient leaseClient;

        BlobUtilsV12.BlobDirectory pageBlobDirectory;
        BlobUtilsV12.BlobDirectory blockBlobPartitionDirectory;

        readonly TimeSpan LeaseDuration = TimeSpan.FromSeconds(45); // max time the lease stays after unclean shutdown
        readonly TimeSpan LeaseRenewal = TimeSpan.FromSeconds(30); // how often we renew the lease
        readonly TimeSpan LeaseSafetyBuffer = TimeSpan.FromSeconds(10); // how much time we want left on the lease before issuing a protected access

        internal FasterTraceHelper TraceHelper { get; private set; }
        internal FasterTraceHelper StorageTracer => this.TraceHelper.IsTracingAtMostDetailedLevel ? this.TraceHelper : null;

        public DateTime IncarnationTimestamp { get; private set; }

        internal BlobUtilsV12.ContainerClients PageBlobContainer => this.pageBlobContainer;

        public IPartitionErrorHandler PartitionErrorHandler { get; private set; }

        internal static SemaphoreSlim AsynchronousStorageReadMaxConcurrency = new SemaphoreSlim(Math.Min(100, Environment.ProcessorCount * 10));
        internal static SemaphoreSlim AsynchronousStorageWriteMaxConcurrency = new SemaphoreSlim(Math.Min(50, Environment.ProcessorCount * 7));

        internal volatile int LeaseUsers;

        volatile Stopwatch leaseTimer;

        internal const long HashTableSize = 1L << 14; // 16 k buckets, 1 MB
        internal const long HashTableSizeBytes = HashTableSize * 64;

        public const int MaxRetries = 10;

        public static TimeSpan GetDelayBetweenRetries(int numAttempts)
            => TimeSpan.FromSeconds(Math.Pow(2, (numAttempts - 1)));

        /// <summary>
        /// Create a blob manager.
        /// </summary>
        /// <param name="leaseBlobName"></param>
        /// <param name="pageBlobDirectory"></param>
        /// <param name="underLease"></param>
        /// <param name="logger">A logger for logging</param>
        /// <param name="performanceLogger"></param>
        /// <param name="logLevelLimit">A limit on log event level emitted</param>
        /// <param name="errorHandler">A handler for errors encountered in this partition</param>
        internal BlobManager(
            string leaseBlobName,
            BlobUtilsV12.BlobDirectory pageBlobDirectory,
            bool underLease,
            ILogger logger,
            ILogger performanceLogger,
            LogLevel logLevelLimit,
            IPartitionErrorHandler errorHandler)
        {
            if (leaseBlobName != null) LeaseBlobName = leaseBlobName;
            this.pageBlobDirectory = pageBlobDirectory;
            this.UseLocalFiles = false;
            this.TraceHelper = new FasterTraceHelper(logger, logLevelLimit, performanceLogger);
            this.PartitionErrorHandler = errorHandler;
            this.shutDownOrTermination = CancellationTokenSource.CreateLinkedTokenSource(errorHandler.Token);
        }

        // For testing and debugging with local files
        bool UseLocalFiles { get; }
        string LeaseBlobName = "commit-lease";
        Task LeaseMaintenanceLoopTask = Task.CompletedTask;
        volatile Task NextLeaseRenewalTask = Task.CompletedTask;

        public async Task StartAsync()
        {
            this.eventLogCommitBlob = this.pageBlobDirectory.GetBlockBlobClient(LeaseBlobName);
            this.leaseClient = this.eventLogCommitBlob.WithRetries.GetBlobLeaseClient();
            await this.AcquireOwnership();
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
            var newLeaseTimer = new Stopwatch();
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
    }
}
