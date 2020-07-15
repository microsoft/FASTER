// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Blob;
using Microsoft.Azure.Storage.RetryPolicies;
using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace FASTER.devices
{
    /// <summary>
    /// Default blob manager
    /// </summary>
    public class DefaultBlobManager : IBlobManager
    {
        private string leaseId;

        private TimeSpan LeaseDuration = TimeSpan.FromSeconds(45); // max time the lease stays after unclean shutdown
        private TimeSpan LeaseRenewal = TimeSpan.FromSeconds(30); // how often we renew the lease
        private TimeSpan LeaseSafetyBuffer = TimeSpan.FromSeconds(10); // how much time we want left on the lease before issuing a protected access
        private volatile Stopwatch leaseTimer;
        private CloudBlockBlob leaseBlob;
        private Task LeaseMaintenanceLoopTask = Task.CompletedTask;
        private volatile Task NextLeaseRenewalTask = Task.CompletedTask;

        /// <inheritdoc />
        public CancellationToken CancellationToken => throw new NotImplementedException();

        /// <inheritdoc />
        public ValueTask ConfirmLeaseAsync()
        {
            if (this.leaseTimer?.Elapsed < this.LeaseDuration - this.LeaseSafetyBuffer)
            {
                return default;
            }
            Debug.WriteLine("Access is waiting for fresh lease");
            return new ValueTask(this.NextLeaseRenewalTask);
        }

        /// <inheritdoc />
        public BlobRequestOptions GetBlobRequestOptions(bool underLease)
        {
            if (underLease)
            {
                return new BlobRequestOptions()
                {
                    RetryPolicy = new LinearRetry(TimeSpan.FromSeconds(2), 2),
                    NetworkTimeout = TimeSpan.FromSeconds(50),
                };
            }
            else
            {
                return new BlobRequestOptions()
                {
                    RetryPolicy = new ExponentialRetry(TimeSpan.FromSeconds(4), 4),
                    NetworkTimeout = TimeSpan.FromSeconds(50),
                };
            }
        }

        /// <inheritdoc />
        public void HandleBlobError(string where, string message, string blobName, Exception e, bool isFatal)
        {
        }

        private async Task AcquireOwnership()
        {
            var newLeaseTimer = new System.Diagnostics.Stopwatch();

            while (true)
            {
                CancellationToken.ThrowIfCancellationRequested();

                try
                {
                    newLeaseTimer.Restart();

                    this.leaseId = await this.leaseBlob.AcquireLeaseAsync(LeaseDuration, null,
                        accessCondition: null, options: this.GetBlobRequestOptions(true), operationContext: null, this.CancellationToken).ConfigureAwait(false);

                    this.leaseTimer = newLeaseTimer;
                    this.LeaseMaintenanceLoopTask = Task.Run(() => this.MaintenanceLoopAsync());
                    return;
                }
                catch (StorageException ex) when (BlobUtils.LeaseConflictOrExpired(ex))
                {
                    Debug.WriteLine("Waiting for lease");

                    // the previous owner has not released the lease yet, 
                    // try again until it becomes available, should be relatively soon
                    // as the transport layer is supposed to shut down the previous owner when starting this
                    await Task.Delay(TimeSpan.FromSeconds(1), this.CancellationToken).ConfigureAwait(false);

                    continue;
                }
                catch (StorageException ex) when (BlobUtils.BlobDoesNotExist(ex))
                {
                    try
                    {
                        // Create blob with empty content, then try again
                        Debug.WriteLine("Creating commit blob");
                        await this.leaseBlob.UploadFromByteArrayAsync(Array.Empty<byte>(), 0, 0).ConfigureAwait(false);
                        continue;
                    }
                    catch (StorageException ex2) when (BlobUtils.LeaseConflictOrExpired(ex2))
                    {
                        // creation race, try from top
                        Debug.WriteLine("Creation race observed, retrying");
                        continue;
                    }
                }
                catch (Exception e)
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
                await Task.Delay(this.LeaseRenewal, this.shutDownOrTermination.Token).ConfigureAwait(false);
                AccessCondition acc = new AccessCondition() { LeaseId = this.leaseId };
                var nextLeaseTimer = new System.Diagnostics.Stopwatch();
                nextLeaseTimer.Start();
                await this.leaseBlob.RenewLeaseAsync(acc, this.CancellationToken).ConfigureAwait(false);
                this.leaseTimer = nextLeaseTimer;
            }
            catch (Exception)
            {
                Debug.WriteLine("Failed to renew lease");
                throw;
            }
        }

        public async Task MaintenanceLoopAsync()
        {
            try
            {
                while (true)
                {
                    // save the task so storage accesses can wait for it
                    this.NextLeaseRenewalTask = this.RenewLeaseTask();

                    // wait for successful renewal, or exit the loop as this throws
                    await this.NextLeaseRenewalTask.ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException)
            {
                // it's o.k. to cancel while waiting
                Debug.WriteLine("Lease renewal loop cleanly canceled");
            }
            catch (StorageException e) when (e.InnerException != null && e.InnerException is OperationCanceledException)
            {
                // it's o.k. to cancel a lease renewal
                Debug.WriteLine("Lease renewal storage operation canceled");
            }
            catch (StorageException ex) when (BlobUtils.LeaseConflict(ex))
            {
                // We lost the lease to someone else. Terminate ownership immediately.
                this.PartitionErrorHandler.HandleError(nameof(MaintenanceLoopAsync), "Lost partition lease", ex, true, true);
            }
            catch (Exception e) when (!Utils.IsFatal(e))
            {
                this.PartitionErrorHandler.HandleError(nameof(MaintenanceLoopAsync), "Could not maintain partition lease", e, true, false);
            }

            Debug.WriteLine("Exited lease maintenance loop");

            if (this.CancellationToken.IsCancellationRequested)
            {
                // this is an unclean shutdown, so we let the lease expire to protect straggling storage accesses
                Debug.WriteLine("Leaving lease to expire on its own");
            }
            else
            {
                try
                {
                    Debug.WriteLine("Releasing lease");

                    AccessCondition acc = new AccessCondition() { LeaseId = this.leaseId };

                    await this.leaseBlob.ReleaseLeaseAsync(accessCondition: acc,
                        options: this.GetBlobRequestOptions(true), operationContext: null, cancellationToken: this.CancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    // it's o.k. if termination is triggered while waiting
                }
                catch (StorageException e) when (e.InnerException != null && e.InnerException is OperationCanceledException)
                {
                    // it's o.k. if termination is triggered while we are releasing the lease
                }
                catch (Exception e)
                {
                    Debug.WriteLine("could not release lease for " + this.leaseBlob.Name);
                    // swallow exceptions when releasing a lease
                }
            }

            this.PartitionErrorHandler.TerminateNormally();

            this.TraceHelper.LeaseProgress("Blob manager stopped");
        }
    }
}