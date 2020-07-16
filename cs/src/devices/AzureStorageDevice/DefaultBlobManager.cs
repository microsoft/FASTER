// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Blob;
using Microsoft.Azure.Storage.Blob.Protocol;
using Microsoft.Azure.Storage.RetryPolicies;
using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace FASTER.devices
{
    /// <summary>
    /// Default blob manager with lease support
    /// </summary>
    public class DefaultBlobManager : IBlobManager
    {
        private readonly CancellationTokenSource cts;
        private readonly bool underLease;
        private string leaseId;

        private readonly TimeSpan LeaseDuration = TimeSpan.FromSeconds(45); // max time the lease stays after unclean shutdown
        private readonly TimeSpan LeaseRenewal = TimeSpan.FromSeconds(30); // how often we renew the lease
        private readonly TimeSpan LeaseSafetyBuffer = TimeSpan.FromSeconds(10); // how much time we want left on the lease before issuing a protected access
        private volatile Stopwatch leaseTimer;
        private Task LeaseMaintenanceLoopTask = Task.CompletedTask;
        private volatile Task NextLeaseRenewalTask = Task.CompletedTask;

        private readonly CloudBlobDirectory leaseDirectory;
        private const string LeaseBlobName = "lease";
        private CloudBlockBlob leaseBlob;

        /// <summary>
        /// Create instance of blob manager
        /// </summary>
        /// <param name="underLease">Should we use blob leases</param>
        /// <param name="leaseDirectory">Directory to store lease file</param>
        public DefaultBlobManager(bool underLease, CloudBlobDirectory leaseDirectory = null)
        {
            this.underLease = underLease;
            this.leaseDirectory = leaseDirectory;
            this.cts = new CancellationTokenSource();

            if (underLease)
            {
                // Start lease maintenance loop
                var _ = StartAsync();
            }
        }

        /// <summary>
        /// Start blob manager and acquire lease
        /// </summary>
        /// <returns></returns>
        private async Task StartAsync()
        {
            this.leaseBlob = this.leaseDirectory.GetBlockBlobReference(LeaseBlobName);
            await this.AcquireOwnership().ConfigureAwait(false);
        }

        /// <summary>
        /// Clean shutdown, wait for everything, then terminate
        /// </summary>
        /// <returns></returns>
        public async Task StopAsync()
        {
            this.cts.Cancel(); // has no effect if already cancelled
            await this.LeaseMaintenanceLoopTask.ConfigureAwait(false); // wait for loop to terminate cleanly
        }

        /// <inheritdoc />
        public CancellationToken CancellationToken => cts.Token;

        /// <inheritdoc />
        public ValueTask ConfirmLeaseAsync()
        {
            if (!underLease)
                return new ValueTask();

            if (this.leaseTimer?.Elapsed < this.LeaseDuration - this.LeaseSafetyBuffer)
            {
                return default;
            }
            Debug.WriteLine("Access is waiting for fresh lease");
            return new ValueTask(this.NextLeaseRenewalTask);
        }

        /// <inheritdoc />
        public BlobRequestOptions GetBlobRequestOptions()
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
            HandleError(where, $"Encountered storage exception for blob {blobName}", e, isFatal);
        }

        private async Task AcquireOwnership()
        {
            var newLeaseTimer = new Stopwatch();

            while (true)
            {
                CancellationToken.ThrowIfCancellationRequested();

                try
                {
                    newLeaseTimer.Restart();

                    this.leaseId = await this.leaseBlob.AcquireLeaseAsync(LeaseDuration, null,
                        accessCondition: null, options: this.GetBlobRequestOptions(), operationContext: null, this.CancellationToken).ConfigureAwait(false);

                    this.leaseTimer = newLeaseTimer;
                    this.LeaseMaintenanceLoopTask = Task.Run(() => this.MaintenanceLoopAsync());
                    return;
                }
                catch (StorageException ex) when (LeaseConflictOrExpired(ex))
                {
                    Debug.WriteLine("Waiting for lease");

                    // the previous owner has not released the lease yet, 
                    // try again until it becomes available, should be relatively soon
                    // as the transport layer is supposed to shut down the previous owner when starting this
                    await Task.Delay(TimeSpan.FromSeconds(1), this.CancellationToken).ConfigureAwait(false);

                    continue;
                }
                catch (StorageException ex) when (BlobDoesNotExist(ex))
                {
                    try
                    {
                        // Create blob with empty content, then try again
                        Debug.WriteLine("Creating commit blob");
                        await this.leaseBlob.UploadFromByteArrayAsync(Array.Empty<byte>(), 0, 0).ConfigureAwait(false);
                        continue;
                    }
                    catch (StorageException ex2) when (LeaseConflictOrExpired(ex2))
                    {
                        // creation race, try from top
                        Debug.WriteLine("Creation race observed, retrying");
                        continue;
                    }
                }
                catch (Exception e)
                {
                    HandleError(nameof(AcquireOwnership), "Could not acquire lease", e, true);
                    throw;
                }
            }
        }

        private void HandleError(string context, string message, Exception exception, bool terminate)
        {
            Debug.WriteLine(context + ": " + message + ", " + exception.ToString());

            // terminate in response to the error
            if (terminate && !cts.IsCancellationRequested)
            {
                Terminate();
            }
        }

        private async Task RenewLeaseTask()
        {
            try
            {
                await Task.Delay(this.LeaseRenewal, this.CancellationToken).ConfigureAwait(false);
                AccessCondition acc = new AccessCondition() { LeaseId = this.leaseId };
                var nextLeaseTimer = new Stopwatch();
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

        private async Task MaintenanceLoopAsync()
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
            catch (StorageException ex) when (LeaseConflict(ex))
            {
                // We lost the lease to someone else. Terminate ownership immediately.
                HandleError(nameof(MaintenanceLoopAsync), "Lost lease", ex, true);
            }
            catch (Exception e) when (!IsFatal(e))
            {
                HandleError(nameof(MaintenanceLoopAsync), "Could not maintain lease", e, true);
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
                        options: this.GetBlobRequestOptions(), operationContext: null, cancellationToken: this.CancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    // it's o.k. if termination is triggered while waiting
                }
                catch (StorageException e) when (e.InnerException != null && e.InnerException is OperationCanceledException)
                {
                    // it's o.k. if termination is triggered while we are releasing the lease
                }
                catch (Exception)
                {
                    Debug.WriteLine("could not release lease for " + this.leaseBlob.Name);
                    // swallow exceptions when releasing a lease
                }
            }

            Terminate();

            Debug.WriteLine("Blob manager stopped");
        }

        private void Terminate()
        {
            try
            {
                cts.Cancel();
            }
            catch (AggregateException aggregate)
            {
                foreach (var e in aggregate.InnerExceptions)
                {
                    HandleError("Terminate", "Encountered exeption while canceling token", e, false);
                }
            }
            catch (Exception e)
            {
                HandleError("Terminate", "Encountered exeption while canceling token", e, false);
            }
        }

        private static bool LeaseConflict(StorageException e)
        {
            return (e.RequestInformation.HttpStatusCode == 409);
        }

        private static bool LeaseConflictOrExpired(StorageException e)
        {
            return (e.RequestInformation.HttpStatusCode == 409) || (e.RequestInformation.HttpStatusCode == 412);
        }

        private static bool BlobDoesNotExist(StorageException e)
        {
            var information = e.RequestInformation.ExtendedErrorInformation;
            return (e.RequestInformation.HttpStatusCode == 404) && (information.ErrorCode.Equals(BlobErrorCodeStrings.BlobNotFound));
        }

        private static bool IsFatal(Exception exception)
        {
            if (exception is OutOfMemoryException || exception is StackOverflowException)
            {
                return true;
            }
            return false;
        }
    }
}