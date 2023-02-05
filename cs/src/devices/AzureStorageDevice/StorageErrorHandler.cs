// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace FASTER.devices
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Extensions.Logging;

    // For indicating and initiating termination, and for tracing errors and warnings relating to a partition.
    // Is is basically a wrapper around CancellationTokenSource with features for diagnostics.
    class StorageErrorHandler : IStorageErrorHandler
    {
        readonly CancellationTokenSource cts = new CancellationTokenSource();
        readonly ILogger logger;
        readonly LogLevel logLevelLimit;
        readonly string account;
        readonly string taskHub;
        readonly TaskCompletionSource<object> shutdownComplete;

        public event Action OnShutdown;

        public CancellationToken Token
        {
            get
            {
                try
                {
                    return this.cts.Token;
                }
                catch (ObjectDisposedException)
                {
                    return new CancellationToken(true);
                }
            }
        }

        public bool IsTerminated => this.terminationStatus != NotTerminated;

        public bool NormalTermination =>  this.terminationStatus == TerminatedNormally;

        volatile int terminationStatus = NotTerminated;
        const int NotTerminated = 0;
        const int TerminatedWithError = 1;
        const int TerminatedNormally = 2;

        public StorageErrorHandler(ILogger logger, LogLevel logLevelLimit, string storageAccountName, string taskHubName)
        {
            this.cts = new CancellationTokenSource();
            this.logger = logger;
            this.logLevelLimit = logLevelLimit;
            this.account = storageAccountName;
            this.taskHub = taskHubName;
            this.shutdownComplete = new TaskCompletionSource<object>();
        }
     
        public void HandleError(string context, string message, Exception exception, bool terminatePartition, bool isWarning)
        {
            this.TraceError(isWarning, context, message, exception, terminatePartition);

            // terminate this partition in response to the error

            if (terminatePartition && this.terminationStatus == NotTerminated)
            {
                if (Interlocked.CompareExchange(ref this.terminationStatus, TerminatedWithError, NotTerminated) == NotTerminated)
                {
                    this.Terminate();
                }
            }
        }

        public void TerminateNormally()
        {
            if (Interlocked.CompareExchange(ref this.terminationStatus, TerminatedNormally, NotTerminated) == NotTerminated)
            {
                this.Terminate();
            }
        }

        void TraceError(bool isWarning, string context, string message, Exception exception, bool terminatePartition)
        {
            var logLevel = isWarning ? LogLevel.Warning : LogLevel.Error;
            if (this.logLevelLimit <= logLevel)
            {
                // for warnings, do not print the entire exception message
                string details = exception == null ? string.Empty : (isWarning ? $"{exception.GetType().FullName}: {exception.Message}" : exception.ToString());
                
                this.logger?.Log(logLevel, "!!! {message} in {context}: {details} terminatePartition={terminatePartition}", message, context, details, terminatePartition);
            }
        }

        void Terminate()
        {
            try
            {
                this.logger?.LogDebug("Started PartitionCancellation");
                this.cts.Cancel();
                this.logger?.LogDebug("Completed PartitionCancellation");
            }
            catch (AggregateException aggregate)
            {
                foreach (var e in aggregate.InnerExceptions)
                {
                    this.HandleError("PartitionErrorHandler.Terminate", "Exception in PartitionCancellation", e, false, true);
                }
            }
            catch (Exception e)
            {
                this.HandleError("PartitionErrorHandler.Terminate", "Exception in PartitionCancellation", e, false, true);
            }

            // we use a dedicated shutdown thread to help debugging and to contain damage if there are hangs
            Thread shutdownThread = TrackedThreads.MakeTrackedThread(Shutdown, "PartitionShutdown");
            shutdownThread.Start();

            void Shutdown()
            {
                try
                {
                    this.logger?.LogDebug("Started PartitionShutdown");

                    if (this.OnShutdown != null)
                    {
                        this.OnShutdown();
                    }

                    this.cts.Dispose();

                    this.logger?.LogDebug("Completed PartitionShutdown");
                }
                catch (AggregateException aggregate)
                {
                    foreach (var e in aggregate.InnerExceptions)
                    {
                        this.HandleError("PartitionErrorHandler.Shutdown", "Exception in PartitionShutdown", e, false, true);
                    }
                }
                catch (Exception e)
                {
                    this.HandleError("PartitionErrorHandler.Shutdown", "Exception in PartitionShutdown", e, false, true);
                }

                this.shutdownComplete.TrySetResult(null);
            }
        }

        public async Task<bool> WaitForTermination(TimeSpan timeout)
        {
            Task timeoutTask = Task.Delay(timeout);
            var first = await Task.WhenAny(timeoutTask, this.shutdownComplete.Task);
            return first == this.shutdownComplete.Task;
        }
    }
}
