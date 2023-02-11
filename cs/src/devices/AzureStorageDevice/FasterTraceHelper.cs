// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace FASTER.devices
{
    using System;
    using Microsoft.Extensions.Logging;

    /// <summary>
    /// FASTER trace helper
    /// </summary>
    public class FasterTraceHelper
    {
        readonly ILogger logger;
        readonly LogLevel logLevelLimit;
        readonly ILogger performanceLogger;

        /// <summary>
        /// Create a trace helper for FASTER
        /// </summary>
        /// <param name="logger"></param>
        /// <param name="logLevelLimit"></param>
        /// <param name="performanceLogger"></param>
        public FasterTraceHelper(ILogger logger, LogLevel logLevelLimit, ILogger performanceLogger)
        {
            this.logger = logger;
            this.logLevelLimit = logLevelLimit;
            this.performanceLogger = performanceLogger;
        }

        /// <summary>
        /// Is tracing at most detailed level
        /// </summary>
        public bool IsTracingAtMostDetailedLevel => this.logLevelLimit == LogLevel.Trace;


        /// <summary>
        /// Perf warning
        /// </summary>
        /// <param name="details"></param>
        public void FasterPerfWarning(string details)
        {
            if (this.logLevelLimit <= LogLevel.Warning)
            {
                this.performanceLogger?.LogWarning("Performance issue detected: {details}", details);
            }
        }

        /// <summary>
        /// Storage progress
        /// </summary>
        /// <param name="details"></param>
        public void FasterStorageProgress(string details)
        {
            if (this.logLevelLimit <= LogLevel.Trace)
            {
                this.logger?.LogTrace("{details}", details);
            }
        }

        /// <summary>
        /// Azure storage access completed
        /// </summary>
        public void FasterAzureStorageAccessCompleted(string intent, long size, string operation, string target, double latency, int attempt)
        {
            if (this.logLevelLimit <= LogLevel.Debug)
            {
                this.logger?.LogDebug("storage access completed intent={intent} size={size} operation={operation} target={target} latency={latency} attempt={attempt}", 
                    intent, size, operation, target, latency, attempt);
            }
        }


        // ----- lease management events


        /// <summary>
        /// Lease acquired
        /// </summary>
        public void LeaseAcquired()
        {
            if (this.logLevelLimit <= LogLevel.Information)
            {
                this.logger?.LogInformation("PartitionLease acquired");
            }
        }

        /// <summary>
        /// Lease renewed
        /// </summary>
        public void LeaseRenewed(double elapsedSeconds, double timing)
        {
            if (this.logLevelLimit <= LogLevel.Debug)
            {
                this.logger?.LogDebug("PartitionLease renewed after {elapsedSeconds:F2}s timing={timing:F2}s", elapsedSeconds, timing);
            }
        }

        /// <summary>
        /// Lease released
        /// </summary>
        public void LeaseReleased(double elapsedSeconds)
        {
            if (this.logLevelLimit <= LogLevel.Information)
            {
                this.logger?.LogInformation("PartitionLease released after {elapsedSeconds:F2}s", elapsedSeconds);
            }
        }

        /// <summary>
        /// Lease lost
        /// </summary>
        public void LeaseLost(double elapsedSeconds, string operation)
        {
            if (this.logLevelLimit <= LogLevel.Warning)
            {
                this.logger?.LogWarning("PartitionLease lost after {elapsedSeconds:F2}s in {operation}", elapsedSeconds, operation);
            }
        }

        /// <summary>
        /// Lease progress
        /// </summary>
        public void LeaseProgress(string operation)
        {
            if (this.logLevelLimit <= LogLevel.Debug)
            {
                this.logger?.LogDebug("PartitionLease progress: {operation}", operation);
            }
        }
    }
}
