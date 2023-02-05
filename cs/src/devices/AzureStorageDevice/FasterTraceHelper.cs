// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace FASTER.devices
{
    using System;
    using Microsoft.Extensions.Logging;

    class FasterTraceHelper
    {
        readonly ILogger logger;
        readonly LogLevel logLevelLimit;
        readonly ILogger performanceLogger;

        public FasterTraceHelper(ILogger logger, LogLevel logLevelLimit, ILogger performanceLogger)
        {
            this.logger = logger;
            this.logLevelLimit = logLevelLimit;
            this.performanceLogger = performanceLogger;
        }

        public bool IsTracingAtMostDetailedLevel => this.logLevelLimit == LogLevel.Trace;

        // ----- faster storage layer events

        public void FasterStoreCreated(long inputQueuePosition, long latencyMs)
        {
            if (this.logLevelLimit <= LogLevel.Information)
            {
                this.logger?.LogInformation("Created Store, inputQueuePosition={inputQueuePosition} latencyMs={latencyMs}", inputQueuePosition, latencyMs);
            }
        }
        public void FasterCheckpointStarted(Guid checkpointId, string details, string storeStats, long commitLogPosition, long inputQueuePosition)
        {
            if (this.logLevelLimit <= LogLevel.Information)
            {
                this.logger?.LogInformation("Started Checkpoint {checkpointId}, details={details}, storeStats={storeStats}, commitLogPosition={commitLogPosition} inputQueuePosition={inputQueuePosition}", checkpointId, details, storeStats, commitLogPosition, inputQueuePosition);
            }
        }

        public void FasterCheckpointPersisted(Guid checkpointId, string details, long commitLogPosition, long inputQueuePosition, long latencyMs)
        {
            if (this.logLevelLimit <= LogLevel.Information)
            {
                this.logger?.LogInformation("Persisted Checkpoint {checkpointId}, details={details}, commitLogPosition={commitLogPosition} inputQueuePosition={inputQueuePosition} latencyMs={latencyMs}", checkpointId, details, commitLogPosition, inputQueuePosition, latencyMs);
            }

            if (latencyMs > 10000)
            {
                this.FasterPerfWarning($"Persisting the checkpoint {checkpointId} took {(double)latencyMs / 1000}s, which is excessive; checkpointId={checkpointId} commitLogPosition={commitLogPosition} inputQueuePosition={inputQueuePosition}");
            }
        }

        public void FasterLogPersisted(long commitLogPosition, long numberEvents, long sizeInBytes, long latencyMs)
        {
            if (this.logLevelLimit <= LogLevel.Debug)
            {
                this.logger?.LogDebug("Persisted Log, commitLogPosition={commitLogPosition} numberEvents={numberEvents} sizeInBytes={sizeInBytes} latencyMs={latencyMs}", commitLogPosition, numberEvents, sizeInBytes, latencyMs);
            }

            if (latencyMs > 10000)
            {
                this.FasterPerfWarning($"Persisting the log took {(double)latencyMs / 1000}s, which is excessive; commitLogPosition={commitLogPosition} numberEvents={numberEvents} sizeInBytes={sizeInBytes}");
            }
        }

        public void FasterPerfWarning(string details)
        {
            if (this.logLevelLimit <= LogLevel.Warning)
            {
                this.performanceLogger?.LogWarning("Performance issue detected: {details}", details);
            }
        }

        public void FasterCheckpointLoaded(long commitLogPosition, long inputQueuePosition, string storeStats, long latencyMs)
        {
            if (this.logLevelLimit <= LogLevel.Information)
            {
                this.logger?.LogInformation("Loaded Checkpoint, commitLogPosition={commitLogPosition} inputQueuePosition={inputQueuePosition}  storeStats={storeStats} latencyMs={latencyMs}", commitLogPosition, inputQueuePosition, storeStats, latencyMs);
            }
        }

        public void FasterLogReplayed(long commitLogPosition, long inputQueuePosition, long numberEvents, long sizeInBytes, string storeStats, long latencyMs)
        {
            if (this.logLevelLimit <= LogLevel.Information)
            {
                this.logger?.LogInformation("Replayed CommitLog, commitLogPosition={commitLogPosition} inputQueuePosition={inputQueuePosition} numberEvents={numberEvents} sizeInBytes={sizeInBytes} storeStats={storeStats} latencyMs={latencyMs}", commitLogPosition, inputQueuePosition, numberEvents, sizeInBytes, storeStats, latencyMs);
            }
        }

        public void FasterStorageError(string context, Exception exception)
        {
            if (this.logLevelLimit <= LogLevel.Error)
            {
                this.logger?.LogError("!!! Faster Storage Error : {context} : {exception}", context, exception);
            }
        }

        public void FasterCacheSizeMeasured(int numPages, long numRecords, long sizeInBytes, long gcMemory, long processMemory, long discrepancy, double elapsedMs)
        {
            if (this.logLevelLimit <= LogLevel.Information)
            {
                this.logger?.LogInformation("Measured CacheSize numPages={numPages} numRecords={numRecords} sizeInBytes={sizeInBytes} gcMemory={gcMemory} processMemory={processMemory} discrepancy={discrepancy} elapsedMs={elapsedMs:F2}", numPages, numRecords, sizeInBytes, gcMemory, processMemory, discrepancy, elapsedMs);
            }
        }

        public void FasterProgress(string details)
        {
            if (this.logLevelLimit <= LogLevel.Debug)
            {
                this.logger?.LogDebug("{details}", details);
            }
        }

        public void FasterStorageProgress(string details)
        {
            if (this.logLevelLimit <= LogLevel.Trace)
            {
                this.logger?.LogTrace("{details}", details);
            }
        }

        public void FasterAzureStorageAccessCompleted(string intent, long size, string operation, string target, double latency, int attempt)
        {
            if (this.logLevelLimit <= LogLevel.Debug)
            {
                this.logger?.LogDebug("storage access completed intent={intent} size={size} operation={operation} target={target} latency={latency} attempt={attempt}", 
                    intent, size, operation, target, latency, attempt);
            }
        }

        public enum CompactionProgress { Skipped, Started, Completed };

        public void FasterCompactionProgress(CompactionProgress progress, string operation, long begin, long safeReadOnly, long tail, long minimalSize, long compactionAreaSize, double elapsedMs)
        {
            if (this.logLevelLimit <= LogLevel.Information)
            {
                this.logger?.LogInformation("Compaction {progress} operation={operation} begin={begin} safeReadOnly={safeReadOnly} tail={tail} minimalSize={minimalSize} compactionAreaSize={compactionAreaSize} elapsedMs={elapsedMs}", progress, operation, begin, safeReadOnly, tail, minimalSize, compactionAreaSize, elapsedMs);
            } 
        }

        // ----- lease management events

        public void LeaseAcquired()
        {
            if (this.logLevelLimit <= LogLevel.Information)
            {
                this.logger?.LogInformation("PartitionLease acquired");
            }
        }

        public void LeaseRenewed(double elapsedSeconds, double timing)
        {
            if (this.logLevelLimit <= LogLevel.Debug)
            {
                this.logger?.LogDebug("PartitionLease renewed after {elapsedSeconds:F2}s timing={timing:F2}s", elapsedSeconds, timing);
            }
        }

        public void LeaseReleased(double elapsedSeconds)
        {
            if (this.logLevelLimit <= LogLevel.Information)
            {
                this.logger?.LogInformation("PartitionLease released after {elapsedSeconds:F2}s", elapsedSeconds);
            }
        }

        public void LeaseLost(double elapsedSeconds, string operation)
        {
            if (this.logLevelLimit <= LogLevel.Warning)
            {
                this.logger?.LogWarning("PartitionLease lost after {elapsedSeconds:F2}s in {operation}", elapsedSeconds, operation);
            }
        }

        public void LeaseProgress(string operation)
        {
            if (this.logLevelLimit <= LogLevel.Debug)
            {
                this.logger?.LogDebug("PartitionLease progress: {operation}", operation);
            }
        }
    }
}
