using System;
using System.Collections.Generic;
using System.Threading;
using FASTER.core;

namespace FASTER.test
{
    public class ErrorSimulationOptions
    {
        public double readTransientErrorRate;
        public double readPermanentErrorRate;
        public double writeTransientErrorRate;
        public double writePermanentErrorRate;
    }
    
    public class SimulatedFlakyDevice : StorageDeviceBase
    {
        private IDevice underlying;
        private ErrorSimulationOptions options;
        private ThreadLocal<Random> random;
        private List<long> permanentlyFailedRangesStart, permanentlyFailedRangesEnd;
        private SimpleVersionScheme versionScheme;

        public SimulatedFlakyDevice(IDevice underlying, ErrorSimulationOptions options) : base(underlying.FileName, underlying.SectorSize, underlying.Capacity)
        {
            this.underlying = underlying;
            this.options = options;
            permanentlyFailedRangesStart = new List<long>();
            permanentlyFailedRangesEnd = new List<long>();
            versionScheme = new SimpleVersionScheme();
            random = new ThreadLocal<Random>(() => new Random());
        }

        public override void RemoveSegmentAsync(int segment, AsyncCallback callback, IAsyncResult result)
        {
            underlying.RemoveSegmentAsync(segment, callback, result);
        }

        public override void WriteAsync(IntPtr sourceAddress, int segmentId, ulong destinationAddress, uint numBytesToWrite,
            DeviceIOCompletionCallback callback, object context)
        {
            var logicalDestStart = segmentId * underlying.SegmentSize + (long) destinationAddress;
            var logicalDestEnd = logicalDestStart + numBytesToWrite;
            versionScheme.Enter();
            if (permanentlyFailedRangesStart.Count != 0)
            {
                // First failed range that's smaller than requested range start
                var startIndex = permanentlyFailedRangesStart.BinarySearch(logicalDestStart);
                if (startIndex < 0) startIndex = ~startIndex - 1;
                
                // First failed range after requested range end
                var endIndex = permanentlyFailedRangesEnd.BinarySearch(logicalDestEnd);
                if (endIndex < 0) endIndex = ~endIndex;

                // Check if there are overlaps
                if (startIndex >= 0 && permanentlyFailedRangesEnd[startIndex] > logicalDestStart ||
                    endIndex < permanentlyFailedRangesStart.Count && permanentlyFailedRangesStart[endIndex] < logicalDestEnd)
                {
                    // If so, simulate a failure by calling callback with an error
                    callback(42, numBytesToWrite, context);
                    versionScheme.Leave();
                    return;
                }
            }
            // Otherwise, decide whether we need to introduce a failure
            if (random.Value.NextDouble() < options.writeTransientErrorRate)
            {
                callback(42, numBytesToWrite, context);
            }
            // decide whether failure should be in fact permanent. Don't necessarily need to fail concurrent requests
            else if (random.Value.NextDouble() < options.writePermanentErrorRate)
            {
                callback(42, numBytesToWrite, context);
                versionScheme.TryAdvanceVersion((_, _) =>
                {
                    var index = permanentlyFailedRangesStart.BinarySearch(logicalDestStart);
                    if (index >= 0)
                        permanentlyFailedRangesEnd[index] =
                            Math.Max(permanentlyFailedRangesEnd[index], logicalDestEnd);
                    else
                    {
                        // This technically does not correctly merge / stores overlapping ranges, but for failing
                        // segments, it does not matter
                        var i = ~index;
                        permanentlyFailedRangesStart.Insert(i, logicalDestStart);
                        permanentlyFailedRangesEnd.Insert(i, logicalDestEnd);
                    }
                });
            }
            versionScheme.Leave();
            underlying.WriteAsync(sourceAddress, segmentId, destinationAddress, numBytesToWrite, callback, context);
        }

        public override void ReadAsync(int segmentId, ulong sourceAddress, IntPtr destinationAddress, uint readLength,
            DeviceIOCompletionCallback callback, object context)
        {
            var logicalDestStart = segmentId * underlying.SegmentSize + (long) destinationAddress;
            var logicalDestEnd = logicalDestStart + readLength;
            versionScheme.Enter();
            if (permanentlyFailedRangesStart.Count != 0)
            {
                // First failed range that's smaller than requested range start
                var startIndex = permanentlyFailedRangesStart.BinarySearch(logicalDestStart);
                if (startIndex < 0) startIndex = ~startIndex - 1;
                
                // First failed range after requested range end
                var endIndex = permanentlyFailedRangesEnd.BinarySearch(logicalDestEnd);
                if (endIndex < 0) endIndex = ~endIndex;
                // Check if there are overlaps
                if (startIndex >= 0 && permanentlyFailedRangesEnd[startIndex] > logicalDestStart ||
                    endIndex < permanentlyFailedRangesStart.Count && permanentlyFailedRangesStart[endIndex] < logicalDestEnd)
                {
                    // If so, simulate a failure by calling callback with an error
                    callback(42, readLength, context);
                    versionScheme.Leave();
                    return;
                }
            }
            // Otherwise, decide whether we need to introduce a failure
            if (random.Value.NextDouble() < options.readTransientErrorRate)
            {
                callback(42, readLength, context);
            }
            else if (random.Value.NextDouble() < options.readPermanentErrorRate)
            {
                callback(42, readLength, context);

                versionScheme.TryAdvanceVersion((_, _) =>
                {
                    var index = permanentlyFailedRangesStart.BinarySearch(logicalDestStart);
                    if (index >= 0)
                        permanentlyFailedRangesEnd[index] =
                            Math.Max(permanentlyFailedRangesEnd[index], logicalDestEnd);
                    else
                    {
                        var i = ~index;
                        permanentlyFailedRangesStart.Insert(i, logicalDestStart);
                        permanentlyFailedRangesEnd.Insert(i, logicalDestEnd);
                    }
                });
            }
            versionScheme.Leave();        
            underlying.ReadAsync(segmentId, sourceAddress, destinationAddress, readLength, callback, context);
        }

        public override void Dispose()
        {
            underlying.Dispose();
        }
    }
}