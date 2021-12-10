using System;
using System.Collections.Generic;
using System.Threading;

namespace FASTER.core
{
    public interface IFallbackScheme : IDisposable
    {
        string BaseName();

        uint SectorSize();

        long SegmentSize();

        long Capacity();

        IDevice GetMainDevice();

        IDevice GetFallbackDevice();

        List<(long, long)> GetFallbackRanges();

        void AddFallbackRange(long rangeStart, long rangeEnd);
    }

    public class FallbackOnFailureDevice : StorageDeviceBase
    {
        private IFallbackScheme fallbackScheme;
        private SimpleVersionScheme versionScheme;
        private List<(long, long)> fallbackRanges;
        private List<long> fallbackRangeOffsets;

        public FallbackOnFailureDevice(IFallbackScheme fallbackScheme) : base(fallbackScheme.BaseName(),
            fallbackScheme.SectorSize(), fallbackScheme.Capacity())
        {
            this.fallbackScheme = fallbackScheme;
            versionScheme = new SimpleVersionScheme();
            fallbackRanges = new List<(long, long)>();
            fallbackRangeOffsets = new List<long>();
            fallbackRangeOffsets.Add(0);
            segmentSize = fallbackScheme.SegmentSize();
        }

        public override void RemoveSegmentAsync(int segment, AsyncCallback callback, IAsyncResult result)
        {
            // TODO(Tianyu): do not (cannot?) delete fallback device segments
            fallbackScheme.GetMainDevice().RemoveSegmentAsync(segment, callback, result);
        }

        public override void WriteAsync(IntPtr sourceAddress, int segmentId, ulong destinationAddress,
            uint numBytesToWrite,
            DeviceIOCompletionCallback callback, object context)
        {
            var logicalDestStart = segmentId * SegmentSize + (long) destinationAddress;
            var logicalDestEnd = logicalDestStart + numBytesToWrite;
            versionScheme.Enter();
            if (fallbackRanges.Count != 0)
            {
                var countdown = new CountdownEvent(1);
                uint aggregateErrorCode = 0;
                // Search for whether the intended range overlaps with one of the remapped regions
                for (var i = 0; i < fallbackRanges.Count; i++)
                {
                    var (rangeStart, rangeEnd) = fallbackRanges[i];
                    // No overlap
                    if (rangeStart >= logicalDestEnd || rangeEnd <= logicalDestStart) continue;

                    // Overlaps and splits range into three chunks
                    var firstChunkStart = logicalDestStart;
                    var firstChunkEnd = rangeStart - logicalDestStart;
                    if (firstChunkStart < firstChunkEnd)
                    {
                        fallbackScheme.GetMainDevice().WriteAsync(sourceAddress, segmentId, destinationAddress,
                            (uint) (firstChunkEnd - firstChunkEnd),
                            (e, n, o) =>
                            {
                                if (countdown.Signal())
                                {
                                    callback(aggregateErrorCode, n, o);
                                    countdown.Dispose();
                                }
                            }, context);
                    }

                    var middleChunkStart = Math.Max(rangeStart, logicalDestStart);
                    var middleChunkEnd = Math.Min(rangeEnd, logicalDestEnd);
                    var translatedMiddleChunkStart = fallbackRangeOffsets[i] + middleChunkStart - rangeStart;
                    fallbackScheme.GetFallbackDevice().WriteAsync(
                        IntPtr.Add(sourceAddress, (int) (middleChunkStart - logicalDestStart)),
                        (int) (translatedMiddleChunkStart / SegmentSize),
                        (ulong) (translatedMiddleChunkStart % SegmentSize),
                        (uint) (middleChunkEnd - middleChunkStart),
                        (e, n, o) =>
                        {
                            if (countdown.Signal())
                            {
                                callback(aggregateErrorCode, n, o);
                                countdown.Dispose();
                            }
                        }, context);

                    versionScheme.Leave();

                    var lastChunkStart = middleChunkEnd;
                    var lastChunkEnd = rangeEnd;
                    if (lastChunkStart < lastChunkEnd)
                        WriteAsync(
                            IntPtr.Add(sourceAddress, (int) (lastChunkStart - logicalDestStart)),
                            segmentId,
                            (ulong) lastChunkStart,
                            (uint) (lastChunkEnd - lastChunkStart),
                            (e, n, o) =>
                            {
                                if (countdown.Signal())
                                {
                                    callback(aggregateErrorCode, n, o);
                                    countdown.Dispose();
                                }
                            }, context);

                    if (countdown.Signal())
                    {
                        callback(aggregateErrorCode, numBytesToWrite, context);
                        countdown.Dispose();
                    }

                    return;
                }
            }

            // Otherwise just forward it as there are no error ranges
            fallbackScheme.GetMainDevice().WriteAsync(sourceAddress, segmentId, destinationAddress, numBytesToWrite,
                callback, context);
            versionScheme.Leave();
        }

        public override void ReadAsync(int segmentId, ulong sourceAddress, IntPtr destinationAddress, uint readLength,
            DeviceIOCompletionCallback callback, object context)
        {
            var logicalDestStart = segmentId * SegmentSize + (long) destinationAddress;
            var logicalDestEnd = logicalDestStart + readLength;
            versionScheme.Enter();
            if (fallbackRanges.Count != 0)
            {
                var countdown = new CountdownEvent(1);
                uint aggregateErrorCode = 0;
                // Search for whether the intended range overlaps with one of the remapped regions
                for (var i = 0; i < fallbackRanges.Count; i++)
                {
                    var (rangeStart, rangeEnd) = fallbackRanges[i];
                    // No overlap
                    if (rangeStart >= logicalDestEnd || rangeEnd <= logicalDestStart) continue;

                    // Overlaps and splits range into three chunks
                    var firstChunkStart = logicalDestStart;
                    var firstChunkEnd = rangeStart - logicalDestStart;
                    if (firstChunkStart < firstChunkEnd)
                    {
                        fallbackScheme.GetMainDevice().ReadAsync(segmentId, sourceAddress, destinationAddress,
                            (uint) (firstChunkEnd - firstChunkEnd),
                            (e, n, o) =>
                            {
                                if (countdown.Signal())
                                {
                                    callback(aggregateErrorCode, n, o);
                                    countdown.Dispose();
                                }
                            }, context);
                    }

                    var middleChunkStart = Math.Max(rangeStart, logicalDestStart);
                    var middleChunkEnd = Math.Min(rangeEnd, logicalDestEnd);
                    var translatedMiddleChunkStart = fallbackRangeOffsets[i] + middleChunkStart - rangeStart;
                    fallbackScheme.GetFallbackDevice().ReadAsync(
                        (int) (translatedMiddleChunkStart / SegmentSize),
                        (ulong) (translatedMiddleChunkStart % SegmentSize),
                        IntPtr.Add(destinationAddress, (int) (middleChunkStart - logicalDestStart)),
                        (uint) (middleChunkEnd - middleChunkStart),
                        (e, n, o) =>
                        {
                            if (countdown.Signal())
                            {
                                callback(aggregateErrorCode, n, o);
                                countdown.Dispose();
                            }
                        }, context);

                    versionScheme.Leave();

                    var lastChunkStart = middleChunkEnd;
                    var lastChunkEnd = rangeEnd;
                    if (lastChunkStart < lastChunkEnd)
                        ReadAsync(
                            segmentId,
                            (ulong) lastChunkStart,
                            IntPtr.Add(destinationAddress, (int) (lastChunkStart - logicalDestStart)),
                            (uint) (lastChunkEnd - lastChunkStart),
                            (e, n, o) =>
                            {
                                if (countdown.Signal())
                                {
                                    callback(aggregateErrorCode, n, o);
                                    countdown.Dispose();
                                }
                            }, context);

                    if (countdown.Signal())
                    {
                        callback(aggregateErrorCode, readLength, context);
                        countdown.Dispose();
                    }

                    return;
                }
            }

            // Otherwise just forward it as there are no error ranges
            fallbackScheme.GetMainDevice().ReadAsync(segmentId, sourceAddress, destinationAddress, readLength,
                callback, context);
        }

        public override void Dispose()
        {
            fallbackScheme.Dispose();
        }

        public void HandleWriteError<Key, Value>(CommitFailureException exception, AllocatorBase<Key, Value> allocator)
        {
            var info = exception.LinkedCommitInfo.CommitInfo;

            ManualResetEventSlim resetEvent = new ManualResetEventSlim();
            versionScheme.TryAdvanceVersion((_, _) =>
            {
                fallbackScheme.AddFallbackRange(info.FromAddress, info.UntilAddress);
                // Pretty sure this would work for overlapping ranges, just perhaps not as efficiently?
                fallbackRanges.Add((info.FromAddress, info.UntilAddress));
                fallbackRangeOffsets.Add(fallbackRangeOffsets[fallbackRangeOffsets.Count - 1] + info.UntilAddress -
                                         info.FromAddress);
                allocator.AsyncFlushPages(info.FromAddress, info.UntilAddress);
                resetEvent.Set();
            });
            resetEvent.Wait();
        }
    }
}