using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;

namespace FASTER.core
{
    public interface IRemapScheme : IDisposable
    {
        string BaseName();

        uint SectorSize();

        long Capacity();

        long SegmentSize();

        IDevice CreateNewMappedRegion(long startAddress);
        
        IEnumerable<(long, IDevice)> ListMappedRegions();
    }

    public class RemapOnFailureLogDevice : StorageDeviceBase
    {
        private IRemapScheme remapScheme;
        private SimpleVersionScheme versionScheme;
        private List<(long, IDevice)> existingMappedRegions;
        private int outstandingWriteRequests = 0;

        private class MappedRegionComparer : IComparer<(long, IDevice)>
        {
            public int Compare((long, IDevice) x, (long, IDevice) y) => (int) (x.Item1 - y.Item1);
        }

        private MappedRegionComparer comparer = new MappedRegionComparer();

        public RemapOnFailureLogDevice(IRemapScheme remapScheme) : base(remapScheme.BaseName(), remapScheme.SectorSize(),
            remapScheme.Capacity())
        {
            this.remapScheme = remapScheme;
            versionScheme = new SimpleVersionScheme();
            existingMappedRegions = new List<(long, IDevice)>();
            segmentSize = remapScheme.SegmentSize();
            foreach (var (startAddress, device) in remapScheme.ListMappedRegions())
                existingMappedRegions.Add((startAddress, device));
            if (existingMappedRegions.Count == 0)
                existingMappedRegions.Add((0, remapScheme.CreateNewMappedRegion(0)));
        }


        public override void RemoveSegmentAsync(int segment, AsyncCallback callback, IAsyncResult result)
        {
            // TODO(Tianyu): Assumes that removing segment implicitly deletes every previous segments
            var countdown = new CountdownEvent(1);

            var removalStartAddress = segment << segmentSizeBits;
            var removalEndAddress = (segment + 1) << segmentSizeBits;

            versionScheme.Enter();
            Interlocked.Increment(ref outstandingWriteRequests);
            var startIndex = existingMappedRegions.BinarySearch((removalStartAddress, null), comparer);
            if (startIndex < 0) startIndex = ~startIndex - 1;

            for (var i = startIndex; i < existingMappedRegions.Count; i++)
            {
                if (existingMappedRegions[i + 1].Item1 > removalEndAddress) break;

                var translatedRemovalStart = removalStartAddress - existingMappedRegions[i].Item1;
                var startAddressOnMappedRegion = Math.Max(translatedRemovalStart, existingMappedRegions[i].Item1);
                var translatedRemovalEnd = removalEndAddress - existingMappedRegions[i].Item1;

                // Check to see if segment stretches into the next mapped region
                if (i + 1 < existingMappedRegions.Count && removalEndAddress >= existingMappedRegions[i + 1].Item1)
                {
                    // If so, safe to just delete this segment
                    countdown.AddCount();
                    existingMappedRegions[i].Item2
                        .RemoveSegmentAsync((int) (startAddressOnMappedRegion >> segmentSizeBits), ar =>
                        {
                            if (countdown.Signal())
                            {
                                callback(ar);
                                countdown.Dispose();
                                Interlocked.Decrement(ref outstandingWriteRequests);
                            }
                        }, result);
                    continue;
                }

                // Otherwise, there are potentially entries from other logical segments in the same physical segment
                // and we must check for that
                var translatedStartSegment = startAddressOnMappedRegion >> segmentSizeBits;
                var translatedEndSegment = translatedRemovalEnd >> segmentSizeBits;
                if (translatedStartSegment + 1 == translatedEndSegment)
                {
                    // If deleted logical segment straddles two physical segments, remove the first one. 
                    countdown.AddCount();
                    existingMappedRegions[i].Item2
                        .RemoveSegmentAsync((int) (startAddressOnMappedRegion >> segmentSizeBits), ar =>
                        {
                            if (countdown.Signal())
                            {
                                callback(ar);
                                countdown.Dispose();
                                Interlocked.Decrement(ref outstandingWriteRequests);
                            }
                        }, result);
                }
                else
                {
                    Debug.Assert(translatedStartSegment == translatedEndSegment);
                    if (translatedRemovalEnd % SegmentSize == 0 &&
                        ((ulong) translatedRemovalEnd & segmentSizeMask) == 0)
                    {
                        // Special case where mapped region's segments and logical device segments align, can issue delete straight away
                        countdown.AddCount();
                        existingMappedRegions[i].Item2
                            .RemoveSegmentAsync((int) (startAddressOnMappedRegion / SegmentSize), ar =>
                            {
                                if (countdown.Signal())
                                {
                                    callback(ar);
                                    countdown.Dispose();
                                    Interlocked.Decrement(ref outstandingWriteRequests);
                                }
                            }, result);
                    }
                    // Otherwise, cannot safely delete
                }
            }

            versionScheme.Leave();
            if (countdown.Signal())
            {
                callback(result);
                countdown.Dispose();
                Interlocked.Decrement(ref outstandingWriteRequests);
            }
        }

        public override void WriteAsync(IntPtr sourceAddress, int segmentId, ulong destinationAddress,
            uint numBytesToWrite,
            DeviceIOCompletionCallback callback, object context)
        {
            var countdown = new CountdownEvent(1);
            var startAddress = (segmentId << segmentSizeBits) + (long) destinationAddress;
            var endAddress = startAddress + numBytesToWrite;
            Debug.Assert(endAddress < (segmentId + 1) << segmentSizeBits);

            versionScheme.Enter();
            Interlocked.Increment(ref outstandingWriteRequests);
            var i = existingMappedRegions.BinarySearch((startAddress, null), comparer);
            if (i < 0) i = ~i - 1;
            uint writtenBytes = 0;
            uint aggregateErrorCode = 0;
            while (writtenBytes < numBytesToWrite)
            {
                var startAddressOnMappedRegion = Math.Max(startAddress, existingMappedRegions[i].Item1);
                var spaceLeft = i == existingMappedRegions.Count - 1
                    ? long.MaxValue
                    : existingMappedRegions[i + 1].Item1 - startAddressOnMappedRegion;
                var bytesToWrite = Math.Min(spaceLeft, numBytesToWrite - writtenBytes);
                countdown.AddCount();
                existingMappedRegions[i].Item2
                    .WriteAsync(IntPtr.Add(sourceAddress, (int) writtenBytes),
                        (int) startAddressOnMappedRegion >> segmentSizeBits,
                        (ulong) startAddressOnMappedRegion & segmentSizeMask, (uint) bytesToWrite, (e, n, o) =>
                        {
                            if (e != 0) aggregateErrorCode = e;
                            if (countdown.Signal())
                            {
                                callback(aggregateErrorCode, n, o);
                                countdown.Dispose();
                                Interlocked.Decrement(ref outstandingWriteRequests);
                            }
                        }, context);
                writtenBytes += (uint) bytesToWrite;
            }

            versionScheme.Leave();

            if (countdown.Signal())
            {
                callback(aggregateErrorCode, numBytesToWrite, context);
                countdown.Dispose();
                Interlocked.Decrement(ref outstandingWriteRequests);
            }
        }

        public override void ReadAsync(int segmentId, ulong sourceAddress, IntPtr destinationAddress, uint readLength,
            DeviceIOCompletionCallback callback, object context)
        {
            var countdown = new CountdownEvent(1);
            var startAddress = (segmentId << segmentSizeBits) + (long) destinationAddress;
            var endAddress = startAddress + readLength;
            Debug.Assert(endAddress < (segmentId + 1) << segmentSizeBits);

            versionScheme.Enter();
            var i = existingMappedRegions.BinarySearch((startAddress, null), comparer);
            if (i < 0) i = ~i - 1;
            uint bytesRead = 0;
            uint aggregateErrorCode = 0;
            while (bytesRead < readLength)
            {
                var startAddressOnMappedRegion = Math.Max(startAddress, existingMappedRegions[i].Item1);
                var bytesLeft = i == existingMappedRegions.Count - 1
                    ? long.MaxValue
                    : existingMappedRegions[i + 1].Item1 - startAddressOnMappedRegion;
                var bytesToRead = Math.Min(bytesLeft, readLength - bytesRead);
                countdown.AddCount();
                existingMappedRegions[i].Item2
                    .ReadAsync((int) startAddressOnMappedRegion >> segmentSizeBits,
                        (ulong) startAddressOnMappedRegion & segmentSizeMask,
                        IntPtr.Add(destinationAddress, (int) bytesRead), (uint) bytesToRead, (e, n, o) =>
                        {
                            if (e != 0) aggregateErrorCode = e;
                            if (countdown.Signal())
                            {
                                callback(aggregateErrorCode, n, o);
                                countdown.Dispose();
                            }
                        }, context);
                bytesRead += (uint) bytesToRead;
            }

            versionScheme.Leave();

            if (countdown.Signal())
            {
                callback(aggregateErrorCode, readLength, context);
                countdown.Dispose();
            }
        }

        public override void Dispose()
        {
            remapScheme.Dispose();
        }

        public void HandleWriteError(FasterLog log, CommitFailureException exception)
        {
            var sealLocation = exception.LinkedCommitInfo.CommitInfo.FromAddress;
            ManualResetEventSlim resetEvent = new ManualResetEventSlim();
            versionScheme.TryAdvanceVersion((_, _) =>
            {
                // Can only seal the last segment
                Debug.Assert(sealLocation > existingMappedRegions[existingMappedRegions.Count - 1].Item1);
                // TODO(Tianyu): Maybe too strict?
                // Wait until there are no ongoing write requests 
                while (outstandingWriteRequests != 0) Thread.Yield();
                existingMappedRegions.Add((sealLocation, remapScheme.CreateNewMappedRegion(sealLocation)));
                log.UnsafeResetFlushStatus();
                resetEvent.Set();
            });
            resetEvent.Wait();
        }
    }
}