using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Threading;

namespace FASTER.core
{
    public interface IRemapScheme : IDisposable
    {
        string BaseName();

        uint SectorSize();

        long Capacity();
        
        IDevice CreateNewMappedRegion(long startAddress);

        void SealLastMappedRegion(long endAddress);

        IEnumerable<(long, IDevice)> ListMappedRegions();
    }
    
    public class RemapOnFailureDevice : StorageDeviceBase
    {
        private IRemapScheme remapScheme;
        private SimpleVersionScheme simpleVersionScheme;
        private List<(long, IDevice)> existingMappedRegions;

        private class MappedRegionComparer : IComparer<(long, IDevice)>
        {
            public int Compare((long, IDevice) x, (long, IDevice) y) => (int) (x.Item1 - y.Item1);
        }

        private MappedRegionComparer comparer = new MappedRegionComparer();

        public RemapOnFailureDevice(IRemapScheme remapScheme) : base(remapScheme.BaseName(), remapScheme.SectorSize(), remapScheme.Capacity())
        {
            this.remapScheme = remapScheme;
            simpleVersionScheme = new SimpleVersionScheme();
            existingMappedRegions = new List<(long, IDevice)>();
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
            
            simpleVersionScheme.Enter();
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
                            } 
                        }, result);
                }
                else
                {
                    Debug.Assert(translatedStartSegment == translatedEndSegment);
                    if (translatedRemovalEnd % SegmentSize == 0 && ((ulong) translatedRemovalEnd & segmentSizeMask) == 0)
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
                                } 
                            }, result);
                    }
                    // Otherwise, cannot safely delete
                }
            }

            simpleVersionScheme.Leave();
            if (countdown.Signal())
            {
                callback(result);
                countdown.Dispose();
            }
        }

        public override void WriteAsync(IntPtr sourceAddress, int segmentId, ulong destinationAddress, uint numBytesToWrite,
            DeviceIOCompletionCallback callback, object context)
        {
            var countdown = new CountdownEvent(1);
            var startAddress =  (segmentId << segmentSizeBits) + (long) destinationAddress;
            var endAddress = startAddress + numBytesToWrite;
            Debug.Assert(endAddress < (segmentId + 1) << segmentSizeBits);
            
            simpleVersionScheme.Enter();
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
                    .WriteAsync(IntPtr.Add(sourceAddress, (int) writtenBytes), (int) startAddressOnMappedRegion >> segmentSizeBits, 
                        (ulong) startAddressOnMappedRegion & segmentSizeMask, (uint) bytesToWrite, (e, n, o) =>
                    {
                        if (e != 0) aggregateErrorCode = e;
                        if (countdown.Signal())
                        {
                            callback(aggregateErrorCode, n, o);
                            countdown.Dispose();
                        } 
                    }, context);
                writtenBytes += (uint) bytesToWrite;
            }
            simpleVersionScheme.Leave();

            if (countdown.Signal())
            {
                callback(aggregateErrorCode, numBytesToWrite, context);
                countdown.Dispose();
            }
        }

        public override void ReadAsync(int segmentId, ulong sourceAddress, IntPtr destinationAddress, uint readLength,
            DeviceIOCompletionCallback callback, object context)
        {
            var countdown = new CountdownEvent(1);
            var startAddress =  (segmentId << segmentSizeBits) + (long) destinationAddress;
            var endAddress = startAddress + readLength;
            Debug.Assert(endAddress < (segmentId + 1) << segmentSizeBits);
            
            simpleVersionScheme.Enter();
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
                        (ulong) startAddressOnMappedRegion & segmentSizeMask, IntPtr.Add(destinationAddress, (int) bytesRead), (uint) bytesToRead, (e, n, o) =>
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
            simpleVersionScheme.Leave();

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

        public void HandleWriteError(CommitFailureException exception, Action beforeNewMappedRegion)
        {
            var sealLocation = exception.LinkedCommitInfo.CommitInfo.FromAddress;
            ManualResetEventSlim resetEvent = new ManualResetEventSlim();
            simpleVersionScheme.TryAdvanceVersion((_, _) =>
            {
                // Can only seal the last segment
                Debug.Assert(sealLocation > existingMappedRegions[existingMappedRegions.Count - 1].Item1);
                remapScheme.SealLastMappedRegion(sealLocation);
                beforeNewMappedRegion();
                existingMappedRegions.Add((sealLocation, remapScheme.CreateNewMappedRegion(sealLocation)));
                resetEvent.Set();
            });
            resetEvent.Wait();
        }
    }
}