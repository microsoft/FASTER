using System;
using System.Collections;
using System.Collections.Generic;

namespace FASTER.core
{
    public interface IRemapScheme
    {
        string BaseName();

        uint SectorSize();

        long Capacity();
        
        IDevice CreateNewMappedRegion(long startAddress);

        void SealMappedRegion(IDevice device, long endAddress);

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
            simpleVersionScheme.Enter();
            var segmentStartAddress = segment * SegmentSize;
            var searchResult = existingMappedRegions.BinarySearch((segmentStartAddress, null), comparer);
            if (searchResult > 0)
            {
                // There is a mapped region that starts on this segment. Remove 
            }
            
        }

        public override void WriteAsync(IntPtr sourceAddress, int segmentId, ulong destinationAddress, uint numBytesToWrite,
            DeviceIOCompletionCallback callback, object context)
        {
            throw new NotImplementedException();
        }

        public override void ReadAsync(int segmentId, ulong sourceAddress, IntPtr destinationAddress, uint readLength,
            DeviceIOCompletionCallback callback, object context)
        {
            throw new NotImplementedException();
        }

        public override void Dispose()
        {
            throw new NotImplementedException();
        }
    }
}