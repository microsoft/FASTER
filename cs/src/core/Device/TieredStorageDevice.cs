using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace FASTER.core.Device
{
    class TieredStorageDevice : StorageDeviceBase
    {
        private readonly IList<IDevice> devices;
        private readonly uint commitPoint;

        public TieredStorageDevice() : base("", 512, -1) {}

        public override void Close()
        {
            throw new NotImplementedException();
        }

        public override void DeleteSegmentRange(int fromSegment, int toSegment)
        {
            throw new NotImplementedException();
        }

        public override void ReadAsync(int segmentId, ulong sourceAddress, IntPtr destinationAddress, uint readLength, IOCompletionCallback callback, IAsyncResult asyncResult)
        {
            throw new NotImplementedException();
        }

        public override void WriteAsync(IntPtr sourceAddress, int segmentId, ulong destinationAddress, uint numBytesToWrite, IOCompletionCallback callback, IAsyncResult asyncResult)
        {
            throw new NotImplementedException();
        }
    }
}
