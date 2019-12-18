using System;
using System.Diagnostics;
using System.Linq;
using System.Security.Cryptography;
using System.Threading;

namespace FASTER.core
{
    public class DeviceLogCommitManager : ILogCommitManager
    {
        private const string LOG_COMMIT_CONTAINER = "logcommits";
        private const string LOG_COMMIT_DEVICE_NAME = "commit";
        private const int HASH_SIZE = 16;
        
        private readonly INamedDeviceFactory deviceFactory;
        private readonly bool removeOutdated;
        // TODO(Tianyu): An external caller might update it for distributed CPR
        private long commitNum;
        
        // Only used if removeOutdated
        private IDevice stable, live;

        public DeviceLogCommitManager(INamedDeviceFactory deviceFactory, bool removeOutdated = false)
        {
            // TODO(Tianyu): Recover code
            this.deviceFactory = deviceFactory;
            this.removeOutdated = removeOutdated;
            if (removeOutdated)
            {
                // Initialize two devices if removeOutdated
                stable = deviceFactory.GetOrCreateFromName(LOG_COMMIT_CONTAINER, LOG_COMMIT_DEVICE_NAME + 0);
                live = deviceFactory.GetOrCreateFromName(LOG_COMMIT_CONTAINER, LOG_COMMIT_DEVICE_NAME + 1);
            }
        }
        
        public unsafe void Commit(long beginAddress, long untilAddress, byte[] commitMetadata)
        {
            // Use a checksum to make sure that we can detect partial writes
            // TODO(Tianyu): Should we create a new md5 instance every time or something?
            using (var md5 = MD5.Create())
            {
                byte[] checksum = md5.ComputeHash(commitMetadata);
                Debug.Assert(checksum.Length == HASH_SIZE, "Unexpected md5 return hash size");
                IDevice device = NextCommitDevice();
                
                // Number of writes we issue
                CountdownEvent countdown = new CountdownEvent(4);
                
                fixed (byte* checksumRaw = checksum,
                             commitNumRaw = BitConverter.GetBytes(commitNum),
                             sizeRaw = BitConverter.GetBytes(commitMetadata.Length),
                             commitMetadataRaw = commitMetadata)
                {
                    // TODO(Tianyu): This is potentially not performant when OS is not buffering writes (e.g. we
                    // are writing through to Azure blobs)
                    device.WriteAsync((IntPtr) checksumRaw, 0, HASH_SIZE, 
                        IOCallback(countdown), null);
                    device.WriteAsync((IntPtr) sizeRaw, HASH_SIZE + sizeof(long), sizeof(int), 
                        IOCallback(countdown), null);
                    device.WriteAsync((IntPtr) commitMetadataRaw, HASH_SIZE + sizeof(long) + sizeof(int) ,
                        (uint)commitMetadata.Length, IOCallback(countdown), null);
                }
                countdown.Wait();
            }
        }

        public byte[] GetCommitMetadata()
        {
            int metadataSize = HASH_SIZE + sizeof(int);
            // Allocate a write pad for performance for metadata
            byte[] writePad = new byte[metadataSize];
            // TODO(Tianyu): Should we create a new md5 instance every time or something?
            using (var md5 = MD5.Create())
            {
                foreach (var device in deviceFactory.ListDevicesNewestToOldest(LOG_COMMIT_CONTAINER))
                {
                    ReadInto(device, 0, writePad, metadataSize);
                    int size = BitConverter.ToInt32(writePad, HASH_SIZE);
                    byte[] body = new byte[size];
                    ReadInto(device, (ulong)metadataSize, body, size);
                    
                    var checksum = new ArraySegment<byte>(writePad, 0, HASH_SIZE);
                    if (md5.ComputeHash(body).SequenceEqual(checksum))
                    {
                        return body;
                    }
                }

                return null;
            }
        }

        private IDevice NextCommitDevice()
        {
            if (!removeOutdated)
            {
                return deviceFactory.GetOrCreateFromName(LOG_COMMIT_CONTAINER, LOG_COMMIT_DEVICE_NAME + commitNum++);
            }

            // swap the two commit files
            IDevice temp = stable;
            stable = live;
            live = temp;
            return live;
        }

        private static unsafe IOCompletionCallback IOCallback(CountdownEvent countdown)
        {
            return (errorCode, numBytes, overlapped) =>
            {
                try
                {
                    if (errorCode != 0)
                    {
                        // TODO(Tianyu): Should we make this fail stop?
                        Trace.TraceError("OverlappedStream GetQueuedCompletionStatus error: {0}", errorCode);
                    }
                    countdown.Signal();
                }
                finally
                {
                    Overlapped.Free(overlapped);
                }
            };
        }

        private static unsafe void ReadInto(IDevice commitDevice, ulong address, byte[] buffer, int size)
        {
            Debug.Assert(buffer.Length >= size);
            fixed (byte* bufferRaw = buffer)
            {
                CountdownEvent countdown = new CountdownEvent(1);
                commitDevice.ReadAsync(address, (IntPtr)bufferRaw, 
                    (uint)size, IOCallback(countdown), null);
                countdown.Wait();
            }
        }
    }
}