using System;

namespace FASTER.core
{
    public class DeviceCheckpointManager : ICheckpointManager
    {
        private readonly INamedDeviceFactory deviceFactory;

        public DeviceCheckpointManager(INamedDeviceFactory deviceFactory)
        {
            this.deviceFactory = deviceFactory;
        }
        
        public void InitializeIndexCheckpoint(Guid indexToken)
        {
            throw new NotImplementedException();
        }

        public void InitializeLogCheckpoint(Guid logToken)
        {
            throw new NotImplementedException();
        }

        public void CommitIndexCheckpoint(Guid indexToken, byte[] commitMetadata)
        {
            throw new NotImplementedException();
        }

        public void CommitLogCheckpoint(Guid logToken, byte[] commitMetadata)
        {
            throw new NotImplementedException();
        }

        public byte[] GetIndexCommitMetadata(Guid indexToken)
        {
            throw new NotImplementedException();
        }

        public byte[] GetLogCommitMetadata(Guid logToken)
        {
            throw new NotImplementedException();
        }

        public IDevice GetIndexDevice(Guid indexToken)
        {
            throw new NotImplementedException();
        }

        public IDevice GetSnapshotLogDevice(Guid token)
        {
            throw new NotImplementedException();
        }

        public IDevice GetSnapshotObjectLogDevice(Guid token)
        {
            throw new NotImplementedException();
        }

        public bool GetLatestCheckpoint(out Guid indexToken, out Guid logToken)
        {
            throw new NotImplementedException();
        }
    }
}