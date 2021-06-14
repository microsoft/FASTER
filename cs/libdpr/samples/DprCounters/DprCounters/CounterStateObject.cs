using System;
using System.IO;
using FASTER.libdpr;

namespace DprCounters
{
    public sealed class CounterStateObject : SimpleStateObject
    {
        private string checkpointDirectory;
        public long value;

        public CounterStateObject(string checkpointDirectory, long version)
        {
            
            this.checkpointDirectory = checkpointDirectory;
            if (version != 0)
                RestoreCheckpoint(version);
        }
        
        protected override void PerformCheckpoint(long version, Action onPersist)
        {
            var fileName = Path.Join(checkpointDirectory, version.ToString());
            using (var fs = File.Open(fileName,
                FileMode.OpenOrCreate, FileAccess.Write, FileShare.None))
            {
                fs.Write(BitConverter.GetBytes(value), 0, sizeof(long));
            }

            onPersist();
        }

        protected override void RestoreCheckpoint(long version)
        {
            var fileName = Path.Join(checkpointDirectory, version.ToString());
            using (var fs = File.Open(fileName,
                FileMode.Open, FileAccess.Read, FileShare.None))
            {
                var byteBuffer = new byte[8];
                var bytesRead = 0;
                while (bytesRead < sizeof(long))
                {
                    bytesRead += fs.Read(byteBuffer, bytesRead, sizeof(long) - bytesRead);
                }

                value = BitConverter.ToInt64(byteBuffer, 0);
            }
        }
    }
}