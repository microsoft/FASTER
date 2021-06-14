using System;
using System.IO;
using FASTER.libdpr;

namespace DprCounters
{
    public class CounterStateObject : SimpleStateObject<string>
    {
        private string checkpointDirectory;
        private int checkpointNum;
        public long value;

        public CounterStateObject(string checkpointDirectory)
        {
            
            this.checkpointDirectory = checkpointDirectory;
            foreach (var file in Directory.GetFiles(checkpointDirectory))
            {
                
            }
        }
        
        protected override void PerformCheckpoint(Action<string> onPersist)
        {
            checkpointNum++;
            var fileName = Path.Join(checkpointDirectory, checkpointNum.ToString());
            using (var fs = File.Open(fileName,
                FileMode.OpenOrCreate, FileAccess.Write, FileShare.None))
            {
                fs.Write(BitConverter.GetBytes(value), 0, sizeof(long));
            }

            onPersist(fileName);
        }

        protected override void RestoreCheckpoint(string token)
        {
            using (var fs = File.Open(token,
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