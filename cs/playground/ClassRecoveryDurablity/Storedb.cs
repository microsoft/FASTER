using System;
using System.IO;
using FASTER.core;

namespace ClassRecoveryDurablity
{
    public class Storedb
    {
        private string dataFolder;

        public FasterKV<Types.StoreKey, Types.StoreValue, Types.StoreInput, Types.StoreOutput, Types.StoreContext, Types.StoreFunctions> db;
        public IDevice log;
        public IDevice objLog;

        public Storedb(string folder)
        {
            this.dataFolder = folder;
        }

        public bool InitAndRecover()
        {
            var logSize = 1L << 20;
            this.log = Devices.CreateLogDevice(@$"{this.dataFolder}\data\Store-hlog.log", preallocateFile: false);
            this.objLog = Devices.CreateLogDevice(@$"{this.dataFolder}\data\Store-hlog-obj.log", preallocateFile: false);

            this.db = new FasterKV
                <Types.StoreKey, Types.StoreValue, Types.StoreInput, Types.StoreOutput, Types.StoreContext, Types.StoreFunctions>(
                    logSize,
                    new Types.StoreFunctions(),
                    new LogSettings
                    {
                        LogDevice = this.log,
                        ObjectLogDevice = this.objLog,
                        MutableFraction = 0.3,
                        PageSizeBits = 15,
                        MemorySizeBits = 20
                    },
                    new CheckpointSettings
                    {
                        CheckpointDir = $"{this.dataFolder}/data/checkpoints"
                    },
                    new SerializerSettings<Types.StoreKey, Types.StoreValue>
                    {
                        keySerializer = () => new Types.StoreKeySerializer(),
                        valueSerializer = () => new Types.StoreValueSerializer()
                    }
                );

            if (Directory.Exists($"{this.dataFolder}/data/checkpoints"))
            {
                Console.WriteLine("call rcover db");

                this.db.Recover();

                return false;
            }

            return true;
        }

        public Guid Checkpoint()
        {
            Guid token = default(Guid);

            this.db.TakeFullCheckpoint(out token);
            this.db.CompleteCheckpointAsync().GetAwaiter().GetResult();

            return token;
        }

        public void Dispose()
        {
            this.db.Dispose();
            this.log.Close();
            this.objLog.Close();
        }
    }
}

