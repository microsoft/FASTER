// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.IO;
using FASTER.core;

namespace ClassRecoveryDurablity
{
    public class Storedb
    {
        private readonly string dataFolder;

        public FasterKV<Types.StoreKey, Types.StoreValue> db;
        public IDevice log;
        public IDevice objLog;

        public Storedb(string folder)
        {
            dataFolder = folder;
        }

        public bool InitAndRecover()
        {
            var logSize = 1L << 20;
            log = Devices.CreateLogDevice(@$"{this.dataFolder}\data\Store-hlog.log", preallocateFile: false);
            objLog = Devices.CreateLogDevice(@$"{this.dataFolder}\data\Store-hlog-obj.log", preallocateFile: false);

            this.db = new FasterKV
                <Types.StoreKey, Types.StoreValue>(
                    logSize,
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
                Console.WriteLine("call recover db");
                db.Recover();
                return false;
            }

            return true;
        }

        public Guid Checkpoint()
        {
            db.TakeFullCheckpoint(out Guid token);
            db.CompleteCheckpointAsync().GetAwaiter().GetResult();
            return token;
        }

        public void Dispose()
        {
            db.Dispose();
            log.Dispose();
            objLog.Dispose();
        }
    }
}

