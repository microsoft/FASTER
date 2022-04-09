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
            log = Devices.CreateLogDevice(@$"{this.dataFolder}\data\Store-hlog.log", preallocateFile: false);
            objLog = Devices.CreateLogDevice(@$"{this.dataFolder}\data\Store-hlog-obj.log", preallocateFile: false);

            FasterKVSettings<Types.StoreKey, Types.StoreValue> fkvSettings = new()
            {
                IndexSize = 1L << 20,
                LogDevice = log,
                ObjectLogDevice = objLog,
                MutableFraction = 0.3,
                PageSize = 1L << 15,
                MemorySize = 1L << 20,
                CheckpointDir = $"{this.dataFolder}/data/checkpoints",
                KeySerializer = () => new Types.StoreKeySerializer(),
                ValueSerializer = () => new Types.StoreValueSerializer()
            };

            this.db = new FasterKV<Types.StoreKey, Types.StoreValue>(fkvSettings);

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
            db.TryInitiateFullCheckpoint(out Guid token, CheckpointType.Snapshot);
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

