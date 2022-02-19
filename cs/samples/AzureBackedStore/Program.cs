// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using FASTER.devices;
using System;
using System.IO;

namespace AzureBackedStore
{
    class Program
    {
        // We use emulated storage string in this sample, needs Azure Storage Emulator
        // installed (see https://azure.microsoft.com/en-us/downloads/)
        // You may also use a real Azure storage account here
        public const string STORAGE_STRING = "UseDevelopmentStorage=true;";
        public const string BASE_CONTAINER = "cloudbackedstore";

        static void Main()
        {
            // Main hybrid log device                        
            var log = new AzureStorageDevice(STORAGE_STRING, BASE_CONTAINER, "", "hlog.log");

            // With non-blittable types, you need an object log device in addition to the
            // main device. FASTER serializes the actual objects in the object log.
            var objlog = new AzureStorageDevice(STORAGE_STRING, BASE_CONTAINER, "", "hlog.obj.log");

            var checkpointManager = new DeviceLogCommitCheckpointManager(
                new AzureStorageNamedDeviceFactory(STORAGE_STRING),
                new DefaultCheckpointNamingScheme($"{BASE_CONTAINER}/checkpoints/"));


            var store = new FasterKV<long, string>(
                1L << 17,
                new LogSettings {  LogDevice = log, ObjectLogDevice = objlog },
                new CheckpointSettings { CheckpointManager = checkpointManager }
                );

            using (var s = store.NewSession(new Functions()))
            {
                for (long i = 0; i < 20000; i++)
                {
                    var _key = i;
                    var value = $"value-{i}";
                    s.Upsert(ref _key, ref value);
                }
                s.CompletePending(true);

                long key = 23;
                string output = default;
                string context = "value-23";

                var status = s.Read(ref key, ref output, context);

                if (!status.Pending)
                {
                    if (status.Found && output == context)
                        Console.WriteLine("Success!");
                    else
                        Console.WriteLine("Error!");
                }
            }

            // Take fold-over checkpoint of FASTER, wait to complete
            store.TakeFullCheckpointAsync(CheckpointType.FoldOver)
                .GetAwaiter().GetResult();

            // Dispose store instance
            store.Dispose();

            // Dispose logs
            log.Dispose();
            objlog.Dispose();

            // Create new store
            log = new AzureStorageDevice(STORAGE_STRING, BASE_CONTAINER, "", "hlog.log");
            objlog = new AzureStorageDevice(STORAGE_STRING, BASE_CONTAINER, "", "hlog.obj.log");

            var store2 = new FasterKV<long, string>(
                1L << 17,
                new LogSettings { LogDevice = log, ObjectLogDevice = objlog },
                new CheckpointSettings { CheckpointManager = checkpointManager }
                );

            // Recover store from latest checkpoint
            store2.Recover();
            
            using (var s = store2.NewSession(new Functions()))
            {
                // Test Read
                long key = 23;
                string output = default;
                string context = "value-23";

                var status = s.Read(ref key, ref output, context);

                if (!status.Pending)
                {
                    if (status.Found && output == context)
                        Console.WriteLine("Success!");
                    else
                        Console.WriteLine("Error!");
                }
            }

            store2.Dispose();

            // Purge cloud log files
            log.PurgeAll();
            objlog.PurgeAll();

            // Purge cloud checkpoints - warning all data under specified base path are removed
            checkpointManager.PurgeAll();

            // Dispose devices
            log.Dispose();
            objlog.Dispose();

            // Dispose checkpoint manager
            checkpointManager.Dispose();

            Console.WriteLine("Press <ENTER> to end");
            Console.ReadLine();
        }
    }
}
