// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;
using System.IO;

namespace StoreCheckpointRecover
{
    class Program
    {
        static void Main()
        {
            // We use classes in this example, replace with blittable structs for 
            // much better performance

            var path = Path.GetTempPath() + "StoreCheckpointRecover\\";

            // Main hybrid log device                        
            var log = Devices.CreateLogDevice(path + "hlog.log");

            // With non-blittable types, you need an object log device in addition to the
            // main device. FASTER serializes the actual objects in the object log.
            var objlog = Devices.CreateLogDevice(path + "hlog.obj.log");

            // Serializers are required for non-blittable types in order to write to storage
            // You can also mark types as DataContract (lower performance)
            var serializerSettings =
                new SerializerSettings<MyKey, MyValue> { 
                    keySerializer = () => new MyKeySerializer(), 
                    valueSerializer = () => new MyValueSerializer()
                };

            var store = new FasterKV<MyKey, MyValue>(
                1L << 20,
                new LogSettings {  LogDevice = log, ObjectLogDevice = objlog },
                new CheckpointSettings { CheckpointDir = path },
                serializerSettings: serializerSettings
                );

            using (var s = store.NewSession(new Functions()))
            {
                for (int i = 0; i < 20000; i++)
                {
                    var _key = new MyKey { key = i };
                    var value = new MyValue { value = i };
                    s.Upsert(ref _key, ref value);
                }

                var key = new MyKey { key = 23 };
                MyOutput g1 = default;
                var status = s.Read(ref key, ref g1);

                if (status == Status.OK && g1.value.value == key.key)
                    Console.WriteLine("Success!");
                else
                    Console.WriteLine("Error!");
            }

            // Take fold-over checkpoint of FASTER, wait to complete
            store.TakeFullCheckpointAsync(CheckpointType.FoldOver)
                .GetAwaiter().GetResult();

            // Dispose store instance
            store.Dispose();

            // Close logs
            log.Dispose();
            objlog.Dispose();

            // Create new store
            log = Devices.CreateLogDevice(path + "hlog.log");
            objlog = Devices.CreateLogDevice(path + "hlog.obj.log");

            var store2 = new FasterKV<MyKey, MyValue>(
                1L << 20,
                new LogSettings { LogDevice = log, ObjectLogDevice = objlog },
                new CheckpointSettings { CheckpointDir = path },
                serializerSettings: serializerSettings
                );

            // Recover store from latest checkpoint
            store2.Recover();
            
            using (var s = store2.NewSession(new Functions()))
            {
                // Test Read
                var key = new MyKey { key = 23 };
                MyOutput g1 = default;
                var status = s.Read(ref key, ref g1);

                if (status == Status.OK && g1.value.value == key.key)
                    Console.WriteLine("Success!");
                else
                    Console.WriteLine("Error!");
            }

            store2.Dispose();
            log.Dispose();
            objlog.Dispose();

            try { new DirectoryInfo(path).Delete(true); } catch { }

            Console.WriteLine("Press <ENTER> to end");
            Console.ReadLine();
        }
    }
}
