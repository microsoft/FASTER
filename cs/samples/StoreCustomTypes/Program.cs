// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;
using System.IO;

namespace StoreCustomTypes
{
    class Program
    {
        static void Main()
        {
            // This sample uses custom class key and value types, which are not blittable

            // You can override the default key equality comparer in two ways;
            // (1) Make Key implement IFasterEqualityComparer<Key> interface
            // (2) Provide IFasterEqualityComparer<Key> instance as param to FASTER constructor

            var path = Path.GetTempPath() + "StoreCustomTypes/";

            FasterKVSettings<MyKey, MyValue> fkvSettings = new()
            {
                IndexSize = 1L << 26,

                // Main hybrid log device
                LogDevice = Devices.CreateLogDevice(path + "hlog.log"),

                // With non-blittable types, you need an object log device in addition to the
                // main device. FASTER serializes the actual objects in the object log.
                ObjectLogDevice = Devices.CreateLogDevice(path + "hlog.obj.log"),

                // Serializers are required for class types in order to write to storage
                // You can also mark types as DataContract (lower performance)
                KeySerializer = () => new MyKeySerializer(),
                ValueSerializer = () => new MyValueSerializer()
            };

            FasterKV<MyKey, MyValue> store = new(fkvSettings);

            // A session calls StartSession to register itself with FASTER
            // Functions specify various callbacks that FASTER makes to user code
            var s = store.NewSession(new Functions());

            MyContext context = default;
            for (int i = 0; i < 20000; i++)
            {
                var _key = new MyKey { key = i };
                var value = new MyValue { value = i };
                s.Upsert(ref _key, ref value, context, 0);
            }

            var key = new MyKey { key = 23 };
            var input = default(MyInput);
            MyOutput g1 = new();
            var status = s.Read(ref key, ref input, ref g1, context, 0);

            if (status.Found && g1.value.value == key.key)
                Console.WriteLine("Success!");
            else
                Console.WriteLine("Error!");

            MyOutput g2 = new();
            key = new MyKey { key = 46 };
            status = s.Read(ref key, ref input, ref g2, context, 0);

            if (status.Found && g2.value.value == key.key)
                Console.WriteLine("Success!");
            else
                Console.WriteLine("Error!");

            // Let's flush the log to storage and evict from memory
            store.Log.FlushAndEvict(true);

            // Read will now be served from disk
            key = new MyKey { key = 46 };
            status = s.Read(ref key, ref input, ref g2, context, 0);

            // We will receive the result via ReadCompletionCallback in Functions 
            if (!status.IsPending)
                Console.WriteLine("Error!");

            // End session when done
            s.Dispose();

            // Dispose store instance
            store.Dispose();

            // Close logs
            fkvSettings.LogDevice.Dispose();
            fkvSettings.ObjectLogDevice.Dispose();

            try { new DirectoryInfo(path).Delete(true); } catch { }

            Console.WriteLine("Press <ENTER> to end");
            Console.ReadLine();
        }
    }
}
