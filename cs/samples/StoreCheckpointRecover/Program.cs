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
            var path = Path.GetTempPath() + "StoreCheckpointRecover";

            PopulateStore(path);
            RecoverAndTestStore(path);

            Console.WriteLine("Press <ENTER> to end");
            Console.ReadLine();
        }

        static void PopulateStore(string path)
        {
            // We use class types MyKey and MyValue in this example, replace with blittable structs for 
            // better performance. Serializers are required for class (non-blittable) types in order to
            // write to storage. You can also mark types as DataContract (lower performance).

            using var settings = new FasterKVSettings<MyKey, MyValue>(path)
            {
                KeySerializer = () => new MyKeySerializer(),
                ValueSerializer = () => new MyValueSerializer()
            };

            using var store = new FasterKV<MyKey, MyValue>(settings);
            using var s = store.NewSession(new Functions());

            for (int i = 0; i < 20000; i++)
            {
                var _key = new MyKey { key = i };
                var value = new MyValue { value = i };
                s.Upsert(ref _key, ref value);
            }

            var key = new MyKey { key = 23 };
            MyOutput g1 = default;
            var status = s.Read(ref key, ref g1);

            if (status.Found && g1.value.value == key.key)
                Console.WriteLine("Success!");
            else
                Console.WriteLine("Error!");

            // Take index + fold-over checkpoint of FASTER, wait to complete
            store.TakeFullCheckpointAsync(CheckpointType.FoldOver).AsTask().GetAwaiter().GetResult();
        }

        static void RecoverAndTestStore(string path)
        {
            using var settings = new FasterKVSettings<MyKey, MyValue>(path, deleteDirOnDispose: true)
            {
                KeySerializer = () => new MyKeySerializer(),
                ValueSerializer = () => new MyValueSerializer(),
                TryRecoverLatest = true,
            };

            using var store = new FasterKV<MyKey, MyValue>(settings);
            using var session = store.NewSession(new Functions());

            // Test Read
            var key = new MyKey { key = 23 };
            MyOutput g1 = default;
            var status = session.Read(ref key, ref g1);

            if (status.Found && g1.value.value == key.key)
                Console.WriteLine("Success!");
            else
                Console.WriteLine("Error!");
        }
    }
}
