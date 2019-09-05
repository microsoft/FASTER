// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ClassSample
{
    public class MyKey : IFasterEqualityComparer<MyKey>
    {
        public int key;

        public long GetHashCode64(ref MyKey key)
        {
            return Utility.GetHashCode(key.key);
        }

        public bool Equals(ref MyKey key1, ref MyKey key2)
        {
            return key1.key == key2.key;
        }
    }

    public class MyKeySerializer : BinaryObjectSerializer<MyKey>
    {
        public override void Serialize(ref MyKey key)
        {
            writer.Write(key.key);
        }

        public override void Deserialize(ref MyKey key)
        {
            key.key = reader.ReadInt32();
        }
    }


    public class MyValue
    {
        public int value;
    }

    public class MyValueSerializer : BinaryObjectSerializer<MyValue>
    {
        public override void Serialize(ref MyValue value)
        {
            writer.Write(value.value);
        }

        public override void Deserialize(ref MyValue value)
        {
            value.value = reader.ReadInt32();
        }
    }

    public class MyInput
    {
        public int value;
    }

    public class MyOutput
    {
        public MyValue value;
    }


    public class MyContext { }

    public class MyFunctions : IFunctions<MyKey, MyValue, MyInput, MyOutput, MyContext>
    {
        public void InitialUpdater(ref MyKey key, ref MyInput input, ref MyValue value) => value.value = input.value;
        public void CopyUpdater(ref MyKey key, ref MyInput input, ref MyValue oldValue, ref MyValue newValue) => newValue = oldValue;
        public bool InPlaceUpdater(ref MyKey key, ref MyInput input, ref MyValue value) { value.value += input.value; return true; }


        public void SingleReader(ref MyKey key, ref MyInput input, ref MyValue value, ref MyOutput dst) => dst.value = value;
        public void SingleWriter(ref MyKey key, ref MyValue src, ref MyValue dst) => dst = src;
        public void ConcurrentReader(ref MyKey key, ref MyInput input, ref MyValue value, ref MyOutput dst) => dst.value = value;
        public bool ConcurrentWriter(ref MyKey key, ref MyValue src, ref MyValue dst) { dst = src; return true; }

        public void ReadCompletionCallback(ref MyKey key, ref MyInput input, ref MyOutput output, MyContext ctx, Status status) { }
        public void UpsertCompletionCallback(ref MyKey key, ref MyValue value, MyContext ctx) { }
        public void RMWCompletionCallback(ref MyKey key, ref MyInput input, MyContext ctx, Status status) { }
        public void DeleteCompletionCallback(ref MyKey key, MyContext ctx) { }
        public void CheckpointCompletionCallback(Guid sessionId, CommitPoint commitPoint) { }
    }

    class Program
    {
        static void Main(string[] args)
        {
            // This sample uses class key and value types, which are not blittable (i.e., they
            // require a pointer to heap objects). Such datatypes include variable length types
            // such as strings. You can override the default key equality comparer in two ways;
            // (1) Make Key implement IFasterEqualityComparer<Key> interface
            // (2) Provide IFasterEqualityComparer<Key> instance as param to constructor
            // FASTER stores the actual objects in a separate object log.
            // Note that serializers are required for class types, see below.

            var log = Devices.CreateLogDevice(Path.GetTempPath() + "hlog.log");
            var objlog = Devices.CreateLogDevice(Path.GetTempPath() + "hlog.obj.log");

            var h = new FasterKV
                <MyKey, MyValue, MyInput, MyOutput, MyContext, MyFunctions>
                (1L << 20, new MyFunctions(),
                new LogSettings {  LogDevice = log, ObjectLogDevice = objlog, MemorySizeBits = 29 },
                null,
                new SerializerSettings<MyKey, MyValue> { keySerializer = () => new MyKeySerializer(), valueSerializer = () => new MyValueSerializer() }
                );

            var context = default(MyContext);

            // Each thread calls StartSession to register itself with FASTER
            h.StartSession();

            for (int i = 0; i < 20000; i++)
            {
                var _key = new MyKey { key = i };
                var value = new MyValue { value = i };
                h.Upsert(ref _key, ref value, context, 0);

                // Each thread calls Refresh periodically for thread coordination
                if (i % 1024 == 0) h.Refresh();
            }
            var key = new MyKey { key = 23 };
            var input = default(MyInput);
            MyOutput g1 = new MyOutput();
            var status = h.Read(ref key, ref input, ref g1, context, 0);

            if (status == Status.OK && g1.value.value == key.key)
                Console.WriteLine("Success!");
            else
                Console.WriteLine("Error!");

            MyOutput g2 = new MyOutput();
            key = new MyKey { key = 46 };
            status = h.Read(ref key, ref input, ref g2, context, 0);

            if (status == Status.OK && g2.value.value == key.key)
                Console.WriteLine("Success!");
            else
                Console.WriteLine("Error!");

            /// Delete key, read to verify deletion
            var output = new MyOutput();
            h.Delete(ref key, context, 0);
            status = h.Read(ref key, ref input, ref output, context, 0);
            if (status == Status.NOTFOUND)
                Console.WriteLine("Success!");
            else
                Console.WriteLine("Error!");

            // Each thread ends session when done
            h.StopSession();

            // Dispose FASTER instance and log
            h.Dispose();
            log.Close();
            objlog.Close();

            Console.WriteLine("Press <ENTER> to end");
            Console.ReadLine();
        }
    }
}
