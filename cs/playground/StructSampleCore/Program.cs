// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;
using System.IO;

namespace StructSampleCore
{
    public class Program
    {
        static void Main(string[] args)
        {
            Sample1();
            Sample2();
            Console.WriteLine("Press <ENTER> to end");
            Console.ReadLine();
        }

        static void Sample1()
        {
            long key = 1, value = 1, input = 10, output = 0;

            // This represents the simplest possible in-memory sample of FASTER
            // Create temp file (auto-deleted on close) as hybrid log
            // Can also use null device as Devices.CreateLogDevice("")
            var log = Devices.CreateLogDevice(Path.GetTempPath() + "hlog.log", false, true);
            var fht = new FasterKV<long, long, long, long, Empty, Sample1Funcs>
              (1L << 20, new Sample1Funcs(), new LogSettings { LogDevice = log }, null, null, new LongComparer());

            fht.StartSession();


            fht.Upsert(ref key, ref value, Empty.Default, 0);
            fht.Read(ref key, ref input, ref output, Empty.Default, 0);
            if (output == value)
                Console.WriteLine("Sample1: Success!");
            else
                Console.WriteLine("Sample1: Error!");

            fht.RMW(ref key, ref input, Empty.Default, 0);
            fht.RMW(ref key, ref input, Empty.Default, 0);
            fht.Read(ref key, ref input, ref output, Empty.Default, 0);
            if (output == value + 2 * input)
                Console.WriteLine("Sample1: Success!");
            else
                Console.WriteLine("Sample1: Error!");

            fht.StopSession();
            fht.Dispose();
            log.Close();
        }

        static void Sample2()
        {
            // This sample uses struct key and value types, which are blittable (i.e., do not
            // require a pointer to heap objects). Such datatypes enables the 
            // "high speed" mode for FASTER by using a specialized BlittableAllocator for the
            // hybrid log. You can override the default key equality comparer in two ways;
            // (1) Make Key implement IFasterEqualityComparer<Key> interface
            // (2) Provide IFasterEqualityComparer<Key> instance as param to constructor
            // Note that serializers are not required/used for blittable key and value types.

            var fht =
                new FasterKV<Key, Value, Input, Output, Empty, Sample2Funcs>
                (1L << 20, new Sample2Funcs(),
                new LogSettings { LogDevice = Devices.CreateLogDevice("") }); // Use Null device

            fht.StartSession();

            Input input = default(Input);
            Output output = default(Output);

            var key1 = new Key { kfield1 = 13, kfield2 = 14 };
            var value = new Value { vfield1 = 23, vfield2 = 24 };

            // Upsert item into store, and read it back
            fht.Upsert(ref key1, ref value, Empty.Default, 0);
            fht.Read(ref key1, ref input, ref output, Empty.Default, 0);

            if ((output.value.vfield1 == value.vfield1) && (output.value.vfield2 == value.vfield2))
                Console.WriteLine("Sample2: Success!");
            else
                Console.WriteLine("Sample2: Error!");

            var key2 = new Key { kfield1 = 15, kfield2 = 16 };
            input = new Input { ifield1 = 25, ifield2 = 26 };

            // Two read-modify-write (RMW) operations (sum aggregator)
            // Followed by read of result
            fht.RMW(ref key2, ref input, Empty.Default, 0);
            fht.RMW(ref key2, ref input, Empty.Default, 0);
            fht.Read(ref key2, ref input, ref output, Empty.Default, 0);

            if ((output.value.vfield1 == input.ifield1 * 2) && (output.value.vfield2 == input.ifield2 * 2))
                Console.WriteLine("Sample2: Success!");
            else
                Console.WriteLine("Sample2: Error!");

            fht.StopSession();
            fht.Dispose();
        }
    }
}
