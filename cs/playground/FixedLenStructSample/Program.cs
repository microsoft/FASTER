// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using FASTER.test.recovery.sumstore.simple;
using System;

namespace FixedLenStructSample
{
    public class Program
    {
        // This sample uses fixed length structs for keys and values
        static void Main()
        {
            while (true)
                new SimpleRecoveryTests().SimpleRecoveryTest3();
            return;

            var log = Devices.CreateLogDevice("hlog.log", deleteOnClose: true);
            var fht = new FasterKV<FixedLenKey, FixedLenValue, string, string, Empty, FixedLenFunctions>
                (128, new FixedLenFunctions(),
                new LogSettings { LogDevice = log, MemorySizeBits = 17, PageSizeBits = 12 }
                );
            fht.StartSession();

            var key = new FixedLenKey("foo");
            var value = new FixedLenValue("bar");

            var status = fht.Upsert(ref key, ref value, Empty.Default, 0);

            if (status != Status.OK)
                Console.WriteLine("FixedLenStructSample: Error!");

            var input = default(string); // unused
            var output = default(string);

            key = new FixedLenKey("xyz");
            status = fht.Read(ref key, ref input, ref output, Empty.Default, 0);

            if (status != Status.NOTFOUND)
                Console.WriteLine("FixedLenStructSample: Error!");

            key = new FixedLenKey("foo");
            status = fht.Read(ref key, ref input, ref output, Empty.Default, 0);

            if (status != Status.OK)
                Console.WriteLine("FixedLenStructSample: Error!");

            if (output.Equals(value.ToString()))
                Console.WriteLine("FixedLenStructSample: Success!");
            else
                Console.WriteLine("FixedLenStructSample: Error!");

            fht.StopSession();
            fht.Dispose();
            log.Close();

            Console.WriteLine("Press <ENTER> to end");
            Console.ReadLine();
        }
    }
}
