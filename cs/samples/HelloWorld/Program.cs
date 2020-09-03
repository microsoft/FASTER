// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;
using System.IO;

namespace HelloWorld
{
    public class Program
    {
        static void Main()
        {
            long key = 1, value = 1, output = 0;

            // This is a simple in-memory sample of FASTER using value types
            
            // Create device for FASTER log (auto-deleted on close)
            var log = Devices.CreateLogDevice(Path.GetTempPath() + "hlog.log", deleteOnClose: true);

            // Create store instance
            var store = new FasterKV<long, long>(1L << 20,
                new LogSettings { LogDevice = log },
                comparer: new LongComparer() // key comparer
                );

            // Create functions for callbacks; we use standard in-built functions
            // Read-modify-writes will perform summation
            var funcs = new SimpleFunctions<long, long>((a, b) => a + b);

            // Each logical sequence of calls to FASTER is associated
            // with a FASTER session. No concurrency allowed within
            // a single session
            var session = store.NewSession(funcs);

            // (1) Upsert and read back upserted value
            session.Upsert(ref key, ref value);
            session.Read(ref key, ref output);
            if (output == value)
                Console.WriteLine("(1) Success!");
            else
                Console.WriteLine("(1) Error!");

            /// (2) Delete key, read to verify deletion
            session.Delete(ref key, Empty.Default, 0);
            var status = session.Read(ref key, ref output);
            if (status == Status.NOTFOUND)
                Console.WriteLine("(2) Success!");
            else
                Console.WriteLine("(2) Error!");

            // (3) Perform two read-modify-writes (summation), verify result
            key = 2;
            long input1 = 25, input2 = 27;

            session.RMW(ref key, ref input1);
            session.RMW(ref key, ref input2);
            session.Read(ref key, ref output);

            if (output == input1 + input2)
                Console.WriteLine("(3) Success!");
            else
                Console.WriteLine("(3) Error!");

            // End session
            session.Dispose();

            // Dispose store
            store.Dispose();

            // Close devices
            log.Close();

            Console.WriteLine("Press <ENTER> to end");
            Console.ReadLine();
        }
    }

    public struct LongComparer : IFasterEqualityComparer<long>
    {
        public bool Equals(ref long k1, ref long k2) => k1 == k2;
        public long GetHashCode64(ref long k) => Utility.GetHashCode(k);
    }
}
