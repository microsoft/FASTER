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

            // Create device for FASTER log
            var path = Path.GetTempPath() + "HelloWorld\\";
            var log = Devices.CreateLogDevice(Path.GetTempPath() + "hlog.log");

            // Create store instance
            var store = new FasterKV<long, long>(
                size: 1L << 20, // 1M cache lines of 64 bytes each = 64MB hash table
                logSettings: new LogSettings { LogDevice = log } // specify log settings (e.g., size of log in memory)
                );

            // Create functions for callbacks; we use a standard in-built function in this sample
            // although you can write your own by extending FunctionsBase or implementing IFunctions
            // In this in-built function, read-modify-writes will perform value merges via summation
            var funcs = new SimpleFunctions<long, long>((a, b) => a + b);

            // Each logical sequence of calls to FASTER is associated
            // with a FASTER session. No concurrency allowed within
            // a single session
            var session = store.NewSession(funcs);

            // (1) Upsert and read back upserted value
            session.Upsert(ref key, ref value);
            
            // In this sample, reads are served back from memory and return synchronously
            // Reads from disk will return PENDING status; see StoreCustomTypes example for details
            var status = session.Read(ref key, ref output);
            if (status == Status.OK && output == value)
                Console.WriteLine("(1) Success!");
            else
                Console.WriteLine("(1) Error!");

            /// (2) Delete key, read to verify deletion
            session.Delete(ref key);
            
            status = session.Read(ref key, ref output);
            if (status == Status.NOTFOUND)
                Console.WriteLine("(2) Success!");
            else
                Console.WriteLine("(2) Error!");

            // (3) Perform two read-modify-writes (summation), verify result
            key = 2;
            long input1 = 25, input2 = 27;

            session.RMW(ref key, ref input1);
            session.RMW(ref key, ref input2);

            status = session.Read(ref key, ref output);

            if (status == Status.OK && output == input1 + input2)
                Console.WriteLine("(3) Success!");
            else
                Console.WriteLine("(3) Error!");

            // End session
            session.Dispose();

            // Dispose store
            store.Dispose();

            // Close devices
            log.Close();

            // Delete the created files
            try { new DirectoryInfo(path).Delete(true); } catch { }

            Console.WriteLine("Press <ENTER> to end");
            Console.ReadLine();
        }
    }
}
