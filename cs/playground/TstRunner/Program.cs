// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using FASTER.test.recovery.objects;
using System;

namespace TstRunner
{
    public class Program
    {
        public static void Main()
        {

            for (int i = 0; i < 10000; i++)
            {
                var device = Devices.CreateLogDevice("d:/test");
                var log = new FasterKV<long, long>(1024, new LogSettings { LogDevice = device });
                log.Dispose();
                device.Dispose();
            }


            GC.Collect();
            GC.WaitForFullGCComplete();

            /*
            var test = new ObjectRecoveryTests2();
            test.Setup();
            test.ObjectRecoveryTest2(CheckpointType.Snapshot, 400, false).GetAwaiter().GetResult();
            test.TearDown();*/
        }
    }
}
