using System;
using FASTER.core;
using FASTER.test.recovery.objects;

namespace TstRunner
{
    public class Program
    {
        public static void Main()
        {
            var test = new ObjectRecoveryTests2();
            test.Setup();
            test.ObjectRecoveryTest2(CheckpointType.Snapshot, 400, false).GetAwaiter().GetResult();
            test.TearDown();
        }
    }
}
