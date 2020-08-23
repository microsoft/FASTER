// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Linq;
using FASTER.core;
using System.IO;
using NUnit.Framework;
using FASTER.test.recovery.sumstore;

namespace FASTER.test.statemachine
{

    [TestFixture]
    public class StateMachineTests
    {
        IDevice log;
        FasterKV<AdId, NumClicks> fht1;
        const int numOps = 5000;
        AdId[] inputArray;

        [SetUp]
        public void Setup()
        {
            log = Devices.CreateLogDevice(TestContext.CurrentContext.TestDirectory + "\\StateMachineTest1.log", deleteOnClose: true);
            Directory.CreateDirectory(TestContext.CurrentContext.TestDirectory + "\\checkpoints4");
            fht1 = new FasterKV<AdId, NumClicks>
                (128,
                logSettings: new LogSettings { LogDevice = log, MutableFraction = 0.1, PageSizeBits = 10, MemorySizeBits = 13 },
                checkpointSettings: new CheckpointSettings { CheckpointDir = TestContext.CurrentContext.TestDirectory + "\\checkpoints4", CheckPointType = CheckpointType.FoldOver }
                );
        }

        [TearDown]
        public void TearDown()
        {
            fht1.Dispose();
            log.Close();
            new DirectoryInfo(TestContext.CurrentContext.TestDirectory + "\\checkpoints4").Delete(true);
        }


        [TestCase]
        public void StateMachineTest1()
        {
            // We should be in REST, 1
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.REST, 1), fht1.SystemState));

            // Take index checkpoint for recovery purposes
            fht1.TakeIndexCheckpoint(out _);
            fht1.CompleteCheckpointAsync().GetAwaiter().GetResult();

            // Index checkpoint does not update version, so
            // we should still be in REST, 1
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.REST, 1), fht1.SystemState));

            inputArray = new AdId[numOps];
            for (int i = 0; i < numOps; i++)
            {
                inputArray[i].adId = i;
            }

            NumClicks value;


            var s1 = fht1.For<NumClicks, NumClicks, Empty>().NewSession(new SimpleFunctions(), "foo", threadAffinitized: true);

            for (int key = 0; key < numOps; key++)
            {
                value.numClicks = key;
                s1.Upsert(ref inputArray[key], ref value, Empty.Default, key);
            }

            // Ensure state machine needs no I/O wait during WAIT_FLUSH
            fht1.Log.ShiftReadOnlyAddress(fht1.Log.TailAddress, true);

            // Start affinitized session s2 on another thread for testing
            var s2 = fht1.For<NumClicks, NumClicks, Empty>().CreateThreadSession(new SimpleFunctions(), threadAffinized: true);

            // We should be in REST, 1
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.REST, 1), fht1.SystemState));

            fht1.TakeHybridLogCheckpoint(out _);

            // We should be in PREPARE, 1
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.PREPARE, 1), fht1.SystemState));

            // Refresh session s2
            s2.Refresh();

            // s1 has not refreshed, so we should still be in PREPARE, 1
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.PREPARE, 1), fht1.SystemState));

            // Refresh s1
            s1.Refresh();

            // We should now be in IN_PROGRESS, 2
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.IN_PROGRESS, 2), fht1.SystemState));

            // Dispose session s2; does not move state machine forward
            s2.Dispose();

            // We should still be in IN_PROGRESS, 2
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.IN_PROGRESS, 2), fht1.SystemState));

            // Since s1 is the only session now, it will fast-foward state machine
            // to completion
            s1.Refresh();

            // We should be in REST, 2
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.REST, 2), fht1.SystemState));

            s1.Dispose();

            RecoverAndTest(log);
        }

        void RecoverAndTest(IDevice log)
        {
            NumClicks inputArg = default;
            NumClicks output = default;

            var fht2 = new FasterKV
                <AdId, NumClicks, NumClicks, NumClicks, Empty, SimpleFunctions>
                (128, new SimpleFunctions(),
                logSettings: new LogSettings { LogDevice = log, MutableFraction = 0.1, PageSizeBits = 10, MemorySizeBits = 13 },
                checkpointSettings: new CheckpointSettings { CheckpointDir = TestContext.CurrentContext.TestDirectory + "\\checkpoints4", CheckPointType = CheckpointType.FoldOver }
                );

            fht2.Recover(); // sync, does not require session

            using (var s3 = fht2.ResumeSession("foo", out CommitPoint lsn))
            {
                Assert.IsTrue(lsn.UntilSerialNo == numOps - 1);

                for (int key = 0; key < numOps; key++)
                {
                    var status = s3.Read(ref inputArray[key], ref inputArg, ref output, Empty.Default, 0);

                    if (status == Status.PENDING)
                        s3.CompletePending(true);
                    else
                    {
                        Assert.IsTrue(output.numClicks == key);
                    }
                }
            }

            fht2.Dispose();
        }
    }

    public class SimpleFunctions : SimpleFunctions<AdId, NumClicks, Empty>
    {
        public override void ReadCompletionCallback(ref AdId key, ref NumClicks input, ref NumClicks output, Empty ctx, Status status)
        {
            Assert.IsTrue(status == Status.OK);
            Assert.IsTrue(output.numClicks == key.adId);
        }
    }
}
