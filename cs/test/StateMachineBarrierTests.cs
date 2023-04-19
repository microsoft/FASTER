// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Collections.Generic;
using System.Threading;
using FASTER.core;
using System.IO;
using NUnit.Framework;
using FASTER.test.recovery.sumstore;
using NUnit.Framework.Interfaces;

namespace FASTER.test.statemachine
{
    [TestFixture]
    public class StateMachineBarrierTests
    {
        IDevice log;
        FasterKV<AdId, NumClicks> fht1;
        const int numOps = 5000;
        AdId[] inputArray;

        [SetUp]
        public void Setup()
        {
            inputArray = new AdId[numOps];
            for (int i = 0; i < numOps; i++)
            {
                inputArray[i].adId = i;
            }

            log = Devices.CreateLogDevice(TestUtils.MethodTestDir + "/StateMachineTest1.log", deleteOnClose: true);
            string checkpointDir = TestUtils.MethodTestDir + "/statemachinetest";
            Directory.CreateDirectory(checkpointDir);
            fht1 = new FasterKV<AdId, NumClicks>
                (128,
                logSettings: new LogSettings { LogDevice = log, MutableFraction = 0.1, PageSizeBits = 10, MemorySizeBits = 13 },
                checkpointSettings: new CheckpointSettings { CheckpointDir = checkpointDir, CheckpointVersionSwitchBarrier = true }
                );
        }

        [TearDown]
        public void TearDown()
        {
            fht1?.Dispose();
            fht1 = null;
            log?.Dispose();
            log = null;
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        [TestCase]
        [Category("FasterKV")]
        [Category("CheckpointRestore")]
        [Category("Smoke")]
        public void StateMachineBarrierTest1()
        {
            Prepare(out var f, out var s1, out var uc1, out var s2);

            // We should be in PREPARE, 1
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.PREPARE, 1), fht1.SystemState));

            // Invoke Refresh on session s2, it will spin (blocked from working due to CheckpointVersionSwitchBarrier)
            s2.Refresh(waitComplete: false);

            // s1 has not refreshed, so we should still be in PREPARE, 1
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.PREPARE, 1), fht1.SystemState));

            // Refresh s1
            uc1.Refresh();

            // We should now be in WAIT_FLUSH, 2 as all threads have reached IN_PROGRESS, 2
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.WAIT_FLUSH, 2), fht1.SystemState));

            // Expect checkpoint completion callback
            f.checkpointCallbackExpectation = 1;

            // We can complete s2 now, as barrier is done
            s2.CompleteOp();

            s2.Refresh();

            // We should be in PERSISTENCE_CALLBACK, 2
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.PERSISTENCE_CALLBACK, 2), fht1.SystemState));

            uc1.Refresh();

            // Completion callback should have been called once
            Assert.AreEqual(0, f.checkpointCallbackExpectation);

            // We should be in REST, 2
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.REST, 2), fht1.SystemState));

            // Dispose session s2; does not move state machine forward
            s2.Dispose();

            uc1.EndUnsafe();
            s1.Dispose();

            RecoverAndTest(log);
        }

        void Prepare(out SimpleFunctions f,
            out ClientSession<AdId, NumClicks, NumClicks, NumClicks, Empty, SimpleFunctions> s1,
            out UnsafeContext<AdId, NumClicks, NumClicks, NumClicks, Empty, SimpleFunctions> uc1,
            out ThreadSession<AdId, NumClicks, NumClicks, NumClicks, Empty, SimpleFunctions> s2,
            long toVersion = -1)
        {
            f = new SimpleFunctions();

            // We should be in REST, 1
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.REST, 1), fht1.SystemState));

            // Take index checkpoint for recovery purposes
            fht1.TryInitiateIndexCheckpoint(out _);
            fht1.CompleteCheckpointAsync().AsTask().GetAwaiter().GetResult();

            // Index checkpoint does not update version, so
            // we should still be in REST, 1
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.REST, 1), fht1.SystemState));

            NumClicks value;

            s1 = fht1.For(f).NewSession<SimpleFunctions>("foo");

            for (int key = 0; key < numOps; key++)
            {
                value.numClicks = key;
                s1.Upsert(ref inputArray[key], ref value, Empty.Default, key);
            }

            // Ensure state machine needs no I/O wait during WAIT_FLUSH
            fht1.Log.ShiftReadOnlyAddress(fht1.Log.TailAddress, true);

            // Create unsafe context and hold epoch to prepare for manual state machine driver
            uc1 = s1.UnsafeContext;
            uc1.BeginUnsafe();

            // Start session s2 on another thread for testing
            s2 = fht1.For(f).CreateThreadSession(f);

            // We should be in REST, 1
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.REST, 1), fht1.SystemState));

            fht1.TryInitiateHybridLogCheckpoint(out _, CheckpointType.FoldOver, targetVersion: toVersion);

            // We should be in PREPARE, 1
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.PREPARE, 1), fht1.SystemState));
        }

        void RecoverAndTest(IDevice log)
        {
            NumClicks inputArg = default;
            NumClicks output = default;
            var f = new SimpleFunctions();

            var fht2 = new FasterKV
                <AdId, NumClicks>
                (128,
                logSettings: new LogSettings { LogDevice = log, MutableFraction = 0.1, PageSizeBits = 10, MemorySizeBits = 13 },
                checkpointSettings: new CheckpointSettings { CheckpointDir = TestUtils.MethodTestDir + "/statemachinetest" }
                );

            fht2.Recover(); // sync, does not require session

            using (var s3 = fht2.ResumeSession(f, "foo", out CommitPoint lsn))
            {
                Assert.AreEqual(numOps - 1, lsn.UntilSerialNo);

                // Expect checkpoint completion callback
                f.checkpointCallbackExpectation = 1;

                s3.Refresh();

                // Completion callback should have been called once
                Assert.AreEqual(0, f.checkpointCallbackExpectation);

                for (var key = 0; key < numOps; key++)
                {
                    var status = s3.Read(ref inputArray[key], ref inputArg, ref output, Empty.Default, s3.SerialNo);

                    if (status.IsPending)
                        s3.CompletePending(true);
                    else
                    {
                        Assert.AreEqual(key, output.numClicks);
                    }
                }
            }

            fht2.Dispose();
        }
    }
}
