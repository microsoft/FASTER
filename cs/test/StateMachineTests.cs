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
    public class StateMachineTests
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
                checkpointSettings: new CheckpointSettings { CheckpointDir = checkpointDir }
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
        public void StateMachineTest1()
        {
            Prepare(out var f, out var s1, out var uc1, out var s2);

            // We should be in PREPARE, 1
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.PREPARE, 1), fht1.SystemState));

            // Refresh session s2
            s2.Refresh();

            // s1 has not refreshed, so we should still be in PREPARE, 1
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.PREPARE, 1), fht1.SystemState));

            // Refresh s1
            uc1.Refresh();

            // We should now be in IN_PROGRESS, 2
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.IN_PROGRESS, 2), fht1.SystemState));

            s2.Refresh();

            // We should be in WAIT_FLUSH, 2
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.WAIT_FLUSH, 2), fht1.SystemState));

            // Expect checkpoint completion callback
            f.checkpointCallbackExpectation = 1;

            uc1.Refresh();

            // Completion callback should have been called once
            Assert.AreEqual(0, f.checkpointCallbackExpectation);


            // We should be in PERSISTENCE_CALLBACK, 2
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.PERSISTENCE_CALLBACK, 2), fht1.SystemState));

            s2.Refresh();

            // We should be in REST, 2
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.REST, 2), fht1.SystemState));

            // Dispose session s2; does not move state machine forward
            s2.Dispose();

            uc1.SuspendThread();
            uc1.Dispose();
            s1.Dispose();

            RecoverAndTest(log);
        }


        [TestCase]
        [Category("FasterKV"), Category("CheckpointRestore")]
        public void StateMachineTest2()
        {
            Prepare(out var f, out var s1, out var uc1, out var s2);

            // We should be in PREPARE, 1
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.PREPARE, 1), fht1.SystemState));

            // Refresh session s2
            s2.Refresh();

            // s1 has not refreshed, so we should still be in PREPARE, 1
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.PREPARE, 1), fht1.SystemState));

            // Refresh s1
            uc1.Refresh();

            // We should now be in IN_PROGRESS, 2
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.IN_PROGRESS, 2), fht1.SystemState));

            // Dispose session s2; moves state machine forward to WAIT_FLUSH, 2
            s2.Dispose();

            // We should be in WAIT_FLUSH, 2
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.WAIT_FLUSH, 2), fht1.SystemState));

            // Expect checkpoint completion callback
            f.checkpointCallbackExpectation = 1;

            // Since s1 is the only session now, it will fast-foward state machine
            // to completion
            uc1.Refresh();

            // Completion callback should have been called once
            Assert.AreEqual(0, f.checkpointCallbackExpectation);

            // We should be in REST, 2
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.REST, 2), fht1.SystemState));

            uc1.SuspendThread();
            uc1.Dispose();
            s1.Dispose();

            RecoverAndTest(log);
        }

        [TestCase]
        [Category("FasterKV"), Category("CheckpointRestore")]
        public void StateMachineTest3()
        {
            Prepare(out var f, out var s1, out var uc1, out var s2);

            // We should be in PREPARE, 1
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.PREPARE, 1), fht1.SystemState));

            // Refresh session s1
            uc1.Refresh();

            // s1 is now in PREPARE, 1
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.PREPARE, 1), SystemState.Make(s1.ctx.phase, s1.ctx.version)));

            // Suspend s1
            uc1.SuspendThread();

            // Since s2 is the only session now, it will fast-foward state machine
            // to completion
            s2.Refresh();

            // We should be in REST, 2
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.REST, 2), fht1.SystemState));

            // Expect checkpoint completion callback
            f.checkpointCallbackExpectation = 1;

            uc1.ResumeThread();

            // Completion callback should have been called once
            Assert.AreEqual(0, f.checkpointCallbackExpectation);

            s2.Dispose();

            uc1.SuspendThread();
            uc1.Dispose();
            s1.Dispose();

            RecoverAndTest(log);
        }

        [TestCase]
        [Category("FasterKV"), Category("CheckpointRestore")]
        public void StateMachineTest4()
        {
            Prepare(out var f, out var s1, out var uc1, out var s2);

            // We should be in PREPARE, 1
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.PREPARE, 1), fht1.SystemState));

            // Refresh session s2
            s2.Refresh();

            // s1 has not refreshed, so we should still be in PREPARE, 1
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.PREPARE, 1), fht1.SystemState));

            // Refresh s1
            uc1.Refresh();

            // We should now be in IN_PROGRESS, 2
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.IN_PROGRESS, 2), fht1.SystemState));

            // s1 is now in IN_PROGRESS, 2
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.IN_PROGRESS, 2), SystemState.Make(s1.ctx.phase, s1.ctx.version)));

            // Suspend s1
            uc1.SuspendThread();

            // Since s2 is the only session now, it will fast-foward state machine
            // to completion
            s2.Refresh();

            // We should be in REST, 2
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.REST, 2), fht1.SystemState));

            // Expect checkpoint completion callback
            f.checkpointCallbackExpectation = 1;

            uc1.ResumeThread();

            // Completion callback should have been called once
            Assert.AreEqual(0, f.checkpointCallbackExpectation);

            s2.Dispose();

            uc1.SuspendThread();
            uc1.Dispose();
            s1.Dispose();

            RecoverAndTest(log);
        }

        [TestCase]
        [Category("FasterKV"), Category("CheckpointRestore")]
        public void StateMachineTest5()
        {
            Prepare(out var f, out var s1, out var uc1, out var s2);

            // We should be in PREPARE, 1
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.PREPARE, 1), fht1.SystemState));

            // Refresh session s2
            uc1.Refresh();
            s2.Refresh();

            // We should now be in IN_PROGRESS, 2
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.IN_PROGRESS, 2), fht1.SystemState));

            uc1.Refresh();

            // We should be in WAIT_FLUSH, 2
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.WAIT_FLUSH, 2), fht1.SystemState));


            s2.Refresh();

            // We should be in PERSISTENCE_CALLBACK, 2
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.PERSISTENCE_CALLBACK, 2), fht1.SystemState));

            // Expect checkpoint completion callback
            f.checkpointCallbackExpectation = 1;

            uc1.Refresh();

            // Completion callback should have been called once
            Assert.AreEqual(0, f.checkpointCallbackExpectation);

            // We should be in REST, 2
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.REST, 2), fht1.SystemState));

            // No callback here since already done
            uc1.Refresh();

            // Suspend s1
            uc1.SuspendThread();

            // Since s2 is the only session now, it will fast-foward state machine
            // to completion
            s2.Refresh();

            // We should be in REST, 2
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.REST, 2), fht1.SystemState));

            // Expect no checkpoint completion callback on resume
            f.checkpointCallbackExpectation = 0;

            uc1.ResumeThread();

            // Completion callback should have been called once
            Assert.AreEqual(0, f.checkpointCallbackExpectation);

            s2.Dispose();

            uc1.SuspendThread();
            uc1.Dispose();
            s1.Dispose();

            RecoverAndTest(log);
        }


        [TestCase]
        [Category("FasterKV"), Category("CheckpointRestore")]
        public void StateMachineTest6()
        {
            Prepare(out var f, out var s1, out var uc1, out var s2);

            // Suspend s1
            uc1.SuspendThread();

            // s1 is now in REST, 1
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.REST, 1), SystemState.Make(s1.ctx.phase, s1.ctx.version)));

            // System should be in PREPARE, 1
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.PREPARE, 1), fht1.SystemState));

            // Since s2 is the only session now, it will fast-foward state machine
            // to completion
            s2.Refresh();

            // We should be in REST, 2
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.REST, 2), fht1.SystemState));

            s2.Dispose();

            fht1.TryInitiateHybridLogCheckpoint(out _, CheckpointType.FoldOver);
            fht1.CompleteCheckpointAsync().AsTask().GetAwaiter().GetResult();

            // We should be in REST, 3
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.REST, 3), fht1.SystemState));

            // Expect checkpoint completion callback on resume
            f.checkpointCallbackExpectation = 1;

            uc1.ResumeThread();

            // Completion callback should have been called once
            Assert.AreEqual(0, f.checkpointCallbackExpectation);

            uc1.SuspendThread();
            uc1.Dispose();
            s1.Dispose();

            RecoverAndTest(log);
        }
        
        [TestCase]
        [Category("FasterKV")]
        [Category("CheckpointRestore")]
        [Category("Smoke")]
        public void StateMachineCallbackTest1()
        {
            var callback = new TestCallback();
            fht1.UnsafeRegisterCallback(callback);
            Prepare(out var f, out var s1, out var uc1, out var s2);

            // We should be in PREPARE, 1
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.PREPARE, 1), fht1.SystemState));
            callback.CheckInvoked(fht1.SystemState);

            // Refresh session s2
            s2.Refresh();
            uc1.Refresh();

            // We should now be in IN_PROGRESS, 2
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.IN_PROGRESS, 2), fht1.SystemState));
            callback.CheckInvoked(fht1.SystemState);

            s2.Refresh();

            // We should be in WAIT_FLUSH, 2
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.WAIT_FLUSH, 2), fht1.SystemState));
            callback.CheckInvoked(fht1.SystemState);

            // Expect checkpoint completion callback
            f.checkpointCallbackExpectation = 1;

            uc1.Refresh();

            // Completion callback should have been called once
            Assert.IsTrue(f.checkpointCallbackExpectation == 0);

            // We should be in PERSISTENCE_CALLBACK, 2
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.PERSISTENCE_CALLBACK, 2), fht1.SystemState));
            callback.CheckInvoked(fht1.SystemState);

            s2.Refresh();

            // We should be in REST, 2
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.REST, 2), fht1.SystemState));
            callback.CheckInvoked(fht1.SystemState);

            // Dispose session s2; does not move state machine forward
            s2.Dispose();

            uc1.SuspendThread();
            uc1.Dispose();
            s1.Dispose();

            RecoverAndTest(log);
        }
        
        
        [TestCase]
        [Category("FasterKV")]
        [Category("CheckpointRestore")]
        public void VersionChangeTest()
        {
            var toVersion = 1 + (1 << 14);
            Prepare(out var f, out var s1, out var uc1, out var s2, toVersion);

            // We should be in PREPARE, 1
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.PREPARE, 1), fht1.SystemState));

            // Refresh session s2
            s2.Refresh();
            uc1.Refresh();

            // We should now be in IN_PROGRESS, toVersion
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.IN_PROGRESS, toVersion), fht1.SystemState));

            s2.Refresh();

            // We should be in WAIT_FLUSH, 2
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.WAIT_FLUSH, toVersion), fht1.SystemState));

            // Expect checkpoint completion callback
            f.checkpointCallbackExpectation = 1;

            uc1.Refresh();

            // Completion callback should have been called once
            Assert.IsTrue(f.checkpointCallbackExpectation == 0);

            // We should be in PERSISTENCE_CALLBACK, 2
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.PERSISTENCE_CALLBACK, toVersion), fht1.SystemState));

            s2.Refresh();

            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.REST, toVersion), fht1.SystemState));


            // Dispose session s2; does not move state machine forward
            s2.Dispose();

            uc1.SuspendThread();
            uc1.Dispose();
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
            uc1 = s1.GetUnsafeContext();
            uc1.ResumeThread();

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

                    if (status.Pending)
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

    public class SimpleFunctions : SimpleFunctions<AdId, NumClicks, Empty>
    {
        public int checkpointCallbackExpectation = 0;

        public override void CheckpointCompletionCallback(int sessionID, string sessionName, CommitPoint commitPoint)
        {
            switch (checkpointCallbackExpectation)
            {
                case 0:
                    Assert.Fail("Unexpected checkpoint callback");
                    break;
                default:
                    Interlocked.Decrement(ref checkpointCallbackExpectation);
                    break;
            }
        }

        public override void ReadCompletionCallback(ref AdId key, ref NumClicks input, ref NumClicks output, Empty ctx, Status status, RecordMetadata recordMetadata)
        {
            Assert.IsTrue(status.Found);
            Assert.AreEqual(key.adId, output.numClicks);
        }
    }

    public class TestCallback : IStateMachineCallback
    {
        private readonly HashSet<SystemState> invokedStates = new();


        public void BeforeEnteringState<Key1, Value>(SystemState next, FasterKV<Key1, Value> faster)
        {
            Assert.IsFalse(invokedStates.Contains(next));
            invokedStates.Add(next);
        }

        public void CheckInvoked(SystemState state)
        {
            Assert.IsTrue(invokedStates.Contains(state));
        }
    }
}
