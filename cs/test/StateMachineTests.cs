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
        FasterKV<AdId, NumClicks, AdInput, Output, Empty, SimpleFunctions> fht1;
        AutoResetEvent ev = new AutoResetEvent(false);
        int numOps = 5000;
        AdId[] inputArray;
        AsyncQueue<string> q = new AsyncQueue<string>();

        private void SecondSession()
        {
            var s2 = fht1.NewSession(null, true);
            ev.Set();

            while (true)
            {
                var cmd = q.DequeueAsync().Result;
                switch (cmd)
                {
                    case "refresh":
                        s2.Refresh();
                        ev.Set();
                        break;
                    case "dispose":
                        s2.Dispose();
                        ev.Set();
                        return;
                    default:
                        throw new Exception("Unsupported command");
                }
            }
        }

        [TestCase]
        public void StateMachineTest1()
        {
            IDevice log;

            var checkpointType = CheckpointType.FoldOver;

            log = Devices.CreateLogDevice(TestContext.CurrentContext.TestDirectory + "\\StateMachineTest1.log", deleteOnClose: true);

            Directory.CreateDirectory(TestContext.CurrentContext.TestDirectory + "\\checkpoints4");

            fht1 = new FasterKV
                <AdId, NumClicks, AdInput, Output, Empty, SimpleFunctions>
                (128, new SimpleFunctions(),
                logSettings: new LogSettings { LogDevice = log, MutableFraction = 0.1, PageSizeBits = 10, MemorySizeBits = 13 },
                checkpointSettings: new CheckpointSettings { CheckpointDir = TestContext.CurrentContext.TestDirectory + "\\checkpoints4", CheckPointType = checkpointType }
                );

            fht1.TakeIndexCheckpoint(out _);

            inputArray = new AdId[numOps];
            for (int i = 0; i < numOps; i++)
            {
                inputArray[i].adId = i;
            }

            NumClicks value;


            var s1 = fht1.NewSession("foo", threadAffinitized: true);

            for (int key = 0; key < numOps; key++)
            {
                value.numClicks = key;
                s1.Upsert(ref inputArray[key], ref value, Empty.Default, key);
            }

            // Ensure state machine needs no I/O wait during WAIT_FLUSH
            fht1.Log.ShiftReadOnlyAddress(fht1.Log.TailAddress, true);

            var ss = new Thread(() => SecondSession());
            ss.Start();
            ev.WaitOne();

            // We should be in REST, 1
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.REST, 1), fht1.SystemState));

            fht1.TakeHybridLogCheckpoint(out _);

            // We should be in PREPARE, 1
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.PREPARE, 1), fht1.SystemState));

            // Refresh session s2
            OtherSession("refresh");

            // s1 has not refreshed, so we should still be in PREPARE, 1
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.PREPARE, 1), fht1.SystemState));

            // Refresh s1
            s1.Refresh();

            // We should now be in IN_PROGRESS, 2
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.IN_PROGRESS, 2), fht1.SystemState));

            // Dispose session s2; does not move state machine forward
            OtherSession("dispose");

            // We should still be in IN_PROGRESS, 2
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.IN_PROGRESS, 2), fht1.SystemState));

            // Since s1 is the only session now, it will fast-foward state machine
            // to completion
            s1.Refresh();

            // We should be in REST, 2
            Assert.IsTrue(SystemState.Equal(SystemState.Make(Phase.REST, 2), fht1.SystemState));

            s1.Dispose();
            fht1.Dispose();

            RecoverAndTest(log);
            log.Close();
            new DirectoryInfo(TestContext.CurrentContext.TestDirectory + "\\checkpoints4").Delete(true);
        }

        private void OtherSession(string command)
        {
            q.Enqueue(command);
            ev.WaitOne();
        }

        void RecoverAndTest(IDevice log)
        {
            AdInput inputArg = default;
            Output output = default;

            var fht2 = new FasterKV
                <AdId, NumClicks, AdInput, Output, Empty, SimpleFunctions>
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
                        Assert.IsTrue(output.value.numClicks == key);
                    }
                }
            }

            fht2.Dispose();
        }
    }

    public class SimpleFunctions : IFunctions<AdId, NumClicks, AdInput, Output, Empty>
    {
        public void RMWCompletionCallback(ref AdId key, ref AdInput input, Empty ctx, Status status)
        {
        }

        public void ReadCompletionCallback(ref AdId key, ref AdInput input, ref Output output, Empty ctx, Status status)
        {
            Assert.IsTrue(status == Status.OK);
            Assert.IsTrue(output.value.numClicks == key.adId);
        }

        public void UpsertCompletionCallback(ref AdId key, ref NumClicks input, Empty ctx)
        {
        }

        public void DeleteCompletionCallback(ref AdId key, Empty ctx)
        {
        }

        public void CheckpointCompletionCallback(string sessionId, CommitPoint commitPoint)
        {
            Console.WriteLine("Session {0} reports persistence until {1}", sessionId, commitPoint.UntilSerialNo);
        }

        // Read functions
        public void SingleReader(ref AdId key, ref AdInput input, ref NumClicks value, ref Output dst)
        {
            dst.value = value;
        }

        public void ConcurrentReader(ref AdId key, ref AdInput input, ref NumClicks value, ref Output dst)
        {
            dst.value = value;
        }

        // Upsert functions
        public void SingleWriter(ref AdId key, ref NumClicks src, ref NumClicks dst)
        {
            dst = src;
        }

        public bool ConcurrentWriter(ref AdId key, ref NumClicks src, ref NumClicks dst)
        {
            dst = src;
            return true;
        }

        // RMW functions
        public void InitialUpdater(ref AdId key, ref AdInput input, ref NumClicks value)
        {
            value = input.numClicks;
        }

        public bool InPlaceUpdater(ref AdId key, ref AdInput input, ref NumClicks value)
        {
            Interlocked.Add(ref value.numClicks, input.numClicks.numClicks);
            return true;
        }

        public void CopyUpdater(ref AdId key, ref AdInput input, ref NumClicks oldValue, ref NumClicks newValue)
        {
            newValue.numClicks += oldValue.numClicks + input.numClicks.numClicks;
        }
    }
}
