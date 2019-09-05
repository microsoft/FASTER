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

namespace FASTER.test.async
{

    [TestFixture]
    public class RecoveryTests
    {
        private FasterKV<AdId, NumClicks, AdInput, Output, Empty, SimpleFunctions> fht1;
        private FasterKV<AdId, NumClicks, AdInput, Output, Empty, SimpleFunctions> fht2;
        private IDevice log;


        [TestCase(CheckpointType.FoldOver)]
        [TestCase(CheckpointType.Snapshot)]
        public async Task AsyncRecoveryTest1(CheckpointType checkpointType)
        {
            log = Devices.CreateLogDevice(TestContext.CurrentContext.TestDirectory + "\\SimpleRecoveryTest2.log", deleteOnClose: true);

            Directory.CreateDirectory(TestContext.CurrentContext.TestDirectory + "\\checkpoints4");

            fht1 = new FasterKV
                <AdId, NumClicks, AdInput, Output, Empty, SimpleFunctions>
                (128, new SimpleFunctions(),
                logSettings: new LogSettings { LogDevice = log, MutableFraction = 0.1, MemorySizeBits = 29 },
                checkpointSettings: new CheckpointSettings { CheckpointDir = TestContext.CurrentContext.TestDirectory + "\\checkpoints4", CheckPointType = checkpointType }
                );

            fht2 = new FasterKV
                <AdId, NumClicks, AdInput, Output, Empty, SimpleFunctions>
                (128, new SimpleFunctions(),
                logSettings: new LogSettings { LogDevice = log, MutableFraction = 0.1, MemorySizeBits = 29 },
                checkpointSettings: new CheckpointSettings { CheckpointDir = TestContext.CurrentContext.TestDirectory + "\\checkpoints4", CheckPointType = checkpointType }
                );


            int numOps = 5000;
            var inputArray = new AdId[numOps];
            for (int i = 0; i < numOps; i++)
            {
                inputArray[i].adId = i;
            }

            NumClicks value;
            AdInput inputArg = default(AdInput);
            Output output = default(Output);

            var s0 = fht1.StartClientSession(); // leave dormant

            var s1 = fht1.StartClientSession();

            // fht1.StartSession();
            for (int key = 0; key < numOps; key++)
            {
                value.numClicks = key;
                s1.Upsert(ref inputArray[key], ref value, Empty.Default, key);
            }

            fht1.TakeFullCheckpoint(out Guid token);

            var s2 = fht1.StartClientSession();

            // s1 becomes dormant
            await s2.CompleteCheckpointAsync();

            s2.Dispose();
            s1.Dispose(); // should receive persistence callback
            s0.UnsafeResumeThread(); // should receive persistence callback
            s0.Dispose();
            fht1.Dispose();

            fht2.Recover(token); // sync, does not require session

            var guid = s1.ID;
            using (var s3 = fht2.ContinueClientSession(guid, out CommitPoint lsn))
            {
                Assert.IsTrue(lsn.UntilSerialNo == numOps - 1);

                for (int key = 0; key < numOps; key++)
                {
                    var status = s3.Read(ref inputArray[key], ref inputArg, ref output, Empty.Default, 0);

                    if (status == Status.PENDING)
                        s3.TryCompletePending(true);
                    else
                    {
                        Assert.IsTrue(output.value.numClicks == key);
                    }
                }
            }

            fht2.Dispose();
            log.Close();
            new DirectoryInfo(TestContext.CurrentContext.TestDirectory + "\\checkpoints4").Delete(true);
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

        public void CheckpointCompletionCallback(Guid sessionId, CommitPoint commitPoint)
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
 