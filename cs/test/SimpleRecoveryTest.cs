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

namespace FASTER.test.recovery.sumstore.simple
{

    [TestFixture]
    internal class SimpleRecoveryTests
    {
        private FasterKV<AdId, NumClicks, Input, Output, Empty, SimpleFunctions> fht1;
        private FasterKV<AdId, NumClicks, Input, Output, Empty, SimpleFunctions> fht2;
        private IDevice log;


        [Test]
        public void SimpleRecoveryTest1()
        {
            log = Devices.CreateLogDevice(TestContext.CurrentContext.TestDirectory + "\\hlog", deleteOnClose: true);

            Directory.CreateDirectory(TestContext.CurrentContext.TestDirectory + "\\checkpoints");

            fht1 = new FasterKV
                <AdId, NumClicks, Input, Output, Empty, SimpleFunctions>
                (128, new SimpleFunctions(),
                logSettings: new LogSettings { LogDevice = log, MutableFraction = 0.1, MemorySizeBits = 29 },
                checkpointSettings: new CheckpointSettings { CheckpointDir = TestContext.CurrentContext.TestDirectory + "\\checkpoints", CheckPointType = CheckpointType.Snapshot }
                );

            fht2 = new FasterKV
                <AdId, NumClicks, Input, Output, Empty, SimpleFunctions>
                (128, new SimpleFunctions(),
                logSettings: new LogSettings { LogDevice = log, MutableFraction = 0.1, MemorySizeBits = 29 },
                checkpointSettings: new CheckpointSettings { CheckpointDir = TestContext.CurrentContext.TestDirectory + "\\checkpoints", CheckPointType = CheckpointType.Snapshot }
                );


            int numOps = 5000;
            var inputArray = new AdId[numOps];
            for (int i = 0; i < numOps; i++)
            {
                inputArray[i].adId = i;
            }

            NumClicks value;
            Input inputArg = default(Input);
            Output output = default(Output);

            fht1.StartSession();
            for (int key = 0; key < numOps; key++)
            {
                value.numClicks = key;
                fht1.Upsert(ref inputArray[key], ref value, Empty.Default, 0);
            }
            fht1.TakeFullCheckpoint(out Guid token);
            fht1.CompleteCheckpoint(true);
            fht1.StopSession();

            fht2.Recover(token);
            fht2.StartSession();
            for (int key = 0; key < numOps; key++)
            {
                var status = fht2.Read(ref inputArray[key], ref inputArg, ref output, Empty.Default, 0);

                if (status == Status.PENDING)
                    fht2.CompletePending(true);
                else
                {
                    Assert.IsTrue(output.value.numClicks == key);
                }
            }
            fht2.StopSession();

            log.Close();
            fht1.Dispose();
            fht2.Dispose();
            new DirectoryInfo(TestContext.CurrentContext.TestDirectory + "\\checkpoints").Delete(true);
        }

        [Test]
        public void SimpleRecoveryTest2()
        {
            log = Devices.CreateLogDevice(TestContext.CurrentContext.TestDirectory + "\\hlog", deleteOnClose: true);

            Directory.CreateDirectory(TestContext.CurrentContext.TestDirectory + "\\checkpoints");

            fht1 = new FasterKV
                <AdId, NumClicks, Input, Output, Empty, SimpleFunctions>
                (128, new SimpleFunctions(),
                logSettings: new LogSettings { LogDevice = log, MutableFraction = 0.1, MemorySizeBits = 29 },
                checkpointSettings: new CheckpointSettings { CheckpointDir = TestContext.CurrentContext.TestDirectory + "\\checkpoints", CheckPointType = CheckpointType.FoldOver }
                );

            fht2 = new FasterKV
                <AdId, NumClicks, Input, Output, Empty, SimpleFunctions>
                (128, new SimpleFunctions(),
                logSettings: new LogSettings { LogDevice = log, MutableFraction = 0.1, MemorySizeBits = 29 },
                checkpointSettings: new CheckpointSettings { CheckpointDir = TestContext.CurrentContext.TestDirectory + "\\checkpoints", CheckPointType = CheckpointType.FoldOver }
                );


            int numOps = 5000;
            var inputArray = new AdId[numOps];
            for (int i = 0; i < numOps; i++)
            {
                inputArray[i].adId = i;
            }

            NumClicks value;
            Input inputArg = default(Input);
            Output output = default(Output);

            fht1.StartSession();
            for (int key = 0; key < numOps; key++)
            {
                value.numClicks = key;
                fht1.Upsert(ref inputArray[key], ref value, Empty.Default, 0);
            }
            fht1.TakeFullCheckpoint(out Guid token);
            fht1.CompleteCheckpoint(true);
            fht1.StopSession();

            fht2.Recover(token);
            fht2.StartSession();
            for (int key = 0; key < numOps; key++)
            {
                var status = fht2.Read(ref inputArray[key], ref inputArg, ref output, Empty.Default, 0);

                if (status == Status.PENDING)
                    fht2.CompletePending(true);
                else
                {
                    Assert.IsTrue(output.value.numClicks == key);
                }
            }
            fht2.StopSession();

            log.Close();
            fht1.Dispose();
            fht2.Dispose();
            new DirectoryInfo(TestContext.CurrentContext.TestDirectory + "\\checkpoints").Delete(true);
        }
    }

    public class SimpleFunctions : IFunctions<AdId, NumClicks, Input, Output, Empty>
    {
        public void RMWCompletionCallback(ref AdId key, ref Input input, Empty ctx, Status status)
        {
        }

        public void ReadCompletionCallback(ref AdId key, ref Input input, ref Output output, Empty ctx, Status status)
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

        public void CheckpointCompletionCallback(Guid sessionId, long serialNum)
        {
            Console.WriteLine("Session {0} reports persistence until {1}", sessionId, serialNum);
        }

        // Read functions
        public void SingleReader(ref AdId key, ref Input input, ref NumClicks value, ref Output dst)
        {
            dst.value = value;
        }

        public void ConcurrentReader(ref AdId key, ref Input input, ref NumClicks value, ref Output dst)
        {
            dst.value = value;
        }

        // Upsert functions
        public void SingleWriter(ref AdId key, ref NumClicks src, ref NumClicks dst)
        {
            dst = src;
        }

        public void ConcurrentWriter(ref AdId key, ref NumClicks src, ref NumClicks dst)
        {
            dst = src;
        }

        // RMW functions
        public void InitialUpdater(ref AdId key, ref Input input, ref NumClicks value)
        {
            value = input.numClicks;
        }

        public void InPlaceUpdater(ref AdId key, ref Input input, ref NumClicks value)
        {
            Interlocked.Add(ref value.numClicks, input.numClicks.numClicks);
        }

        public void CopyUpdater(ref AdId key, ref Input input, ref NumClicks oldValue, ref NumClicks newValue)
        {
            newValue.numClicks += oldValue.numClicks + input.numClicks.numClicks;
        }
    }
}