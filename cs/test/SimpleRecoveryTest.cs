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
        private ICustomFaster fht1;
        private ICustomFaster fht2;
        private IDevice log;


        [Test]
        public unsafe void SimpleRecoveryTest1()
        {
            log = FasterFactory.CreateLogDevice(TestContext.CurrentContext.TestDirectory + "\\hlog", deleteOnClose: true);

            Directory.CreateDirectory(TestContext.CurrentContext.TestDirectory + "\\checkpoints");

            fht1 = FasterFactory.Create
                <AdId, NumClicks, Input, Output, Empty, SimpleFunctions, ICustomFaster>
                (indexSizeBuckets: 128,
                logSettings: new LogSettings { LogDevice = log, MutableFraction = 0.1, PageSizeBits = 9, MemorySizeBits = 13 },
                checkpointSettings: new CheckpointSettings { CheckpointDir = TestContext.CurrentContext.TestDirectory + "\\checkpoints", CheckPointType = CheckpointType.Snapshot }
                );

            fht2 = FasterFactory.Create
                <AdId, NumClicks, Input, Output, Empty, SimpleFunctions, ICustomFaster>
                (indexSizeBuckets: 128,
                logSettings: new LogSettings { LogDevice = log, MutableFraction = 0.1, PageSizeBits = 9, MemorySizeBits = 13 },
                checkpointSettings: new CheckpointSettings { CheckpointDir = TestContext.CurrentContext.TestDirectory + "\\checkpoints", CheckPointType = CheckpointType.Snapshot }
                );


            int numOps = 5000;
            var inputArray = new AdId[numOps];
            for (int i = 0; i < numOps; i++)
            {
                inputArray[i].adId = i;
            }

            NumClicks value;
            Input inputArg;
            Output output;


            fixed (AdId* input = inputArray)
            {

                fht1.StartSession();
                for (int key = 0; key < numOps; key++)
                {
                    value.numClicks = key;
                    fht1.Upsert(input + key, &value, null, 0);
                }
                fht1.TakeFullCheckpoint(out Guid token);
                fht1.CompleteCheckpoint(true);
                fht1.StopSession();

                fht2.Recover(token);
                fht2.StartSession();
                for (int key = 0; key < numOps; key++)
                {
                    var status = fht2.Read(input + key, &inputArg, &output, null, 0);

                    if (status == Status.PENDING)
                        fht2.CompletePending(true);
                    else
                    {
                        Assert.IsTrue(output.value.numClicks == key);
                    }
                }
                fht2.StopSession();
            }

            log.Close();
            new DirectoryInfo(TestContext.CurrentContext.TestDirectory + "\\checkpoints").Delete(true);
        }

        [Test]
        public unsafe void SimpleRecoveryTest2()
        {
            log = FasterFactory.CreateLogDevice(TestContext.CurrentContext.TestDirectory + "\\hlog", deleteOnClose: true);

            Directory.CreateDirectory(TestContext.CurrentContext.TestDirectory + "\\checkpoints");

            fht1 = FasterFactory.Create
                <AdId, NumClicks, Input, Output, Empty, SimpleFunctions, ICustomFaster>
                (indexSizeBuckets: 128,
                logSettings: new LogSettings { LogDevice = log, MutableFraction = 0.1, PageSizeBits = 9, MemorySizeBits = 13 },
                checkpointSettings: new CheckpointSettings { CheckpointDir = TestContext.CurrentContext.TestDirectory + "\\checkpoints", CheckPointType = CheckpointType.FoldOver }
                );

            fht2 = FasterFactory.Create
                <AdId, NumClicks, Input, Output, Empty, SimpleFunctions, ICustomFaster>
                (indexSizeBuckets: 128,
                logSettings: new LogSettings { LogDevice = log, MutableFraction = 0.1, PageSizeBits = 9, MemorySizeBits = 13 },
                checkpointSettings: new CheckpointSettings { CheckpointDir = TestContext.CurrentContext.TestDirectory + "\\checkpoints", CheckPointType = CheckpointType.FoldOver }
                );


            int numOps = 5000;
            var inputArray = new AdId[numOps];
            for (int i = 0; i < numOps; i++)
            {
                inputArray[i].adId = i;
            }

            NumClicks value;
            Input inputArg;
            Output output;


            fixed (AdId* input = inputArray)
            {

                fht1.StartSession();
                for (int key = 0; key < numOps; key++)
                {
                    value.numClicks = key;
                    fht1.Upsert(input + key, &value, null, 0);
                }
                fht1.TakeFullCheckpoint(out Guid token);
                fht1.CompleteCheckpoint(true);
                fht1.StopSession();

                fht2.Recover(token);
                fht2.StartSession();
                for (int key = 0; key < numOps; key++)
                {
                    var status = fht2.Read(input + key, &inputArg, &output, null, 0);

                    if (status == Status.PENDING)
                        fht2.CompletePending(true);
                    else
                    {
                        Assert.IsTrue(output.value.numClicks == key);
                    }
                }
                fht2.StopSession();
            }

            log.Close();
            new DirectoryInfo(TestContext.CurrentContext.TestDirectory + "\\checkpoints").Delete(true);
        }


    }

    public unsafe class SimpleFunctions
    {
        public static void RMWCompletionCallback(AdId* key, Input* input, Empty* ctx, Status status)
        {
        }

        public static void ReadCompletionCallback(AdId* key, Input* input, Output* output, Empty* ctx, Status status)
        {
            Assert.IsTrue(status == Status.OK);
            Assert.IsTrue(output->value.numClicks == key->adId);
        }

        public static void UpsertCompletionCallback(AdId* key, NumClicks* input, Empty* ctx)
        {
        }

        public static void PersistenceCallback(long thread_id, long serial_num)
        {
            Console.WriteLine("Thread {0} reports persistence until {1}", thread_id, serial_num);
        }

        // Read functions
        public static void SingleReader(AdId* key, Input* input, NumClicks* value, Output* dst)
        {
            NumClicks.Copy(value, (NumClicks*)dst);
        }

        public static void ConcurrentReader(AdId* key, Input* input, NumClicks* value, Output* dst)
        {
            NumClicks.AcquireReadLock(value);
            NumClicks.Copy(value, (NumClicks*)dst);
            NumClicks.ReleaseReadLock(value);
        }

        // Upsert functions
        public static void SingleWriter(AdId* key, NumClicks* src, NumClicks* dst)
        {
            NumClicks.Copy(src, dst);
        }

        public static void ConcurrentWriter(AdId* key, NumClicks* src, NumClicks* dst)
        {
            NumClicks.AcquireWriteLock(dst);
            NumClicks.Copy(src, dst);
            NumClicks.ReleaseWriteLock(dst);
        }

        // RMW functions
        public static int InitialValueLength(AdId* key, Input* input)
        {
            return NumClicks.GetLength(default(NumClicks*));
        }

        public static void InitialUpdater(AdId* key, Input* input, NumClicks* value)
        {
            NumClicks.Copy(&input->numClicks, value);
        }

        public static void InPlaceUpdater(AdId* key, Input* input, NumClicks* value)
        {
            Interlocked.Add(ref value->numClicks, input->numClicks.numClicks);
        }

        public static void CopyUpdater(AdId* key, Input* input, NumClicks* oldValue, NumClicks* newValue)
        {
            newValue->numClicks += oldValue->numClicks + input->numClicks.numClicks;
        }
    }
}
