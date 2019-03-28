// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using FASTER.core;
using System.IO;
using NUnit.Framework;
using System.Diagnostics;

namespace FASTER.test.recovery.sumstore.recover_continue
{
    [TestFixture]
    internal class RecoverContinueTests
    {
        private FasterKV<AdId, NumClicks, Input, Output, Empty, SimpleFunctions> fht1;
        private FasterKV<AdId, NumClicks, Input, Output, Empty, SimpleFunctions> fht2;
        private FasterKV<AdId, NumClicks, Input, Output, Empty, SimpleFunctions> fht3;
        private IDevice log;
        private int numOps;

        [SetUp]
        public void Setup()
        {
            log = Devices.CreateLogDevice(TestContext.CurrentContext.TestDirectory + "\\RecoverContinueTests.log", deleteOnClose: true);
            Directory.CreateDirectory(TestContext.CurrentContext.TestDirectory + "\\checkpoints3");

            fht1 = new FasterKV
                <AdId, NumClicks, Input, Output, Empty, SimpleFunctions>
                (128, new SimpleFunctions(),
                logSettings: new LogSettings { LogDevice = log, MutableFraction = 0.1, MemorySizeBits = 29 },
                checkpointSettings: new CheckpointSettings { CheckpointDir = TestContext.CurrentContext.TestDirectory + "\\checkpoints3", CheckPointType = CheckpointType.Snapshot }
                );

            fht2 = new FasterKV
                <AdId, NumClicks, Input, Output, Empty, SimpleFunctions>
                (128, new SimpleFunctions(),
                logSettings: new LogSettings { LogDevice = log, MutableFraction = 0.1, MemorySizeBits = 29 },
                checkpointSettings: new CheckpointSettings { CheckpointDir = TestContext.CurrentContext.TestDirectory + "\\checkpoints3", CheckPointType = CheckpointType.Snapshot }
                );

            fht3 = new FasterKV
                <AdId, NumClicks, Input, Output, Empty, SimpleFunctions>
                (128, new SimpleFunctions(),
                logSettings: new LogSettings { LogDevice = log, MutableFraction = 0.1, MemorySizeBits = 29 },
                checkpointSettings: new CheckpointSettings { CheckpointDir = TestContext.CurrentContext.TestDirectory + "\\checkpoints3", CheckPointType = CheckpointType.Snapshot }
                );

            numOps = 5000;
        }

        [TearDown]
        public void TearDown()
        {
            fht1.Dispose();
            fht2.Dispose();
            fht3.Dispose();
            fht1 = null;
            fht2 = null;
            fht3 = null;
            log.Close();
            Directory.Delete(TestContext.CurrentContext.TestDirectory + "\\checkpoints3", true);
        }

        public static void DeleteDirectory(string path)
        {
            foreach (string directory in Directory.GetDirectories(path))
            {
                DeleteDirectory(directory);
            }

            try
            {
                Directory.Delete(path, true);
            }
            catch (IOException)
            {
                Directory.Delete(path, true);
            }
            catch (UnauthorizedAccessException)
            {
                Directory.Delete(path, true);
            }
        }

        [Test]
        public void RecoverContinueTest()
        {

            long sno = 0;

            Guid sessionToken = fht1.StartSession();
            IncrementAllValues(ref fht1, ref sno);
            fht1.TakeFullCheckpoint(out Guid token);
            fht1.CompleteCheckpoint(true);
            fht1.StopSession();

            // Check if values after checkpoint are correct
            fht1.StartSession();
            CheckAllValues(ref fht1, 1);
            fht1.StopSession();

            // Recover and check if recovered values are correct
            fht2.Recover();
            fht2.StartSession();
            CheckAllValues(ref fht2, 1);
            fht2.StopSession();

            // Continue and increment values
            long newSno = fht2.ContinueSession(sessionToken);
            Assert.IsTrue(newSno == sno - 1);
            IncrementAllValues(ref fht2, ref sno);
            fht2.TakeFullCheckpoint(out token);
            fht2.CompleteCheckpoint(true);
            fht2.StopSession();

            // Check if values after continue checkpoint are correct
            fht2.StartSession();
            CheckAllValues(ref fht2, 2);
            fht2.StopSession();


            // Recover and check if recovered values are correct
            fht3.Recover();
            long newSno2 = fht3.ContinueSession(sessionToken);
            Assert.IsTrue(newSno2 == sno - 1);
            CheckAllValues(ref fht3, 2);
            fht3.StopSession();
        }

        private void CheckAllValues(
            ref FasterKV<AdId, NumClicks, Input, Output, Empty, SimpleFunctions> fht,
            int value)
        {
            Input inputArg = default(Input);
            Output outputArg = default(Output);
            for (var key = 0; key < numOps; key++)
            {
                inputArg.adId.adId = key;
                var status = fht.Read(ref inputArg.adId, ref inputArg, ref outputArg, Empty.Default, key);

                if (status == Status.PENDING)
                    fht2.CompletePending(true);
                else
                {
                    Assert.IsTrue(outputArg.value.numClicks == value);
                }
            }

            fht.CompletePending(true);
        }

        private void IncrementAllValues(
            ref FasterKV<AdId, NumClicks, Input, Output, Empty, SimpleFunctions> fht, 
            ref long sno)
        {
            Input inputArg = default(Input);
            for (int key = 0; key < numOps; key++, sno++)
            {
                inputArg.adId.adId = key;
                inputArg.numClicks.numClicks = 1;
                fht.RMW(ref inputArg.adId, ref inputArg, Empty.Default, sno);
            }
            fht.CompletePending(true);
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
