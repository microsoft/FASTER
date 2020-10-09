// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Threading;
using FASTER.core;
using System.IO;
using NUnit.Framework;
using FASTER.devices;

namespace FASTER.test.recovery.sumstore.simple
{

    [TestFixture]
    public class RecoveryTests
    {
        private FasterKV<AdId, NumClicks> fht1;
        private FasterKV<AdId, NumClicks> fht2;
        private IDevice log;
        public const string EMULATED_STORAGE_STRING = "UseDevelopmentStorage=true;";
        public const string TEST_CONTAINER = "checkpoints4";


        [TestCase(CheckpointType.FoldOver)]
        [TestCase(CheckpointType.Snapshot)]
        public void PageBlobSimpleRecoveryTest(CheckpointType checkpointType)
        {
            if ("yes".Equals(Environment.GetEnvironmentVariable("RunAzureTests")))
            {
                ICheckpointManager checkpointManager = new DeviceLogCommitCheckpointManager(
                    new AzureStorageNamedDeviceFactory(EMULATED_STORAGE_STRING),
                    new DefaultCheckpointNamingScheme($"{TEST_CONTAINER}/PageBlobSimpleRecoveryTest"));
                SimpleRecoveryTest1(checkpointType, checkpointManager);
                checkpointManager.PurgeAll();
                checkpointManager.Dispose();
            }
        }

        [TestCase(CheckpointType.FoldOver)]
        [TestCase(CheckpointType.Snapshot)]
        public void LocalDeviceSimpleRecoveryTest(CheckpointType checkpointType)
        {
            ICheckpointManager checkpointManager = new DeviceLogCommitCheckpointManager(
                new LocalStorageNamedDeviceFactory(),
                new DefaultCheckpointNamingScheme($"{TEST_CONTAINER}/PageBlobSimpleRecoveryTest"));
            SimpleRecoveryTest1(checkpointType, checkpointManager);
            checkpointManager.PurgeAll();
            checkpointManager.Dispose();
        }


        [TestCase(CheckpointType.FoldOver)]
        [TestCase(CheckpointType.Snapshot)]
        public void SimpleRecoveryTest1(CheckpointType checkpointType, ICheckpointManager checkpointManager = null)
        {
            string checkpointDir = TestContext.CurrentContext.TestDirectory + $"\\{TEST_CONTAINER}";

            if (checkpointManager != null)
                checkpointDir = null;

            log = Devices.CreateLogDevice(TestContext.CurrentContext.TestDirectory + "\\SimpleRecoveryTest1.log", deleteOnClose: true);

            fht1 = new FasterKV
                <AdId, NumClicks>
                (128,
                logSettings: new LogSettings { LogDevice = log, MutableFraction = 0.1, MemorySizeBits = 29 },
                checkpointSettings: new CheckpointSettings { CheckpointDir = checkpointDir, CheckpointManager = checkpointManager, CheckPointType = checkpointType }
                );

            fht2 = new FasterKV
                <AdId, NumClicks>
                (128,
                logSettings: new LogSettings { LogDevice = log, MutableFraction = 0.1, MemorySizeBits = 29 },
                checkpointSettings: new CheckpointSettings { CheckpointDir = checkpointDir, CheckpointManager = checkpointManager, CheckPointType = checkpointType }
                );


            int numOps = 5000;
            var inputArray = new AdId[numOps];
            for (int i = 0; i < numOps; i++)
            {
                inputArray[i].adId = i;
            }

            NumClicks value;
            AdInput inputArg = default;
            Output output = default;

            var session1 = fht1.NewSession(new SimpleFunctions());
            for (int key = 0; key < numOps; key++)
            {
                value.numClicks = key;
                session1.Upsert(ref inputArray[key], ref value, Empty.Default, 0);
            }
            fht1.TakeFullCheckpoint(out Guid token);
            fht1.CompleteCheckpointAsync().GetAwaiter().GetResult();
            session1.Dispose();

            fht2.Recover(token);

            var session2 = fht2.NewSession(new SimpleFunctions());
            for (int key = 0; key < numOps; key++)
            {
                var status = session2.Read(ref inputArray[key], ref inputArg, ref output, Empty.Default, 0);

                if (status == Status.PENDING)
                    session2.CompletePending(true);
                else
                {
                    Assert.IsTrue(output.value.numClicks == key);
                }
            }
            session2.Dispose();

            log.Dispose();
            fht1.Dispose();
            fht2.Dispose();

            if (checkpointManager == null)
                new DirectoryInfo(checkpointDir).Delete(true);
        }

        [TestCase(CheckpointType.FoldOver)]
        [TestCase(CheckpointType.Snapshot)]
        public void SimpleRecoveryTest2(CheckpointType checkpointType)
        {
            var checkpointManager = new DeviceLogCommitCheckpointManager(new LocalStorageNamedDeviceFactory(), new DefaultCheckpointNamingScheme(TestContext.CurrentContext.TestDirectory + "\\checkpoints4"), false);

            log = Devices.CreateLogDevice(TestContext.CurrentContext.TestDirectory + "\\SimpleRecoveryTest2.log", deleteOnClose: true);

            // Directory.CreateDirectory(TestContext.CurrentContext.TestDirectory + "\\checkpoints4");

            fht1 = new FasterKV
                <AdId, NumClicks>
                (128,
                logSettings: new LogSettings { LogDevice = log, MutableFraction = 0.1, MemorySizeBits = 29 },
                checkpointSettings: new CheckpointSettings { CheckpointManager = checkpointManager, CheckPointType = checkpointType }
                );

            fht2 = new FasterKV
                <AdId, NumClicks>
                (128,
                logSettings: new LogSettings { LogDevice = log, MutableFraction = 0.1, MemorySizeBits = 29 },
                checkpointSettings: new CheckpointSettings { CheckpointManager = checkpointManager, CheckPointType = checkpointType }
                );


            int numOps = 5000;
            var inputArray = new AdId[numOps];
            for (int i = 0; i < numOps; i++)
            {
                inputArray[i].adId = i;
            }

            NumClicks value;
            AdInput inputArg = default;
            Output output = default;

            var session1 = fht1.NewSession(new SimpleFunctions());
            for (int key = 0; key < numOps; key++)
            {
                value.numClicks = key;
                session1.Upsert(ref inputArray[key], ref value, Empty.Default, 0);
            }
            fht1.TakeFullCheckpoint(out Guid token);
            fht1.CompleteCheckpointAsync().GetAwaiter().GetResult();
            session1.Dispose();

            fht2.Recover(token);

            var session2 = fht2.NewSession(new SimpleFunctions());
            for (int key = 0; key < numOps; key++)
            {
                var status = session2.Read(ref inputArray[key], ref inputArg, ref output, Empty.Default, 0);

                if (status == Status.PENDING)
                    session2.CompletePending(true);
                else
                {
                    Assert.IsTrue(output.value.numClicks == key);
                }
            }
            session2.Dispose();

            log.Dispose();
            fht1.Dispose();
            fht2.Dispose();
            checkpointManager.Dispose();

            new DirectoryInfo(TestContext.CurrentContext.TestDirectory + "\\checkpoints4").Delete(true);
        }

        [Test]
        public void ShouldRecoverBeginAddress()
        {
            log = Devices.CreateLogDevice(TestContext.CurrentContext.TestDirectory + "\\SimpleRecoveryTest2.log", deleteOnClose: true);

            Directory.CreateDirectory(TestContext.CurrentContext.TestDirectory + "\\checkpoints6");

            fht1 = new FasterKV
                <AdId, NumClicks>
                (128,
                logSettings: new LogSettings { LogDevice = log, MutableFraction = 0.1, MemorySizeBits = 29 },
                checkpointSettings: new CheckpointSettings { CheckpointDir = TestContext.CurrentContext.TestDirectory + "\\checkpoints6", CheckPointType = CheckpointType.FoldOver }
                );

            fht2 = new FasterKV
                <AdId, NumClicks>
                (128,
                logSettings: new LogSettings { LogDevice = log, MutableFraction = 0.1, MemorySizeBits = 29 },
                checkpointSettings: new CheckpointSettings { CheckpointDir = TestContext.CurrentContext.TestDirectory + "\\checkpoints6", CheckPointType = CheckpointType.FoldOver }
                );


            int numOps = 5000;
            var inputArray = new AdId[numOps];
            for (int i = 0; i < numOps; i++)
            {
                inputArray[i].adId = i;
            }

            NumClicks value;

            var session1 = fht1.NewSession(new SimpleFunctions());
            var address = 0L;
            for (int key = 0; key < numOps; key++)
            {
                value.numClicks = key;
                session1.Upsert(ref inputArray[key], ref value, Empty.Default, 0);

                if (key == 2999)
                    address = fht1.Log.TailAddress;
            }

            fht1.Log.ShiftBeginAddress(address);

            fht1.TakeFullCheckpoint(out Guid token);
            fht1.CompleteCheckpointAsync().GetAwaiter().GetResult();
            session1.Dispose();

            fht2.Recover(token);

            Assert.AreEqual(address, fht2.Log.BeginAddress);

            log.Dispose();
            fht1.Dispose();
            fht2.Dispose();
            new DirectoryInfo(TestContext.CurrentContext.TestDirectory + "\\checkpoints6").Delete(true);
        }
    }

    public class SimpleFunctions : IFunctions<AdId, NumClicks, AdInput, Output, Empty>
    {
        public void RMWCompletionCallback(ref AdId key, ref AdInput input, Empty ctx, Status status)
        {
        }

        public void ReadCompletionCallback(ref AdId key, ref AdInput input, ref Output output, Empty ctx, Status status, RecordInfo _)
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

        public bool NeedCopyUpdate(ref AdId key, ref AdInput input, ref NumClicks oldValue) => true;

        public void CopyUpdater(ref AdId key, ref AdInput input, ref NumClicks oldValue, ref NumClicks newValue)
        {
            newValue.numClicks += oldValue.numClicks + input.numClicks.numClicks;
        }
    }
}
 