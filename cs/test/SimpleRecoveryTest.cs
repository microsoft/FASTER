// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Threading;
using System.Threading.Tasks;
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
        public const string TEST_CONTAINER = "checkpoints4444";

        [Test]
        [Category("FasterKV")]
        public async ValueTask PageBlobSimpleRecoveryTest([Values]CheckpointType checkpointType, [Values]bool isAsync)
        {
            if ("yes".Equals(Environment.GetEnvironmentVariable("RunAzureTests")))
            {
                ICheckpointManager checkpointManager = new DeviceLogCommitCheckpointManager(
                    new AzureStorageNamedDeviceFactory(EMULATED_STORAGE_STRING),
                    new DefaultCheckpointNamingScheme($"{TEST_CONTAINER}/PageBlobSimpleRecoveryTest"));
                await SimpleRecoveryTest1_Worker(checkpointType, checkpointManager, isAsync);
                checkpointManager.PurgeAll();
                checkpointManager.Dispose();
            }
        }

        [Test]
        [Category("FasterKV")]
        public async ValueTask LocalDeviceSimpleRecoveryTest([Values] CheckpointType checkpointType, [Values] bool isAsync)
        {
            ICheckpointManager checkpointManager = new DeviceLogCommitCheckpointManager(
                new LocalStorageNamedDeviceFactory(),
                new DefaultCheckpointNamingScheme($"{TEST_CONTAINER}/PageBlobSimpleRecoveryTest"));
            await SimpleRecoveryTest1_Worker(checkpointType, checkpointManager, isAsync);
            checkpointManager.PurgeAll();
            checkpointManager.Dispose();
        }


        [Test]
        [Category("FasterKV")]
        public async ValueTask SimpleRecoveryTest1([Values]CheckpointType checkpointType, [Values]bool isAsync)
        {
            await SimpleRecoveryTest1_Worker(checkpointType, null, isAsync);
        }

        private async ValueTask SimpleRecoveryTest1_Worker(CheckpointType checkpointType, ICheckpointManager checkpointManager, bool isAsync)
        {
            string checkpointDir = TestContext.CurrentContext.TestDirectory + $"/{TEST_CONTAINER}";

            if (checkpointManager != null)
                checkpointDir = null;

            log = Devices.CreateLogDevice(TestContext.CurrentContext.TestDirectory + "/SimpleRecoveryTest1.log", deleteOnClose: true);

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

            var session1 = fht1.NewSession(new AdSimpleFunctions());
            for (int key = 0; key < numOps; key++)
            {
                value.numClicks = key;
                session1.Upsert(ref inputArray[key], ref value, Empty.Default, 0);
            }
            fht1.TakeFullCheckpoint(out Guid token);
            fht1.CompleteCheckpointAsync().GetAwaiter().GetResult();
            session1.Dispose();

            if (isAsync)
                await fht2.RecoverAsync(token);
            else
                fht2.Recover(token);

            var session2 = fht2.NewSession(new AdSimpleFunctions());
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

        [Test]
        [Category("FasterKV")]
        public async ValueTask SimpleRecoveryTest2([Values]CheckpointType checkpointType, [Values]bool isAsync)
        {
            var checkpointManager = new DeviceLogCommitCheckpointManager(new LocalStorageNamedDeviceFactory(), new DefaultCheckpointNamingScheme(TestContext.CurrentContext.TestDirectory + "/checkpoints4"), false);

            log = Devices.CreateLogDevice(TestContext.CurrentContext.TestDirectory + "/SimpleRecoveryTest2.log", deleteOnClose: true);

            // Directory.CreateDirectory(TestContext.CurrentContext.TestDirectory + "/checkpoints4");

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

            var session1 = fht1.NewSession(new AdSimpleFunctions());
            for (int key = 0; key < numOps; key++)
            {
                value.numClicks = key;
                session1.Upsert(ref inputArray[key], ref value, Empty.Default, 0);
            }
            fht1.TakeFullCheckpoint(out Guid token);
            fht1.CompleteCheckpointAsync().GetAwaiter().GetResult();
            session1.Dispose();

            if (isAsync)
                await fht2.RecoverAsync(token);
            else 
                fht2.Recover(token);

            var session2 = fht2.NewSession(new AdSimpleFunctions());
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

            new DirectoryInfo(TestContext.CurrentContext.TestDirectory + "/checkpoints4").Delete(true);
        }

        [Test]
        [Category("FasterKV")]
        public async ValueTask ShouldRecoverBeginAddress([Values]bool isAsync)
        {
            log = Devices.CreateLogDevice(TestContext.CurrentContext.TestDirectory + "/SimpleRecoveryTest2.log", deleteOnClose: true);

            Directory.CreateDirectory(TestContext.CurrentContext.TestDirectory + "/checkpoints6");

            fht1 = new FasterKV
                <AdId, NumClicks>
                (128,
                logSettings: new LogSettings { LogDevice = log, MutableFraction = 0.1, MemorySizeBits = 29 },
                checkpointSettings: new CheckpointSettings { CheckpointDir = TestContext.CurrentContext.TestDirectory + "/checkpoints6", CheckPointType = CheckpointType.FoldOver }
                );

            fht2 = new FasterKV
                <AdId, NumClicks>
                (128,
                logSettings: new LogSettings { LogDevice = log, MutableFraction = 0.1, MemorySizeBits = 29 },
                checkpointSettings: new CheckpointSettings { CheckpointDir = TestContext.CurrentContext.TestDirectory + "/checkpoints6", CheckPointType = CheckpointType.FoldOver }
                );


            int numOps = 5000;
            var inputArray = new AdId[numOps];
            for (int i = 0; i < numOps; i++)
            {
                inputArray[i].adId = i;
            }

            NumClicks value;

            var session1 = fht1.NewSession(new AdSimpleFunctions());
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

            if (isAsync)
                await fht2.RecoverAsync(token);
            else
                fht2.Recover(token);

            Assert.AreEqual(address, fht2.Log.BeginAddress);

            log.Dispose();
            fht1.Dispose();
            fht2.Dispose();
            new DirectoryInfo(TestContext.CurrentContext.TestDirectory + "/checkpoints6").Delete(true);
        }
    }

    public class AdSimpleFunctions : FunctionsBase<AdId, NumClicks, AdInput, Output, Empty>
    {
        public override void ReadCompletionCallback(ref AdId key, ref AdInput input, ref Output output, Empty ctx, Status status)
        {
            Assert.IsTrue(status == Status.OK);
            Assert.IsTrue(output.value.numClicks == key.adId);
        }

        // Read functions
        public override void SingleReader(ref AdId key, ref AdInput input, ref NumClicks value, ref Output dst) => dst.value = value;

        public override void ConcurrentReader(ref AdId key, ref AdInput input, ref NumClicks value, ref Output dst) => dst.value = value;

        // RMW functions
        public override void InitialUpdater(ref AdId key, ref AdInput input, ref NumClicks value, ref Output output)
        {
            value = input.numClicks;
        }

        public override bool InPlaceUpdater(ref AdId key, ref AdInput input, ref NumClicks value, ref Output output)
        {
            Interlocked.Add(ref value.numClicks, input.numClicks.numClicks);
            return true;
        }

        public override bool NeedCopyUpdate(ref AdId key, ref AdInput input, ref NumClicks oldValue, ref Output output) => true;

        public override void CopyUpdater(ref AdId key, ref AdInput input, ref NumClicks oldValue, ref NumClicks newValue, ref Output output)
        {
            newValue.numClicks += oldValue.numClicks + input.numClicks.numClicks;
        }
    }
}
 