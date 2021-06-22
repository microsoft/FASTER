// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Threading;
using System.Threading.Tasks;
using FASTER.core;
using NUnit.Framework;
using FASTER.devices;

namespace FASTER.test.recovery.sumstore.simple
{
    [TestFixture]
    public class RecoveryTests
    {
        const int numOps = 5000;
        AdId[] inputArray;

        string checkpointDir;
        ICheckpointManager checkpointManager;

        private FasterKV<AdId, NumClicks> fht1;
        private FasterKV<AdId, NumClicks> fht2;
        private IDevice log;

        [SetUp]
        public void Setup()
        {
            checkpointManager = default;
            checkpointDir = default;
            inputArray = new AdId[numOps];
            for (int i = 0; i < numOps; i++)
                inputArray[i].adId = i;
        }

        [TearDown]
        public void TearDown()
        {
            fht1?.Dispose();
            fht1 = null;
            fht2?.Dispose();
            fht2 = null;
            log?.Dispose();
            log = null;

            checkpointManager?.Dispose();
            if (!string.IsNullOrEmpty(checkpointDir))
                TestUtils.DeleteDirectory(checkpointDir);
        }

        [Test]
        [Category("FasterKV"), Category("CheckpointRestore")]
        public async ValueTask PageBlobSimpleRecoveryTest([Values]CheckpointType checkpointType, [Values]bool isAsync)
        {
            if ("yes".Equals(Environment.GetEnvironmentVariable("RunAzureTests")))
            {
                checkpointManager = new DeviceLogCommitCheckpointManager(
                    new AzureStorageNamedDeviceFactory(TestUtils.AzureEmulatedStorageString),
                    new DefaultCheckpointNamingScheme($"{TestUtils.AzureMethodTestContainer}/checkpoints"));
                await SimpleRecoveryTest1_Worker(checkpointType, isAsync);
                checkpointManager.PurgeAll();
            }
        }

        [Test]
        [Category("FasterKV"), Category("CheckpointRestore")]
        public async ValueTask LocalDeviceSimpleRecoveryTest([Values] CheckpointType checkpointType, [Values] bool isAsync)
        {
            checkpointManager = new DeviceLogCommitCheckpointManager(
                new LocalStorageNamedDeviceFactory(),
                new DefaultCheckpointNamingScheme($"{TestUtils.MethodTestDir}/checkpoints"));
            await SimpleRecoveryTest1_Worker(checkpointType, isAsync);
            checkpointManager.PurgeAll();
        }

        [Test]
        [Category("FasterKV"), Category("CheckpointRestore")]
        public async ValueTask SimpleRecoveryTest1([Values]CheckpointType checkpointType, [Values]bool isAsync)
        {
            await SimpleRecoveryTest1_Worker(checkpointType, isAsync);
        }

        private async ValueTask SimpleRecoveryTest1_Worker(CheckpointType checkpointType, bool isAsync)
        {
            if (checkpointManager is null)
                checkpointDir = TestUtils.MethodTestDir + $"/checkpoints";

            log = Devices.CreateLogDevice(TestUtils.MethodTestDir + "/SimpleRecoveryTest1.log", deleteOnClose: true);

            fht1 = new FasterKV<AdId, NumClicks>(128,
                logSettings: new LogSettings { LogDevice = log, MutableFraction = 0.1, MemorySizeBits = 29 },
                checkpointSettings: new CheckpointSettings { CheckpointDir = checkpointDir, CheckpointManager = checkpointManager, CheckPointType = checkpointType }
                );

            fht2 = new FasterKV<AdId, NumClicks>(128,
                logSettings: new LogSettings { LogDevice = log, MutableFraction = 0.1, MemorySizeBits = 29 },
                checkpointSettings: new CheckpointSettings { CheckpointDir = checkpointDir, CheckpointManager = checkpointManager, CheckPointType = checkpointType }
                );

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
                    Assert.IsTrue(output.value.numClicks == key);
            }
            session2.Dispose();
        }

        [Test]
        [Category("FasterKV"), Category("CheckpointRestore")]
        public async ValueTask SimpleRecoveryTest2([Values]CheckpointType checkpointType, [Values]bool isAsync)
        {
            checkpointManager = new DeviceLogCommitCheckpointManager(new LocalStorageNamedDeviceFactory(), new DefaultCheckpointNamingScheme(TestUtils.MethodTestDir + "/checkpoints4"), false);
            log = Devices.CreateLogDevice(TestUtils.MethodTestDir + "/SimpleRecoveryTest2.log", deleteOnClose: true);

            fht1 = new FasterKV<AdId, NumClicks>(128,
                logSettings: new LogSettings { LogDevice = log, MutableFraction = 0.1, MemorySizeBits = 29 },
                checkpointSettings: new CheckpointSettings { CheckpointManager = checkpointManager, CheckPointType = checkpointType }
                );

            fht2 = new FasterKV<AdId, NumClicks>(128,
                logSettings: new LogSettings { LogDevice = log, MutableFraction = 0.1, MemorySizeBits = 29 },
                checkpointSettings: new CheckpointSettings { CheckpointManager = checkpointManager, CheckPointType = checkpointType }
                );


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
        }

        [Test]
        [Category("FasterKV"), Category("CheckpointRestore")]
        public async ValueTask ShouldRecoverBeginAddress([Values]bool isAsync)
        {
            log = Devices.CreateLogDevice(TestUtils.MethodTestDir + "/SimpleRecoveryTest2.log", deleteOnClose: true);
            checkpointDir = TestUtils.MethodTestDir + "/checkpoints6";

            fht1 = new FasterKV<AdId, NumClicks>(128,
                logSettings: new LogSettings { LogDevice = log, MutableFraction = 0.1, MemorySizeBits = 29 },
                checkpointSettings: new CheckpointSettings { CheckpointDir = checkpointDir, CheckPointType = CheckpointType.FoldOver }
                );

            fht2 = new FasterKV<AdId, NumClicks>(128,
                logSettings: new LogSettings { LogDevice = log, MutableFraction = 0.1, MemorySizeBits = 29 },
                checkpointSettings: new CheckpointSettings { CheckpointDir = checkpointDir, CheckPointType = CheckpointType.FoldOver }
                );

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
        public override void InitialUpdater(ref AdId key, ref AdInput input, ref NumClicks value)
        {
            value = input.numClicks;
        }

        public override bool InPlaceUpdater(ref AdId key, ref AdInput input, ref NumClicks value)
        {
            Interlocked.Add(ref value.numClicks, input.numClicks.numClicks);
            return true;
        }

        public override bool NeedCopyUpdate(ref AdId key, ref AdInput input, ref NumClicks oldValue) => true;

        public override void CopyUpdater(ref AdId key, ref AdInput input, ref NumClicks oldValue, ref NumClicks newValue)
        {
            newValue.numClicks += oldValue.numClicks + input.numClicks.numClicks;
        }
    }
}
 