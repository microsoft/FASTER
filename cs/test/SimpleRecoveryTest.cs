// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Linq;
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

        private byte[] commitCookie;
        string checkpointDir;
        ICheckpointManager checkpointManager;

        private FasterKV<AdId, NumClicks> fht1;
        private FasterKV<AdId, NumClicks> fht2;
        private IDevice log;
        

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
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
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        [Test]
        [Category("FasterKV"), Category("CheckpointRestore")]
        public async ValueTask PageBlobSimpleRecoveryTest([Values]CheckpointType checkpointType, [Values]bool isAsync, [Values]bool testCommitCookie)
        {
            TestUtils.IgnoreIfNotRunningAzureTests();
            checkpointManager = new DeviceLogCommitCheckpointManager(
                new AzureStorageNamedDeviceFactory(TestUtils.AzureEmulatedStorageString),
                new DefaultCheckpointNamingScheme($"{TestUtils.AzureTestContainer}/{TestUtils.AzureTestDirectory}"));
            await SimpleRecoveryTest1_Worker(checkpointType, isAsync, testCommitCookie);
            checkpointManager.PurgeAll();
        }

        [Test]
        [Category("FasterKV")]
        [Category("CheckpointRestore")]
        [Category("Smoke")]

        public async ValueTask LocalDeviceSimpleRecoveryTest([Values] CheckpointType checkpointType, [Values] bool isAsync, [Values]bool testCommitCookie)
        {
            checkpointManager = new DeviceLogCommitCheckpointManager(
                new LocalStorageNamedDeviceFactory(),
                new DefaultCheckpointNamingScheme($"{TestUtils.MethodTestDir}/chkpt"));
            await SimpleRecoveryTest1_Worker(checkpointType, isAsync, testCommitCookie);
            checkpointManager.PurgeAll();
        }

        [Test]
        [Category("FasterKV"), Category("CheckpointRestore")]
        public async ValueTask SimpleRecoveryTest1([Values]CheckpointType checkpointType, [Values]bool isAsync, [Values]bool testCommitCookie)
        {
            await SimpleRecoveryTest1_Worker(checkpointType, isAsync, testCommitCookie);
        }

        private async ValueTask SimpleRecoveryTest1_Worker(CheckpointType checkpointType, bool isAsync, bool testCommitCookie)
        {
            if (testCommitCookie)
            {
                // Generate a new unique byte sequence for test
                commitCookie = Guid.NewGuid().ToByteArray();
            }

            if (checkpointManager is null)
                checkpointDir = TestUtils.MethodTestDir + $"/checkpoints";

            log = Devices.CreateLogDevice(TestUtils.MethodTestDir + "/SimpleRecoveryTest1.log", deleteOnClose: true);

            fht1 = new FasterKV<AdId, NumClicks>(128,
                logSettings: new LogSettings { LogDevice = log, MutableFraction = 0.1, MemorySizeBits = 29 },
                checkpointSettings: new CheckpointSettings { CheckpointDir = checkpointDir, CheckpointManager = checkpointManager }
                );

            fht2 = new FasterKV<AdId, NumClicks>(128,
                logSettings: new LogSettings { LogDevice = log, MutableFraction = 0.1, MemorySizeBits = 29 },
                checkpointSettings: new CheckpointSettings { CheckpointDir = checkpointDir, CheckpointManager = checkpointManager }
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

            if (testCommitCookie)
                fht1.CommitCookie = commitCookie;
            fht1.TryInitiateFullCheckpoint(out Guid token, checkpointType);
            fht1.CompleteCheckpointAsync().AsTask().GetAwaiter().GetResult();
            session1.Dispose();

            if (isAsync)
                await fht2.RecoverAsync(token);
            else
                fht2.Recover(token);

            if (testCommitCookie)
                Assert.IsTrue(fht2.RecoveredCommitCookie.SequenceEqual(commitCookie));
            else
                Assert.Null(fht2.RecoveredCommitCookie);
            
            var session2 = fht2.NewSession(new AdSimpleFunctions());
            Assert.AreEqual(2, session2.ID);

            for (int key = 0; key < numOps; key++)
            {
                var status = session2.Read(ref inputArray[key], ref inputArg, ref output, Empty.Default, 0);

                if (status == Status.PENDING)
                {
                    session2.CompletePendingWithOutputs(out var outputs, wait: true);
                    Assert.IsTrue(outputs.Next());
                    output = outputs.Current.Output;
                    Assert.IsFalse(outputs.Next());
                    outputs.Current.Dispose();
                }
                else
                    Assert.AreEqual(Status.OK, status);
                Assert.AreEqual(key, output.value.numClicks);
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
                checkpointSettings: new CheckpointSettings { CheckpointManager = checkpointManager }
                );

            fht2 = new FasterKV<AdId, NumClicks>(128,
                logSettings: new LogSettings { LogDevice = log, MutableFraction = 0.1, MemorySizeBits = 29 },
                checkpointSettings: new CheckpointSettings { CheckpointManager = checkpointManager }
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
            fht1.TryInitiateFullCheckpoint(out Guid token, checkpointType);
            fht1.CompleteCheckpointAsync().AsTask().GetAwaiter().GetResult();
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
                    Assert.AreEqual(key, output.value.numClicks);
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
                checkpointSettings: new CheckpointSettings { CheckpointDir = checkpointDir }
                );

            fht2 = new FasterKV<AdId, NumClicks>(128,
                logSettings: new LogSettings { LogDevice = log, MutableFraction = 0.1, MemorySizeBits = 29 },
                checkpointSettings: new CheckpointSettings { CheckpointDir = checkpointDir }
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

            fht1.TryInitiateFullCheckpoint(out Guid token, CheckpointType.FoldOver);
            fht1.CompleteCheckpointAsync().AsTask().GetAwaiter().GetResult();
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
        public override void ReadCompletionCallback(ref AdId key, ref AdInput input, ref Output output, Empty ctx, Status status, RecordMetadata recordMetadata)
        {
            Assert.AreEqual(Status.OK, status);
            Assert.AreEqual(key.adId, output.value.numClicks);
        }

        // Read functions
        public override bool SingleReader(ref AdId key, ref AdInput input, ref NumClicks value, ref Output dst, ref RecordInfo recordInfo, long address)
        {
            dst.value = value;
            return true;
        }

        public override bool ConcurrentReader(ref AdId key, ref AdInput input, ref NumClicks value, ref Output dst, ref RecordInfo recordInfo, long address)
        {
            dst.value = value;
            return true;
        }

        // RMW functions
        public override void InitialUpdater(ref AdId key, ref AdInput input, ref NumClicks value, ref Output output, ref RecordInfo recordInfo, long address)
        {
            value = input.numClicks;
        }

        public override bool InPlaceUpdater(ref AdId key, ref AdInput input, ref NumClicks value, ref Output output, ref RecordInfo recordInfo, long address)
        {
            Interlocked.Add(ref value.numClicks, input.numClicks.numClicks);
            return true;
        }

        public override bool NeedCopyUpdate(ref AdId key, ref AdInput input, ref NumClicks oldValue, ref Output output) => true;

        public override void CopyUpdater(ref AdId key, ref AdInput input, ref NumClicks oldValue, ref NumClicks newValue, ref Output output, ref RecordInfo recordInfo, long address)
        {
            newValue.numClicks += oldValue.numClicks + input.numClicks.numClicks;
        }
    }
}
 