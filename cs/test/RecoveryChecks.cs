// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Threading.Tasks;
using FASTER.core;
using NUnit.Framework;
using FASTER.test.recovery.sumstore;
using System;
using FASTER.devices;
using System.Linq;

namespace FASTER.test.recovery
{
    public enum DeviceMode
    {
        Local,
        Cloud
    }

    public class RecoveryCheckBase
    {
        protected IDevice log;
        protected const int numOps = 5000;
        protected AdId[] inputArray;
        protected string path;

        protected void BaseSetup()
        {
            inputArray = new AdId[numOps];
            for (int i = 0; i < numOps; i++)
            {
                inputArray[i].adId = i;
            }

            path = TestUtils.MethodTestDir + "/";
            log = Devices.CreateLogDevice(path + "hlog.log", deleteOnClose: false);
            TestUtils.RecreateDirectory(path);
        }

        protected void BaseTearDown()
        {
            log?.Dispose();
            log = null;
            TestUtils.DeleteDirectory(path);
        }

        public class MyFunctions : SimpleFunctions<long, long>
        {
            public override void ReadCompletionCallback(ref long key, ref long input, ref long output, Empty ctx, Status status, RecordMetadata recordMetadata)
            {
                Assert.IsTrue(status.Found, $"status = {status}");
                Assert.AreEqual(key, output, $"output = {output}");
            }
        }

        public class MyFunctions2 : SimpleFunctions<long, long>
        {
            public override void ReadCompletionCallback(ref long key, ref long input, ref long output, Empty ctx, Status status, RecordMetadata recordMetadata)
            {
                Verify(status, key, output);
            }

            internal static void Verify(Status status, long key, long output)
            {
                Assert.IsTrue(status.Found);
                if (key < 950)
                    Assert.AreEqual(key, output);
                else
                    Assert.AreEqual(key + 1, output);
            }
        }
    }

    [TestFixture]
    public class RecoveryCheck1Tests : RecoveryCheckBase
    {
        [SetUp]
        public void Setup() => BaseSetup();

        [TearDown]
        public void TearDown() => BaseTearDown();

        [Test]
        [Category("FasterKV")]
        [Category("CheckpointRestore")]
        [Category("Smoke")]

        public async ValueTask RecoveryCheck1([Values] CheckpointType checkpointType, [Values] bool isAsync, [Values] bool useReadCache, [Values(128, 1<<10)]int size)
        {
            using var fht1 = new FasterKV<long, long>
                (size,
                logSettings: new LogSettings { LogDevice = log, MutableFraction = 1, PageSizeBits = 10, MemorySizeBits = 20, ReadCacheSettings = useReadCache ? new ReadCacheSettings() : null },
                checkpointSettings: new CheckpointSettings { CheckpointDir = path }
                );

            using var s1 = fht1.NewSession(new MyFunctions());
            for (long key = 0; key < 1000; key++)
            {
                s1.Upsert(ref key, ref key);
            }

            if (useReadCache)
            {
                fht1.Log.FlushAndEvict(true);
                for (long key = 0; key < 1000; key++)
                {
                    long output = default;
                    var status = s1.Read(ref key, ref output);
                    if (!status.IsPending)
                    {
                        Assert.IsTrue(status.Found, $"status = {status}");
                        Assert.AreEqual(key, output, $"output = {output}");
                    }
                }
                s1.CompletePending(true);
            }

            var task = fht1.TakeFullCheckpointAsync(checkpointType);

            using var fht2 = new FasterKV<long, long>
                (size,
                logSettings: new LogSettings { LogDevice = log, MutableFraction = 1, PageSizeBits = 10, MemorySizeBits = 20, ReadCacheSettings = useReadCache ? new ReadCacheSettings() : null },
                checkpointSettings: new CheckpointSettings { CheckpointDir = path }
                );

            if (isAsync)
            {
                var (status, token) = await task;
                await fht2.RecoverAsync(default, token);
            }
            else
            {
                var (status, token) = task.AsTask().GetAwaiter().GetResult();
                fht2.Recover(default, token);
            }

            Assert.AreEqual(fht1.Log.HeadAddress, fht2.Log.HeadAddress);
            Assert.AreEqual(fht1.Log.ReadOnlyAddress, fht2.Log.ReadOnlyAddress);
            Assert.AreEqual(fht1.Log.TailAddress, fht2.Log.TailAddress);

            using var s2 = fht2.NewSession(new MyFunctions());
            for (long key = 0; key < 1000; key++)
            {
                long output = default;
                var status = s2.Read(ref key, ref output);
                if (!status.IsPending)
                {
                    Assert.IsTrue(status.Found, $"status = {status}");
                    Assert.AreEqual(key, output, $"output = {output}");
                }
            }
            s2.CompletePending(true);
        }

    }

    [TestFixture]
    public class RecoveryCheck2Tests : RecoveryCheckBase
    {
        [SetUp]
        public void Setup() => BaseSetup();

        [TearDown]
        public void TearDown() => BaseTearDown();

        [Test]
        [Category("FasterKV"), Category("CheckpointRestore")]
        public async ValueTask RecoveryCheck2([Values] CheckpointType checkpointType, [Values] bool isAsync, [Values] bool useReadCache, [Values(128, 1 << 10)] int size)
        {
            using var fht1 = new FasterKV<long, long>
                (size,
                logSettings: new LogSettings { LogDevice = log, MutableFraction = 1, PageSizeBits = 10, MemorySizeBits = 20, ReadCacheSettings = useReadCache ? new ReadCacheSettings() : null },
                checkpointSettings: new CheckpointSettings { CheckpointDir = path }
                );

            using var s1 = fht1.NewSession(new SimpleFunctions<long, long>());

            using var fht2 = new FasterKV<long, long>
                (size,
                logSettings: new LogSettings { LogDevice = log, MutableFraction = 1, PageSizeBits = 10, MemorySizeBits = 20, ReadCacheSettings = useReadCache ? new ReadCacheSettings() : null },
                checkpointSettings: new CheckpointSettings { CheckpointDir = path }
                );

            for (int i = 0; i < 5; i++)
            {
                for (long key = 1000*i; key < 1000 * i + 1000; key++)
                {
                    s1.Upsert(ref key, ref key);
                }

                if (useReadCache)
                {
                    fht1.Log.FlushAndEvict(true);
                    for (long key = 1000 * i; key < 1000 * i + 1000; key++)
                    {
                        long output = default;
                        var status = s1.Read(ref key, ref output);
                        if (!status.IsPending)
                        {
                            Assert.IsTrue(status.Found, $"status = {status}");
                            Assert.AreEqual(key, output, $"output = {output}");
                        }
                    }
                    s1.CompletePending(true);
                }

                var task = fht1.TakeHybridLogCheckpointAsync(checkpointType);

                if (isAsync)
                {
                    var (status, token) = await task;
                    await fht2.RecoverAsync(default, token);
                }
                else
                {
                    var(status, token) = task.AsTask().GetAwaiter().GetResult();
                    fht2.Recover(default, token);
                }

                Assert.AreEqual(fht1.Log.HeadAddress, fht2.Log.HeadAddress);
                Assert.AreEqual(fht1.Log.ReadOnlyAddress, fht2.Log.ReadOnlyAddress);
                Assert.AreEqual(fht1.Log.TailAddress, fht2.Log.TailAddress);

                using var s2 = fht2.NewSession(new SimpleFunctions<long, long>());
                for (long key = 0; key < 1000 * i + 1000; key++)
                {
                    long output = default;
                    var status = s2.Read(ref key, ref output);
                    if (!status.IsPending)
                    {
                        Assert.IsTrue(status.Found, $"status = {status}");
                        Assert.AreEqual(key, output, $"output = {output}");
                    }
                }
                s2.CompletePending(true);
            }
        }

        [Test]
        [Category("FasterKV"), Category("CheckpointRestore")]
        public void RecoveryCheck2Repeated([Values] CheckpointType checkpointType)
        {
            Guid token = default;

            for (int i = 0; i < 6; i++)
            {
                using var fht1 = new FasterKV<long, long>
                    (128,
                    logSettings: new LogSettings { LogDevice = log, MutableFraction = 1, PageSizeBits = 10, MemorySizeBits = 20 },
                    checkpointSettings: new CheckpointSettings { CheckpointDir = path }
                    );

                if (i > 0)
                {
                    fht1.Recover(default, token);
                    if (i == 3) fht1.DisposeRecoverableSessions();
                    int recoverableSessionCount = fht1.RecoverableSessions.Count();
                    if (i < 3)
                        Assert.AreEqual(i, recoverableSessionCount);
                    else
                        Assert.AreEqual(i - 3, recoverableSessionCount);
                }

                using var s1 = fht1.NewSession(new SimpleFunctions<long, long>());

                for (long key = 1000 * i; key < 1000 * i + 1000; key++)
                {
                    s1.Upsert(ref key, ref key);
                }

                var task = fht1.TakeHybridLogCheckpointAsync(checkpointType);
                bool success;
                (success, token) = task.AsTask().GetAwaiter().GetResult();
                Assert.IsTrue(success);

                using var s2 = fht1.NewSession(new SimpleFunctions<long, long>());
                for (long key = 0; key < 1000 * i + 1000; key++)
                {
                    long output = default;
                    var status = s2.Read(ref key, ref output);
                    if (!status.IsPending)
                    {
                        Assert.IsTrue(status.Found, $"status = {status}");
                        Assert.AreEqual(key, output, $"output = {output}");
                    }
                }
                s2.CompletePending(true);
            }
        }

        [Test]
        [Category("FasterKV"), Category("CheckpointRestore")]
        public void RecoveryRollback([Values] CheckpointType checkpointType)
        {
            using var fht1 = new FasterKV<long, long>
                (128,
                logSettings: new LogSettings { LogDevice = log, MutableFraction = 1, PageSizeBits = 10, MemorySizeBits = 11, SegmentSizeBits = 11 },
                checkpointSettings: new CheckpointSettings { CheckpointDir = path }
                );

            using var s1 = fht1.NewSession(new SimpleFunctions<long, long>());

            for (long key = 0; key < 1000; key++)
            {
                s1.Upsert(ref key, ref key);
            }

            var task = fht1.TakeHybridLogCheckpointAsync(checkpointType);
            (bool success, Guid token) = task.AsTask().GetAwaiter().GetResult();
            Assert.IsTrue(success);

            for (long key = 0; key < 1000; key++)
            {
                long output = default;
                var status = s1.Read(ref key, ref output);
                if (!status.IsPending)
                {
                    Assert.IsTrue(status.Found, $"status = {status}");
                    Assert.AreEqual(key, output, $"output = {output}");
                }
            }
            s1.CompletePendingWithOutputs(out var completedOutputs, true);
            while (completedOutputs.Next())
            {
                Assert.IsTrue(completedOutputs.Current.Status.Found);
                Assert.AreEqual(completedOutputs.Current.Key, completedOutputs.Current.Output, $"output = {completedOutputs.Current.Output}");
            }
            completedOutputs.Dispose();

            for (long key = 1000; key < 2000; key++)
            {
                s1.Upsert(ref key, ref key);
            }

            // Reset store to empty state
            fht1.Reset();

            for (long key = 0; key < 2000; key++)
            {
                long output = default;
                var status = s1.Read(ref key, ref output);
                if (!status.IsPending)
                {
                    Assert.IsTrue(status.NotFound, $"status = {status}");
                }
            }
            s1.CompletePendingWithOutputs(out completedOutputs, true);
            while (completedOutputs.Next())
            {
                Assert.IsTrue(completedOutputs.Current.Status.NotFound);
            }
            completedOutputs.Dispose();

            // Rollback to previous checkpoint
            fht1.Recover(default, token);

            for (long key = 0; key < 1000; key++)
            {
                long output = default;
                var status = s1.Read(ref key, ref output);
                if (!status.IsPending)
                {
                    Assert.IsTrue(status.Found, $"status = {status}");
                    Assert.AreEqual(key, output, $"output = {output}");
                }
            }
            s1.CompletePendingWithOutputs(out completedOutputs, true);
            while (completedOutputs.Next())
            {
                Assert.IsTrue(completedOutputs.Current.Status.Found);
                Assert.AreEqual(completedOutputs.Current.Key, completedOutputs.Current.Output, $"output = {completedOutputs.Current.Output}");
            }
            completedOutputs.Dispose();

            for (long key = 1000; key < 2000; key++)
            {
                long output = default;
                var status = s1.Read(ref key, ref output);
                if (!status.IsPending)
                {
                    Assert.IsTrue(status.NotFound, $"status = {status}");
                }
            }
            s1.CompletePendingWithOutputs(out completedOutputs, true);
            while (completedOutputs.Next())
            {
                Assert.IsTrue(completedOutputs.Current.Status.NotFound);
            }
            completedOutputs.Dispose();

            for (long key = 1000; key < 2000; key++)
            {
                s1.Upsert(ref key, ref key);
            }

            for (long key = 0; key < 2000; key++)
            {
                long output = default;
                var status = s1.Read(ref key, ref output);
                if (!status.IsPending)
                {
                    Assert.IsTrue(status.Found, $"status = {status}");
                    Assert.AreEqual(key, output, $"output = {output}");
                }
                else
                {
                    s1.CompletePendingWithOutputs(out completedOutputs, true);
                    while (completedOutputs.Next())
                    {
                        Assert.IsTrue(completedOutputs.Current.Status.Found);
                        Assert.AreEqual(completedOutputs.Current.Key, completedOutputs.Current.Output, $"output = {completedOutputs.Current.Output}");
                    }
                    completedOutputs.Dispose();
                }
            }
            s1.CompletePendingWithOutputs(out completedOutputs, true);
            while (completedOutputs.Next())
            {
                Assert.IsTrue(completedOutputs.Current.Status.Found);
                Assert.AreEqual(completedOutputs.Current.Key, completedOutputs.Current.Output, $"output = {completedOutputs.Current.Output}");
            }
            completedOutputs.Dispose();
        }
    }

    [TestFixture]
    public class RecoveryCheck3Tests : RecoveryCheckBase
    {
        [SetUp]
        public void Setup() => BaseSetup();

        [TearDown]
        public void TearDown() => BaseTearDown();

        [Test]
        [Category("FasterKV"), Category("CheckpointRestore")]
        public async ValueTask RecoveryCheck3([Values] CheckpointType checkpointType, [Values] bool isAsync, [Values] bool useReadCache, [Values(128, 1 << 10)] int size)
        {
            using var fht1 = new FasterKV<long, long>
                (size,
                logSettings: new LogSettings { LogDevice = log, MutableFraction = 1, PageSizeBits = 10, MemorySizeBits = 20, ReadCacheSettings = useReadCache ? new ReadCacheSettings() : null },
                checkpointSettings: new CheckpointSettings { CheckpointDir = path }
                );

            using var s1 = fht1.NewSession(new SimpleFunctions<long, long>());

            using var fht2 = new FasterKV<long, long>
                (size,
                logSettings: new LogSettings { LogDevice = log, MutableFraction = 1, PageSizeBits = 10, MemorySizeBits = 20, ReadCacheSettings = useReadCache ? new ReadCacheSettings() : null },
                checkpointSettings: new CheckpointSettings { CheckpointDir = path }
                );

            for (int i = 0; i < 5; i++)
            {
                for (long key = 1000 * i; key < 1000 * i + 1000; key++)
                {
                    s1.Upsert(ref key, ref key);
                }

                if (useReadCache)
                {
                    fht1.Log.FlushAndEvict(true);
                    for (long key = 1000 * i; key < 1000 * i + 1000; key++)
                    {
                        long output = default;
                        var status = s1.Read(ref key, ref output);
                        if (!status.IsPending)
                        {
                            Assert.IsTrue(status.Found, $"status = {status}");
                            Assert.AreEqual(key, output, $"output = {output}");
                        }
                    }
                    s1.CompletePending(true);
                }

                var task = fht1.TakeFullCheckpointAsync(checkpointType);

                if (isAsync)
                {
                    var (status, token) = await task;
                    await fht2.RecoverAsync(default, token);
                }
                else
                {
                    var (status, token) = task.AsTask().GetAwaiter().GetResult();
                    fht2.Recover(default, token);
                }

                Assert.AreEqual(fht1.Log.HeadAddress, fht2.Log.HeadAddress);
                Assert.AreEqual(fht1.Log.ReadOnlyAddress, fht2.Log.ReadOnlyAddress);
                Assert.AreEqual(fht1.Log.TailAddress, fht2.Log.TailAddress);

                using var s2 = fht2.NewSession(new SimpleFunctions<long, long>());
                for (long key = 0; key < 1000 * i + 1000; key++)
                {
                    long output = default;
                    var status = s2.Read(ref key, ref output);
                    if (!status.IsPending)
                    {
                        Assert.IsTrue(status.Found, $"status = {status}");
                        Assert.AreEqual(key, output, $"output = {output}");
                    }
                }
                s2.CompletePending(true);
            }
        }

    }

    [TestFixture]
    public class RecoveryCheck4Tests : RecoveryCheckBase
    {
        [SetUp]
        public void Setup() => BaseSetup();

        [TearDown]
        public void TearDown() => BaseTearDown();

        [Test]
        [Category("FasterKV"), Category("CheckpointRestore")]
        public async ValueTask RecoveryCheck4([Values] CheckpointType checkpointType, [Values] bool isAsync, [Values] bool useReadCache, [Values(128, 1 << 10)] int size)
        {
            using var fht1 = new FasterKV<long, long>
                (size,
                logSettings: new LogSettings { LogDevice = log, MutableFraction = 1, PageSizeBits = 10, MemorySizeBits = 20, ReadCacheSettings = useReadCache ? new ReadCacheSettings() : null },
                checkpointSettings: new CheckpointSettings { CheckpointDir = path }
                );

            using var s1 = fht1.NewSession(new SimpleFunctions<long, long>());

            using var fht2 = new FasterKV<long, long>
                (size,
                logSettings: new LogSettings { LogDevice = log, MutableFraction = 1, PageSizeBits = 10, MemorySizeBits = 20, ReadCacheSettings = useReadCache ? new ReadCacheSettings() : null },
                checkpointSettings: new CheckpointSettings { CheckpointDir = path }
                );

            for (int i = 0; i < 5; i++)
            {
                for (long key = 1000 * i; key < 1000 * i + 1000; key++)
                {
                    s1.Upsert(ref key, ref key);
                }

                if (useReadCache)
                {
                    fht1.Log.FlushAndEvict(true);
                    for (long key = 1000 * i; key < 1000 * i + 1000; key++)
                    {
                        long output = default;
                        var status = s1.Read(ref key, ref output);
                        if (!status.IsPending)
                        {
                            Assert.IsTrue(status.Found, $"status = {status}");
                            Assert.AreEqual(key, output, $"output = {output}");
                        }
                    }
                    s1.CompletePending(true);
                }

                if (i == 0)
                    fht1.TakeIndexCheckpointAsync().AsTask().GetAwaiter().GetResult();

                var task = fht1.TakeHybridLogCheckpointAsync(checkpointType);

                if (isAsync)
                {
                    var (status, token) = await task;
                    await fht2.RecoverAsync(default, token);
                }
                else
                {
                    var (status, token) = task.AsTask().GetAwaiter().GetResult();
                    fht2.Recover(default, token);
                }

                Assert.AreEqual(fht1.Log.HeadAddress, fht2.Log.HeadAddress);
                Assert.AreEqual(fht1.Log.ReadOnlyAddress, fht2.Log.ReadOnlyAddress);
                Assert.AreEqual(fht1.Log.TailAddress, fht2.Log.TailAddress);

                using var s2 = fht2.NewSession(new SimpleFunctions<long, long>());
                for (long key = 0; key < 1000 * i + 1000; key++)
                {
                    long output = default;
                    var status = s2.Read(ref key, ref output);
                    if (!status.IsPending)
                    {
                        Assert.IsTrue(status.Found, $"status = {status}");
                        Assert.AreEqual(key, output, $"output = {output}");
                    }
                }
                s2.CompletePending(true);
            }
        }

    }

    [TestFixture]
    public class RecoveryCheck5Tests : RecoveryCheckBase
    {
        [SetUp]
        public void Setup() => BaseSetup();

        [TearDown]
        public void TearDown() => BaseTearDown();

        [Test]
        [Category("FasterKV")]
        [Category("CheckpointRestore")]
        public async ValueTask RecoveryCheck5([Values] CheckpointType checkpointType, [Values] bool isAsync, [Values] bool useReadCache, [Values(128, 1 << 10)] int size)
        {
            using var fht1 = new FasterKV<long, long>
                (size,
                logSettings: new LogSettings { LogDevice = log, MutableFraction = 1, PageSizeBits = 10, MemorySizeBits = 14, ReadCacheSettings = useReadCache ? new ReadCacheSettings() : null },
                checkpointSettings: new CheckpointSettings { CheckpointDir = path },
                lockingMode : LockingMode.Ephemeral
                );

            using var s1 = fht1.NewSession(new MyFunctions());
            for (long key = 0; key < 1000; key++)
            {
                s1.Upsert(ref key, ref key);
            }

            if (useReadCache)
            {
                fht1.Log.FlushAndEvict(true);
                for (long key = 0; key < 1000; key++)
                {
                    long output = default;
                    var status = s1.Read(ref key, ref output);
                    if (!status.IsPending)
                    {
                        Assert.IsTrue(status.Found, $"status = {status}");
                        Assert.AreEqual(key, output, $"output = {output}");
                    }
                }
                s1.CompletePending(true);
            }
            
            fht1.GrowIndex();

            for (long key = 0; key < 1000; key++)
            {
                long output = default;
                var status = s1.Read(ref key, ref output);
                if (!status.IsPending)
                {
                    Assert.IsTrue(status.Found, $"status = {status}");
                    Assert.AreEqual(key, output, $"output = {output}");
                }
            }
            s1.CompletePending(true);

            var task = fht1.TakeFullCheckpointAsync(checkpointType);

            using var fht2 = new FasterKV<long, long>
                (size,
                logSettings: new LogSettings { LogDevice = log, MutableFraction = 1, PageSizeBits = 10, MemorySizeBits = 20, ReadCacheSettings = useReadCache ? new ReadCacheSettings() : null },
                checkpointSettings: new CheckpointSettings { CheckpointDir = path }
                );

            if (isAsync)
            {
                var (status, token) = await task;
                await fht2.RecoverAsync(default, token);
            }
            else
            {
                var (status, token) = task.AsTask().GetAwaiter().GetResult();
                fht2.Recover(default, token);
            }

            Assert.AreEqual(fht1.Log.HeadAddress, fht2.Log.HeadAddress);
            Assert.AreEqual(fht1.Log.ReadOnlyAddress, fht2.Log.ReadOnlyAddress);
            Assert.AreEqual(fht1.Log.TailAddress, fht2.Log.TailAddress);

            using var s2 = fht2.NewSession(new MyFunctions());
            for (long key = 0; key < 1000; key++)
            {
                long output = default;
                var status = s2.Read(ref key, ref output);
                if (!status.IsPending)
                {
                    Assert.IsTrue(status.Found, $"status = {status}");
                    Assert.AreEqual(key, output, $"output = {output}");
                }
            }
            s2.CompletePending(true);
        }
    }

    [TestFixture]
    public class RecoveryCheckSnapshotTests : RecoveryCheckBase
    {
        [SetUp]
        public void Setup() => BaseSetup();

        [TearDown]
        public void TearDown() => BaseTearDown();

        [Test]
        [Category("FasterKV")]
        [Category("CheckpointRestore")]
        [Category("Smoke")]
        public async ValueTask IncrSnapshotRecoveryCheck([Values] DeviceMode deviceMode)
        {
            ICheckpointManager checkpointManager;
            if (deviceMode == DeviceMode.Local)
            {
                checkpointManager = new DeviceLogCommitCheckpointManager(
                    new LocalStorageNamedDeviceFactory(),
                    new DefaultCheckpointNamingScheme(TestUtils.MethodTestDir + "/checkpoints/"));  // PurgeAll deletes this directory
            }
            else
            {
                TestUtils.IgnoreIfNotRunningAzureTests();
                checkpointManager = new DeviceLogCommitCheckpointManager(
                    new AzureStorageNamedDeviceFactory(TestUtils.AzureEmulatedStorageString),
                    new DefaultCheckpointNamingScheme($"{TestUtils.AzureTestContainer}/{TestUtils.AzureTestDirectory}"));
            }

            await IncrSnapshotRecoveryCheck(checkpointManager);
            checkpointManager.PurgeAll();
            checkpointManager.Dispose();
        }

        private async ValueTask IncrSnapshotRecoveryCheck(ICheckpointManager checkpointManager)
        {
            using var fht1 = new FasterKV<long, long>
                (1 << 10,
                logSettings: new LogSettings { LogDevice = log, MutableFraction = 1, PageSizeBits = 10, MemorySizeBits = 20, ReadCacheSettings = null },
                checkpointSettings: new CheckpointSettings { CheckpointManager = checkpointManager }
                );

            using var s1 = fht1.NewSession(new MyFunctions2());
            for (long key = 0; key < 1000; key++)
            {
                s1.Upsert(ref key, ref key);
            }

            var task = fht1.TakeHybridLogCheckpointAsync(CheckpointType.Snapshot);
            var (success, token) = await task;

            for (long key = 950; key < 1000; key++)
            {
                s1.Upsert(key, key+1);
            }

            var version1 = fht1.CurrentVersion;
            var _result1 = fht1.TryInitiateHybridLogCheckpoint(out var _token1, CheckpointType.Snapshot, true);
            await fht1.CompleteCheckpointAsync();

            Assert.IsTrue(_result1);
            Assert.AreEqual(token, _token1);

            for (long key = 1000; key < 2000; key++)
            {
                s1.Upsert(key, key + 1);
            }

            var version2 = fht1.CurrentVersion;
            var _result2 = fht1.TryInitiateHybridLogCheckpoint(out var _token2, CheckpointType.Snapshot, true);
            await fht1.CompleteCheckpointAsync();

            Assert.IsTrue(_result2);
            Assert.AreEqual(token, _token2);

            // Test that we can recover to latest version
            using var fht2 = new FasterKV<long, long>
                (1 << 10,
                logSettings: new LogSettings { LogDevice = log, MutableFraction = 1, PageSizeBits = 10, MemorySizeBits = 14, ReadCacheSettings = null },
                checkpointSettings: new CheckpointSettings { CheckpointManager = checkpointManager }
                );

            await fht2.RecoverAsync(default, _token2);

            Assert.AreEqual(fht2.Log.TailAddress, fht1.Log.TailAddress);

            using var s2 = fht2.NewSession(new MyFunctions2());
            for (long key = 0; key < 2000; key++)
            {
                long output = default;
                var status = s2.Read(ref key, ref output);
                if (!status.IsPending)
                {
                    MyFunctions2.Verify(status, key, output);
                }
            }
            s2.CompletePending(true);
            
            // Test that we can recover to earlier version
            using var fht3 = new FasterKV<long, long>
            (1 << 10,
                logSettings: new LogSettings { LogDevice = log, MutableFraction = 1, PageSizeBits = 10, MemorySizeBits = 14, ReadCacheSettings = null },
                checkpointSettings: new CheckpointSettings { CheckpointManager = checkpointManager }
            );

            await fht3.RecoverAsync(recoverTo: version1);

            Assert.IsTrue(fht3.EntryCount == 1000);
            using var s3 = fht3.NewSession(new MyFunctions2());
            for (long key = 0; key < 1000; key++)
            {
                long output = default;
                var status = s3.Read(ref key, ref output);
                if (!status.IsPending)
                {
                    MyFunctions2.Verify(status, key, output);
                }
            }
            s3.CompletePending(true);
        }
    }
}
