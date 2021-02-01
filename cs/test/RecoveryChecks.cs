// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Threading.Tasks;
using FASTER.core;
using System.IO;
using NUnit.Framework;
using FASTER.test.recovery.sumstore;

namespace FASTER.test.recovery
{

    [TestFixture]
    public class RecoveryChecks
    {
        IDevice log;
        const int numOps = 5000;
        AdId[] inputArray;
        string path;

        [SetUp]
        public void Setup()
        {
            inputArray = new AdId[numOps];
            for (int i = 0; i < numOps; i++)
            {
                inputArray[i].adId = i;
            }

            path = TestContext.CurrentContext.TestDirectory + "/RecoveryChecks/";
            log = Devices.CreateLogDevice(path + "hlog.log", deleteOnClose: true);
            Directory.CreateDirectory(path);
        }

        [TearDown]
        public void TearDown()
        {
            log.Dispose();
            new DirectoryInfo(path).Delete(true);
        }

        public class MyFunctions : SimpleFunctions<long, long>
        {
            public override void ReadCompletionCallback(ref long key, ref long input, ref long output, Empty ctx, Status status)
            {
                Assert.IsTrue(status == Status.OK && output == key);
            }
        }

        [Test]
        [Category("FasterKV")]
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
                    if (status != Status.PENDING)
                        Assert.IsTrue(status == Status.OK && output == key);
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
                await task;
                await fht2.RecoverAsync();
            }
            else
            {
                task.GetAwaiter().GetResult();
                fht2.Recover();
            }

            Assert.IsTrue(fht1.Log.HeadAddress == fht2.Log.HeadAddress);
            Assert.IsTrue(fht1.Log.ReadOnlyAddress == fht2.Log.ReadOnlyAddress);
            Assert.IsTrue(fht1.Log.TailAddress == fht2.Log.TailAddress);

            using var s2 = fht2.NewSession(new MyFunctions());
            for (long key = 0; key < 1000; key++)
            {
                long output = default;
                var status = s2.Read(ref key, ref output);
                if (status != Status.PENDING)
                    Assert.IsTrue(status == Status.OK && output == key);
            }
            s2.CompletePending(true);
        }

        [Test]
        [Category("FasterKV")]
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
                        if (status != Status.PENDING)
                            Assert.IsTrue(status == Status.OK && output == key);
                    }
                    s1.CompletePending(true);
                }

                var task = fht1.TakeHybridLogCheckpointAsync(checkpointType);

                if (isAsync)
                {
                    await task;
                    await fht2.RecoverAsync();
                }
                else
                {
                    task.GetAwaiter().GetResult();
                    fht2.Recover();
                }

                Assert.IsTrue(fht1.Log.HeadAddress == fht2.Log.HeadAddress);
                Assert.IsTrue(fht1.Log.ReadOnlyAddress == fht2.Log.ReadOnlyAddress);
                Assert.IsTrue(fht1.Log.TailAddress == fht2.Log.TailAddress);

                using var s2 = fht2.NewSession(new SimpleFunctions<long, long>());
                for (long key = 0; key < 1000 * i + 1000; key++)
                {
                    long output = default;
                    var status = s2.Read(ref key, ref output);
                    if (status != Status.PENDING)
                        Assert.IsTrue(status == Status.OK && output == key);
                }
                s2.CompletePending(true);
            }
        }

        [Test]
        [Category("FasterKV")]
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
                        if (status != Status.PENDING)
                            Assert.IsTrue(status == Status.OK && output == key);
                    }
                    s1.CompletePending(true);
                }

                var task = fht1.TakeFullCheckpointAsync(checkpointType);

                if (isAsync)
                {
                    await task;
                    await fht2.RecoverAsync();
                }
                else
                {
                    task.GetAwaiter().GetResult();
                    fht2.Recover();
                }

                Assert.IsTrue(fht1.Log.HeadAddress == fht2.Log.HeadAddress);
                Assert.IsTrue(fht1.Log.ReadOnlyAddress == fht2.Log.ReadOnlyAddress);
                Assert.IsTrue(fht1.Log.TailAddress == fht2.Log.TailAddress);

                using var s2 = fht2.NewSession(new SimpleFunctions<long, long>());
                for (long key = 0; key < 1000 * i + 1000; key++)
                {
                    long output = default;
                    var status = s2.Read(ref key, ref output);
                    if (status != Status.PENDING)
                        Assert.IsTrue(status == Status.OK && output == key);
                }
                s2.CompletePending(true);
            }
        }

        [Test]
        [Category("FasterKV")]
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
                        if (status != Status.PENDING)
                            Assert.IsTrue(status == Status.OK && output == key);
                    }
                    s1.CompletePending(true);
                }

                if (i == 0)
                    fht1.TakeIndexCheckpointAsync().GetAwaiter().GetResult();

                var task = fht1.TakeHybridLogCheckpointAsync(checkpointType);

                if (isAsync)
                {
                    await task;
                    await fht2.RecoverAsync();
                }
                else
                {
                    task.GetAwaiter().GetResult();
                    fht2.Recover();
                }

                Assert.IsTrue(fht1.Log.HeadAddress == fht2.Log.HeadAddress);
                Assert.IsTrue(fht1.Log.ReadOnlyAddress == fht2.Log.ReadOnlyAddress);
                Assert.IsTrue(fht1.Log.TailAddress == fht2.Log.TailAddress);

                using var s2 = fht2.NewSession(new SimpleFunctions<long, long>());
                for (long key = 0; key < 1000 * i + 1000; key++)
                {
                    long output = default;
                    var status = s2.Read(ref key, ref output);
                    if (status != Status.PENDING)
                        Assert.IsTrue(status == Status.OK && output == key);
                }
                s2.CompletePending(true);
            }
        }

        [Test]
        public async ValueTask RecoveryCheck5([Values] CheckpointType checkpointType, [Values] bool isAsync, [Values] bool useReadCache, [Values(128, 1 << 10)] int size)
        {
            using var fht1 = new FasterKV<long, long>
                (size,
                logSettings: new LogSettings { LogDevice = log, MutableFraction = 1, PageSizeBits = 10, MemorySizeBits = 14, ReadCacheSettings = useReadCache ? new ReadCacheSettings() : null },
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
                    if (status != Status.PENDING)
                        Assert.IsTrue(status == Status.OK && output == key);
                }
                s1.CompletePending(true);
            }
            
            fht1.GrowIndex();

            for (long key = 0; key < 1000; key++)
            {
                long output = default;
                var status = s1.Read(ref key, ref output);
                if (status != Status.PENDING)
                    Assert.IsTrue(status == Status.OK && output == key);
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
                await task;
                await fht2.RecoverAsync();
            }
            else
            {
                task.GetAwaiter().GetResult();
                fht2.Recover();
            }

            Assert.IsTrue(fht1.Log.HeadAddress == fht2.Log.HeadAddress);
            Assert.IsTrue(fht1.Log.ReadOnlyAddress == fht2.Log.ReadOnlyAddress);
            Assert.IsTrue(fht1.Log.TailAddress == fht2.Log.TailAddress);

            using var s2 = fht2.NewSession(new MyFunctions());
            for (long key = 0; key < 1000; key++)
            {
                long output = default;
                var status = s2.Read(ref key, ref output);
                if (status != Status.PENDING)
                    Assert.IsTrue(status == Status.OK && output == key);
            }
            s2.CompletePending(true);
        }
    }
}
