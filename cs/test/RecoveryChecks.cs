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
using System.Diagnostics;
using System.Net;

namespace FASTER.test.recovery
{

    [TestFixture]
    public class RecoveryChecks
    {
        IDevice log;
        FasterKV<long, long> fht1;
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

            path = TestContext.CurrentContext.TestDirectory + "\\RecoveryChecks\\";
            log = Devices.CreateLogDevice(path + "hlog.log", deleteOnClose: true);
            Directory.CreateDirectory(path);
            fht1 = new FasterKV<long, long>
                (1L << 10,
                logSettings: new LogSettings { LogDevice = log, MutableFraction = 1, PageSizeBits = 10, MemorySizeBits = 20 },
                checkpointSettings: new CheckpointSettings { CheckpointDir = path }
                );
        }

        [TearDown]
        public void TearDown()
        {
            fht1.Dispose();
            log.Dispose();
            new DirectoryInfo(path).Delete(true);
        }


        [Test]
        public void RecoveryCheck1([Values] CheckpointType checkpointType)
        {
            using var s1 = fht1.NewSession(new SimpleFunctions<long, long>());
            for (long key = 0; key < 1000; key++)
            {
                s1.Upsert(ref key, ref key);
            }
            fht1.TakeHybridLogCheckpointAsync(checkpointType).GetAwaiter().GetResult();

            using var fht2 = new FasterKV<long, long>
                (1L << 10,
                logSettings: new LogSettings { LogDevice = log, MutableFraction = 1, PageSizeBits = 10, MemorySizeBits = 20 },
                checkpointSettings: new CheckpointSettings { CheckpointDir = path }
                );
            fht2.Recover();

            Assert.IsTrue(fht1.Log.HeadAddress == fht2.Log.HeadAddress);
            Assert.IsTrue(fht1.Log.ReadOnlyAddress == fht2.Log.ReadOnlyAddress);
            Assert.IsTrue(fht1.Log.TailAddress == fht2.Log.TailAddress);

            using var s2 = fht2.NewSession(new SimpleFunctions<long, long>());
            for (long key = 0; key < 1000; key++)
            {
                long output = default;
                var status = s2.Read(ref key, ref output);
                Assert.IsTrue(status == Status.OK && output == key);
            }
        }

        [Test]
        public void RecoveryCheck2([Values] CheckpointType checkpointType)
        {
            using var s1 = fht1.NewSession(new SimpleFunctions<long, long>());

            using var fht2 = new FasterKV<long, long>
                (1L << 10,
                logSettings: new LogSettings { LogDevice = log, MutableFraction = 1, PageSizeBits = 10, MemorySizeBits = 20 },
                checkpointSettings: new CheckpointSettings { CheckpointDir = path }
                );

            for (int i = 0; i < 5; i++)
            {
                for (long key = 1000*i; key < 1000 * i + 1000; key++)
                {
                    s1.Upsert(ref key, ref key);
                }
                fht1.TakeHybridLogCheckpointAsync(checkpointType).GetAwaiter().GetResult();

                fht2.Recover();

                Assert.IsTrue(fht1.Log.HeadAddress == fht2.Log.HeadAddress);
                Assert.IsTrue(fht1.Log.ReadOnlyAddress == fht2.Log.ReadOnlyAddress);
                Assert.IsTrue(fht1.Log.TailAddress == fht2.Log.TailAddress);

                using var s2 = fht2.NewSession(new SimpleFunctions<long, long>());
                for (long key = 0; key < 1000 * i + 1000; key++)
                {
                    long output = default;
                    var status = s2.Read(ref key, ref output);
                    Assert.IsTrue(status == Status.OK && output == key);
                }
            }
        }

        [Test]
        public void RecoveryCheck3([Values] CheckpointType checkpointType)
        {
            using var s1 = fht1.NewSession(new SimpleFunctions<long, long>());

            using var fht2 = new FasterKV<long, long>
                (1L << 10,
                logSettings: new LogSettings { LogDevice = log, MutableFraction = 1, PageSizeBits = 10, MemorySizeBits = 20 },
                checkpointSettings: new CheckpointSettings { CheckpointDir = path }
                );

            for (int i = 0; i < 5; i++)
            {
                for (long key = 1000 * i; key < 1000 * i + 1000; key++)
                {
                    s1.Upsert(ref key, ref key);
                }
                fht1.TakeFullCheckpointAsync(checkpointType).GetAwaiter().GetResult();

                fht2.Recover();

                Assert.IsTrue(fht1.Log.HeadAddress == fht2.Log.HeadAddress);
                Assert.IsTrue(fht1.Log.ReadOnlyAddress == fht2.Log.ReadOnlyAddress);
                Assert.IsTrue(fht1.Log.TailAddress == fht2.Log.TailAddress);

                using var s2 = fht2.NewSession(new SimpleFunctions<long, long>());
                for (long key = 0; key < 1000 * i + 1000; key++)
                {
                    long output = default;
                    var status = s2.Read(ref key, ref output);
                    Assert.IsTrue(status == Status.OK && output == key);
                }
            }
        }

        [Test]
        public void RecoveryCheck4([Values] CheckpointType checkpointType)
        {
            using var s1 = fht1.NewSession(new SimpleFunctions<long, long>());

            using var fht2 = new FasterKV<long, long>
                (1L << 10,
                logSettings: new LogSettings { LogDevice = log, MutableFraction = 1, PageSizeBits = 10, MemorySizeBits = 20 },
                checkpointSettings: new CheckpointSettings { CheckpointDir = path }
                );

            for (int i = 0; i < 5; i++)
            {
                for (long key = 1000 * i; key < 1000 * i + 1000; key++)
                {
                    s1.Upsert(ref key, ref key);
                }

                if (i == 0)
                    fht1.TakeIndexCheckpointAsync().GetAwaiter().GetResult();

                fht1.TakeHybridLogCheckpointAsync(checkpointType).GetAwaiter().GetResult();

                fht2.Recover();

                Assert.IsTrue(fht1.Log.HeadAddress == fht2.Log.HeadAddress);
                Assert.IsTrue(fht1.Log.ReadOnlyAddress == fht2.Log.ReadOnlyAddress);
                Assert.IsTrue(fht1.Log.TailAddress == fht2.Log.TailAddress);

                using var s2 = fht2.NewSession(new SimpleFunctions<long, long>());
                for (long key = 0; key < 1000 * i + 1000; key++)
                {
                    long output = default;
                    var status = s2.Read(ref key, ref output);
                    Assert.IsTrue(status == Status.OK && output == key);
                }
            }
        }


    }
}
