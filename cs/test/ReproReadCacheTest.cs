// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using FASTER.core;
using NUnit.Framework;
using static FASTER.test.TestUtils;

namespace FASTER.test.ReadCacheTests
{
    [TestFixture]
    internal class RandomReadCacheTests
    {
        public class Context
        {
            public Status Status { get; set; }
        }

        class Functions : FunctionsBase<SpanByte, long, long, long, Context>
        {
            public override bool ConcurrentReader(ref SpanByte key, ref long input, ref long value, ref long dst, ref ReadInfo readInfo)
            {
                dst = value;
                return true;
            }

            public override bool SingleReader(ref SpanByte key, ref long input, ref long value, ref long dst, ref ReadInfo readInfo)
            {
                dst = value;
                return true;
            }

            public override void ReadCompletionCallback(ref SpanByte key, ref long input, ref long output, Context context, Status status, RecordMetadata recordMetadata)
            {
                Assert.IsTrue(status.Found);
                Assert.AreEqual(input, output);
                context.Status = status;
            }
        }

        IDevice log = default;
        FasterKV<SpanByte, long> fht = default;

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);

            ReadCacheSettings readCacheSettings = default;
            bool disableEphemeralLocking = false;
            string filename = MethodTestDir + "/BasicFasterTests.log";

            foreach (var arg in TestContext.CurrentContext.Test.Arguments)
            {
                if (arg is ReadCacheMode rcm)
                {
                    if (rcm == ReadCacheMode.UseReadCache)
                        readCacheSettings = new()
                        {
                            MemorySizeBits = 15,
                            PageSizeBits = 12,
                            SecondChanceFraction = 0.1,
                        };
                    continue;
                }
                if (arg is EphemeralLockingMode elm)
                {
                    disableEphemeralLocking = elm == EphemeralLockingMode.NoEphemeralLocking;
                    continue;
                }
                if (arg is DeviceType deviceType)
                {
                    log = CreateTestDevice(deviceType, filename, deleteOnClose: true);
                    continue;
                }
            }
            this.log ??= Devices.CreateLogDevice(filename, deleteOnClose: true);

            fht = new FasterKV<SpanByte, long>(
                size: 1L << 20,
                new LogSettings
                {
                    LogDevice = log,
                    MemorySizeBits = 15,
                    PageSizeBits = 12,
                    ReadCacheSettings = readCacheSettings,
                }, disableEphemeralLocking: disableEphemeralLocking);
        }

        [TearDown]
        public void TearDown()
        {
            fht?.Dispose();
            fht = default;
            log?.Dispose();
            log = default;
            DeleteDirectory(MethodTestDir);
        }

        [Test]
        [Category(FasterKVTestCategory)]
        [Category(ReadCacheTestCategory)]
        [Category(StressTestCategory)]
        //[Repeat(300)]
        public unsafe void RandomReadCacheTest([Values(1, 2, 4, 8)] int numThreads, [Values] KeyContentionMode keyContentionMode,
                                                [Values] EphemeralLockingMode ephemeralLockingMode, [Values] ReadCacheMode readCacheMode,
#if WINDOWS
                                                [Values(DeviceType.LSD
#else
                                                [Values(DeviceType.MLSD
#endif
                                                )] DeviceType deviceType)
        {
            if (numThreads == 1 && keyContentionMode == KeyContentionMode.Contention)
                Assert.Ignore("Skipped because 1 thread cannot have contention");
            if (numThreads > 2 && TestUtils.IsRunningAzureTests)
                Assert.Ignore("Skipped because > 2 threads when IsRunningAzureTests");
            if (TestContext.CurrentContext.CurrentRepeatCount > 0)
                Debug.WriteLine($"*** Current test iteration: {TestContext.CurrentContext.CurrentRepeatCount + 1} ***");

            void LocalRead(BasicContext<SpanByte, long, long, long, Context, Functions> sessionContext, int i)
            {
                var keyString = $"{i}";
                var key = MemoryMarshal.Cast<char, byte>(keyString.AsSpan());
                fixed (byte* _ = key)
                {
                    var context = new Context();
                    var sb = SpanByte.FromFixedSpan(key);
                    long input = i * 2;
                    long output = 0;
                    var status = sessionContext.Read(ref sb, ref input, ref output, context);

                    if (status.Found)
                    {
                        Assert.AreEqual(input, output);
                        return;
                    }

                    Assert.IsTrue(status.IsPending, $"was not Pending: {keyString}; status {status}");
                    sessionContext.CompletePending(wait: true);
                }
            }

            void LocalRun(int startKey, int endKey)
            {
                var session = fht.For(new Functions()).NewSession<Functions>();
                var sessionContext = session.BasicContext;

                // read through the keys in order (works)
                for (int i = startKey; i < endKey; i++)
                    LocalRead(sessionContext, i);

                // pick random keys to read
                var r = new Random(2115);
                for (int i = startKey; i < endKey; i++)
                    LocalRead(sessionContext, r.Next(startKey, endKey));
            }

            const int MaxKeys = 24000;

            { // Write the values first (single-threaded, all keys)
                var session = fht.For(new Functions()).NewSession<Functions>();
                for (int i = 0; i < MaxKeys; i++)
                {
                    var keyString = $"{i}";
                    var key = MemoryMarshal.Cast<char, byte>(keyString.AsSpan());
                    fixed (byte* _ = key)
                    {
                        var sb = SpanByte.FromFixedSpan(key);
                        var status = session.Upsert(sb, i * 2);
                        Assert.IsTrue(!status.Found && status.Record.Created, status.ToString());
                    }
                }
            }

            if (numThreads == 1)
            {
                LocalRun(0, MaxKeys);
                return;
            }

            var numKeysPerThread = MaxKeys / numThreads;

            List<Task> tasks = new();   // Task rather than Thread for propagation of exception.
            for (int t = 0; t < numThreads; t++)
            {
                var tid = t;
                if (keyContentionMode == KeyContentionMode.Contention)
                    tasks.Add(Task.Factory.StartNew(() => LocalRun(0, MaxKeys)));
                else
                    tasks.Add(Task.Factory.StartNew(() => LocalRun(numKeysPerThread * tid, numKeysPerThread * (tid + 1))));
            }
            Task.WaitAll(tasks.ToArray());
        }
    }
}
