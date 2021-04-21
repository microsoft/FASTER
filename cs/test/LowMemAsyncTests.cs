// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System.IO;
using NUnit.Framework;
using FASTER.test.recovery.sumstore;
using System.Threading.Tasks;
using System.Threading;

namespace FASTER.test.async
{

    [TestFixture]
    public class LowMemAsyncTests
    {
        IDevice log;
        FasterKV<long, long> fht1;
        const int numOps = 5000;
        string path;

        [SetUp]
        public void Setup()
        {
            path = TestContext.CurrentContext.TestDirectory + "/SimpleAsyncTests/";
             log = new LocalMemoryDevice(1L << 30, 1L << 25, 1, latencyMs: 20);
             // log = Devices.CreateLogDevice(path + "Async.log", deleteOnClose: true);
            Directory.CreateDirectory(path);
            fht1 = new FasterKV<long, long>
                (1L << 10,
                logSettings: new LogSettings { LogDevice = log, MutableFraction = 1, PageSizeBits = 10, MemorySizeBits = 12 },
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
        [Category("FasterKV")]
        public async Task ConcurrentUpsertReadAsyncTest()
        {
            await Task.Yield();
            using var s1 = fht1.NewSession(new SimpleFunctions<long, long>((a, b) => a + b));

            // First Upsert all keys
            var tasks = new ValueTask<FasterKV<long, long>.UpsertAsyncResult<long, long, Empty>>[numOps];
            for (long key = 0; key < numOps; key++)
            {
                tasks[key] = s1.UpsertAsync(ref key, ref key);
            }

            bool done = false;
            while (!done)
            {
                done = true;
                for (long key = 0; key < numOps; key++)
                {
                    var result = await tasks[key].ConfigureAwait(false);
                    if (result.Status == Status.PENDING)
                    {
                        done = false;
                        tasks[key] = result.CompleteAsync();

                    }
                }
            }

            // Then Read all keys
            var readtasks = new ValueTask<FasterKV<long, long>.ReadAsyncResult<long, long, Empty>>[numOps];
            for (long key = 0; key < numOps; key++)
            {
                readtasks[key] = s1.ReadAsync(ref key, ref key);
            }

            for (long key = 0; key < numOps; key++)
            {
                var result = (await readtasks[key].ConfigureAwait(false)).Complete();
                Assert.IsTrue(result.status == Status.OK && result.output == key);
            }
        }

        [Test]
        [Category("FasterKV")]
        public async Task ConcurrentUpsertRMWReadAsyncTest()
        {
            await Task.Yield();
            using var s1 = fht1.NewSession(new SimpleFunctions<long, long>((a, b) => a + b));

            // First upsert all keys
            var tasks = new ValueTask<FasterKV<long, long>.UpsertAsyncResult<long, long, Empty>>[numOps];
            for (long key = 0; key < numOps; key++)
            {
                tasks[key] = s1.UpsertAsync(ref key, ref key);
            }

            bool done = false;
            while (!done)
            {
                done = true;
                for (long key = 0; key < numOps; key++)
                {
                    var result = await tasks[key].ConfigureAwait(false);
                    if (result.Status == Status.PENDING)
                    {
                        done = false;
                        tasks[key] = result.CompleteAsync();
                    }
                }
            }

            // Then RMW all keys
            var rmwtasks = new ValueTask<FasterKV<long, long>.RmwAsyncResult<long, long, Empty>>[numOps];
            for (long key = 0; key < numOps; key++)
            {
                rmwtasks[key] = s1.RMWAsync(ref key, ref key);
            }

            done = false;
            while (!done)
            {
                done = true;
                for (long key = 0; key < numOps; key++)
                {
                    var result = await rmwtasks[key].ConfigureAwait(false);
                    if (result.Status == Status.PENDING)
                    {
                        done = false;
                        rmwtasks[key] = result.CompleteAsync();
                    }
                }
            }

            // Then Read all keys
            var readtasks = new ValueTask<FasterKV<long, long>.ReadAsyncResult<long, long, Empty>>[numOps];
            for (long key = 0; key < numOps; key++)
            {
                readtasks[key] = s1.ReadAsync(ref key, ref key);
            }

            for (long key = 0; key < numOps; key++)
            {
                var result = (await readtasks[key].ConfigureAwait(false)).Complete();
                Assert.IsTrue(result.status == Status.OK && result.output == key + key);
            }
        }
    }
}
