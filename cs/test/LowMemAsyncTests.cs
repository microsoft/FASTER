// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System.IO;
using NUnit.Framework;
using System.Threading.Tasks;

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
            path = TestUtils.MethodTestDir;
            TestUtils.DeleteDirectory(path, wait: true);
            log = new LocalMemoryDevice(1L << 30, 1L << 25, 1, latencyMs: 20);
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
            fht1?.Dispose();
            fht1 = null;
            log?.Dispose();
            log = null;
            TestUtils.DeleteDirectory(path);
        }

        private static async Task Populate(ClientSession<long, long, long, long, Empty, IFunctions<long, long, long, long, Empty>> s1)
        {
            var tasks = new ValueTask<FasterKV<long, long>.UpsertAsyncResult<long, long, Empty>>[numOps];
            for (long key = 0; key < numOps; key++)
            {
                tasks[key] = s1.UpsertAsync(ref key, ref key);
            }

            for (var done = false; !done; /* set in loop */)
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

            // This should return immediately, if we have no async concurrency issues in pending count management.
            s1.CompletePending(true);
        }

        [Test]
        [Category("FasterKV")]
        [Category("Stress")]
        public async Task LowMemConcurrentUpsertReadAsyncTest()
        {
            await Task.Yield();
            using var s1 = fht1.NewSession(new SimpleFunctions<long, long>((a, b) => a + b));

            await Populate(s1).ConfigureAwait(false);

            // Read all keys
            var readtasks = new ValueTask<FasterKV<long, long>.ReadAsyncResult<long, long, Empty>>[numOps];
            for (long key = 0; key < numOps; key++)
                readtasks[key] = s1.ReadAsync(ref key, ref key);

            for (long key = 0; key < numOps; key++)
            {
                var (status, output) = (await readtasks[key].ConfigureAwait(false)).Complete();
                Assert.AreEqual(Status.OK, status);
                Assert.AreEqual(key, output);
            }
        }

        [Test]
        [Category("FasterKV")]
        [Category("Stress")]
        public async Task LowMemConcurrentUpsertRMWReadAsyncTest()
        {
            await Task.Yield();
            using var s1 = fht1.NewSession(new SimpleFunctions<long, long>((a, b) => a + b));

            await Populate(s1).ConfigureAwait(false);

            // RMW all keys
            var rmwtasks = new ValueTask<FasterKV<long, long>.RmwAsyncResult<long, long, Empty>>[numOps];
            for (long key = 0; key < numOps; key++)
                rmwtasks[key] = s1.RMWAsync(ref key, ref key);

            for (var done = false; !done; /* set in loop */)
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
                readtasks[key] = s1.ReadAsync(ref key, ref key);

            for (long key = 0; key < numOps; key++)
            {
                var (status, output) = (await readtasks[key].ConfigureAwait(false)).Complete();
                Assert.AreEqual(Status.OK, status);
                Assert.AreEqual(key + key, output);
            }
        }
    }
}
