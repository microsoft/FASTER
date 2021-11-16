// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using NUnit.Framework;
using FASTER.test.recovery.sumstore;
using System.Threading.Tasks;
using System.Threading;

namespace FASTER.test.async
{
    [TestFixture]
    public class SimpleAsyncTests
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

            path = TestUtils.MethodTestDir + "/";
            TestUtils.RecreateDirectory(path);
            log = Devices.CreateLogDevice(path + "Async.log", deleteOnClose: true);
            fht1 = new FasterKV<long, long>
                (1L << 10,
                logSettings: new LogSettings { LogDevice = log, MutableFraction = 1, PageSizeBits = 10, MemorySizeBits = 15 },
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

        // Test that does .ReadAsync with minimum parameters (ref key)
        [Test]
        [Category("FasterKV")]
        [Category("Smoke")]
        public async Task ReadAsyncMinParamTest()
        {
            using var s1 = fht1.NewSession(new SimpleFunctions<long, long>());
            for (long key = 0; key < numOps; key++)
            {
                var r = await s1.UpsertAsync(ref key, ref key);
                while (r.Status == Status.PENDING)
                    r = await r.CompleteAsync(); // test async version of Upsert completion
            }

            for (long key = 0; key < numOps; key++)
            {
                var (status, output) = (await s1.ReadAsync(ref key)).Complete();
                Assert.AreEqual(Status.OK, status);
                Assert.AreEqual(key, output);
            }
        }

        // Test that does .ReadAsync with minimum parameters but no default (ref key, userContext, serialNo, token)
        [Test]
        [Category("FasterKV")]
        public async Task ReadAsyncMinParamTestNoDefaultTest()
        {
            CancellationToken cancellationToken = default;

            using var s1 = fht1.NewSession(new SimpleFunctions<long, long>());
            for (long key = 0; key < numOps; key++)
            {
                var r = await s1.UpsertAsync(ref key, ref key);
                r.Complete(); // test sync version of Upsert completion
            }

            for (long key = 0; key < numOps; key++)
            {
                var (status, output) = (await s1.ReadAsync(ref key, Empty.Default, 99, cancellationToken)).Complete();
                Assert.AreEqual(Status.OK, status);
                Assert.AreEqual(key, output);
            }
        }

        // Test that does .ReadAsync no ref key (key)
        [Test]
        [Category("FasterKV")]
        [Category("Smoke")]
        public async Task ReadAsyncNoRefKeyTest()
        {
            using var s1 = fht1.NewSession(new SimpleFunctions<long, long>());
            for (long key = 0; key < numOps; key++)
            {
                var r = await s1.UpsertAsync(ref key, ref key);
                r.Complete(); // test sync version of Upsert completion
            }

            for (long key = 0; key < numOps; key++)
            {
                var (status, output) = (await s1.ReadAsync(key,Empty.Default, 99)).Complete();
                Assert.AreEqual(Status.OK, status);
                Assert.AreEqual(key, output);
            }
        }

        // Test that does .ReadAsync ref key and ref input (ref key, ref input)
        [Test]
        [Category("FasterKV")]
        public async Task ReadAsyncRefKeyRefInputTest()
        {
            Status status;
            long key = default, input = default, output = default;

            using var s1 = fht1.NewSession(new SimpleFunctions<long, long>((a, b) => a + b));
            for (key = 0; key < numOps; key++)
            {
                (await s1.RMWAsync(ref key, ref key)).Complete();
            }

            for (key = 0; key < numOps; key++)
            {
                (status, output) = (await s1.ReadAsync(ref key, ref output)).Complete();
                Assert.AreEqual(Status.OK, status);
                Assert.AreEqual(key, output);
            }

            key = 0;
            input = 35;
            var t1 = s1.RMWAsync(ref key, ref input);
            var t2 = s1.RMWAsync(ref key, ref input);

            (await t1).Complete();
            (await t2).Complete(); // should trigger RMW re-do

            (status, output) = (await s1.ReadAsync(ref key, ref output)).Complete();
            Assert.AreEqual(Status.OK, status);
            Assert.AreEqual(key + input + input, output);
        }


        // Test that does .ReadAsync no ref key and no ref input (key, input)
        [Test]
        [Category("FasterKV")]
        public async Task ReadAsyncNoRefKeyNoRefInputTest()
        {
            Status status;
            long key = default, input = default, output = default;

            using var s1 = fht1.NewSession(new RMWSimpleFunctions<long, long>((a, b) => a + b));
            for (key = 0; key < numOps; key++)
            {
                (status, output) = (await s1.RMWAsync(ref key, ref key, Empty.Default)).Complete();
                Assert.AreNotEqual(Status.PENDING, status);
                Assert.AreEqual(key, output);
            }

            for (key = 0; key < numOps; key++)
            {
                (status, output) = (await s1.ReadAsync(key, output)).Complete();
                Assert.AreEqual(Status.OK, status);
                Assert.AreEqual(key, output);
            }

            key = 0;
            input = 9912;
            var t1 = s1.RMWAsync(ref key, ref input);
            var t2 = s1.RMWAsync(ref key, ref input);

            (await t1).Complete();
            (await t2).Complete(); // should trigger RMW re-do

            (status, output) = (await s1.ReadAsync(key, output,Empty.Default, 129)).Complete();
            Assert.AreEqual(Status.OK, status);
            Assert.AreEqual(key + input + input, output);
        }

        // Test that does .UpsertAsync, .ReadAsync, .DeleteAsync, .ReadAsync with minimum parameters passed by reference (ref key)
        [Test]
        [Category("FasterKV")]
        [Category("Smoke")]
        public async Task UpsertReadDeleteReadAsyncMinParamByRefTest()
        {
            using var s1 = fht1.NewSession(new SimpleFunctions<long, long>());
            for (long key = 0; key < numOps; key++)
            {
                var r = await s1.UpsertAsync(ref key, ref key);
                while (r.Status == Status.PENDING)
                    r = await r.CompleteAsync(); // test async version of Upsert completion
            }

            Assert.Greater(numOps, 100);

            for (long key = 0; key < numOps; key++)
            {
                var (status, output) = (await s1.ReadAsync(ref key)).Complete();
                Assert.AreEqual(Status.OK, status);
                Assert.AreEqual(key, output);
            }

            {   // Scope for variables
                long deleteKey = 99;
                var r = await s1.DeleteAsync(ref deleteKey);
                while (r.Status == Status.PENDING)
                    r = await r.CompleteAsync(); // test async version of Delete completion

                var (status, _) = (await s1.ReadAsync(ref deleteKey)).Complete();
                Assert.AreEqual(Status.NOTFOUND, status);
            }
        }

        // Test that does .UpsertAsync, .ReadAsync, .DeleteAsync, .ReadAsync with minimum parameters passed by value (key)
        [Test]
        [Category("FasterKV")]
        [Category("Smoke")]
        public async Task UpsertReadDeleteReadAsyncMinParamByValueTest()
        {
            using var s1 = fht1.NewSession(new SimpleFunctions<long, long>());
            for (long key = 0; key < numOps; key++)
            {
                var status = (await s1.UpsertAsync(key, key)).Complete();   // test sync version of Upsert completion
                Assert.AreNotEqual(Status.PENDING, status);
            }

            Assert.Greater(numOps, 100);

            for (long key = 0; key < numOps; key++)
            {
                var (status, output) = (await s1.ReadAsync(key)).Complete();
                Assert.AreEqual(Status.OK, status);
                Assert.AreEqual(key, output);
            }

            {   // Scope for variables
                long deleteKey = 99;
                var status = (await s1.DeleteAsync(deleteKey)).Complete(); // test sync version of Delete completion
                Assert.AreNotEqual(Status.PENDING, status);

                (status, _) = (await s1.ReadAsync(deleteKey)).Complete();
                Assert.AreEqual(Status.NOTFOUND, status);
            }
        }

        // Test that uses StartAddress parameter
        // (ref key, ref input, StartAddress,  userContext, serialNo, CancellationToken)
        [Test]
        [Category("FasterKV")]
        public async Task AsyncStartAddressParamTest()
        {
            Status status;
            long key = default, input = default, output = default;

            var addresses = new long[numOps];
            long recordSize = fht1.Log.FixedRecordSize;

            using var s1 = fht1.NewSession(new RMWSimpleFunctions<long, long>((a, b) => a + b));
            for (key = 0; key < numOps; key++)
            {
                // We can predict the address as TailAddress because we're single-threaded, *unless* a page was allocated;
                // in that case the new address is at the start of the newly-allocated page. Since we can't predict that,
                // we take advantage of knowing we have fixed-length records and that TailAddress is open-ended, so we
                // subtract after the insert to get record start address.
                (status, output) = (await s1.RMWAsync(ref key, ref key)).Complete();
                addresses[key] = fht1.Log.TailAddress - recordSize;
                Assert.AreNotEqual(Status.PENDING, status);
                Assert.AreEqual(key, output);
            }

            for (key = 0; key < numOps; key++)
            {
                (status, output) = (await s1.ReadAsync(ref key, ref output, addresses[key], ReadFlags.None)).Complete();
                Assert.AreEqual(Status.OK, status);
                Assert.AreEqual(key, output);
            }

            key = 0;
            input = 22;
            var t1 = s1.RMWAsync(ref key, ref input);
            var t2 = s1.RMWAsync(ref key, ref input);

            (await t1).Complete();
            (await t2).Complete(); // should trigger RMW re-do

            // Because of our small log-memory size, RMW of key 0 causes an RCW (Read-Copy-Write) and an insertion at the tail
            // of the log. Use the same pattern as above to get the new record address.
            addresses[key] = fht1.Log.TailAddress - recordSize;

            (status, output) = (await s1.ReadAsync(ref key, ref output, addresses[key], ReadFlags.None, Empty.Default, 129)).Complete();
            Assert.AreEqual(Status.OK, status);
            Assert.AreEqual(key + input + input, output);
        }

        // Test of RMWAsync where No ref used
        [Test]
        [Category("FasterKV")]
        public async Task ReadAsyncRMWAsyncNoRefTest()
        {
            Status status;
            long key = default, input = default, output = default;

            using var s1 = fht1.NewSession(new RMWSimpleFunctions<long, long>((a, b) => a + b));
            for (key = 0; key < numOps; key++)
            {
                var asyncResult = await (await s1.RMWAsync(key, key)).CompleteAsync();
                Assert.AreNotEqual(Status.PENDING, asyncResult.Status);
                Assert.AreEqual(key, asyncResult.Output);
            }

            for (key = 0; key < numOps; key++)
            {
                (status, output) = (await s1.ReadAsync(ref key, ref output)).Complete();
                Assert.AreEqual(Status.OK, status);
                Assert.AreEqual(key, output);
            }

            key = 0;
            input = 35;
            var t1 = s1.RMWAsync(key, input);
            var t2 = s1.RMWAsync(key, input);

            (await t1).Complete();
            (await t2).Complete(); // should trigger RMW re-do

            (status, output) = (await s1.ReadAsync(ref key, ref output)).Complete();
            Assert.AreEqual(Status.OK, status);
            Assert.AreEqual(key + input + input, output);
        }

        // Test of ReadyToCompletePendingAsync
        // Note: This should be looked into more to make it more of a "test" with proper verfication vs calling it to make sure just pop exception
        [Test]
        [Category("FasterKV")]
        public async Task ReadyToCompletePendingAsyncTest()
        {
            Status status;
            long key = default, input = default, output = default;

            using var s1 = fht1.NewSession(new SimpleFunctions<long, long>((a, b) => a + b));
            for (key = 0; key < numOps; key++)
            {
                (await s1.RMWAsync(key, key)).Complete();

                await s1.ReadyToCompletePendingAsync();
            }

            for (key = 0; key < numOps; key++)
            {
                (status, output) = (await s1.ReadAsync(ref key, ref output)).Complete();
                Assert.AreEqual(Status.OK, status);
                Assert.AreEqual(key, output);
            }

            key = 0;
            input = 35;
            var t1 = s1.RMWAsync(key, input);
            var t2 = s1.RMWAsync(key, input);

            (await t1).Complete();
            (await t2).Complete(); // should trigger RMW re-do

            (status, output) = (await s1.ReadAsync(ref key, ref output)).Complete();
            Assert.AreEqual(Status.OK, status);
            Assert.AreEqual(key + input + input, output);
        }

        // Test that does both UpsertAsync and RMWAsync to populate the FasterKV and update it, possibly after flushing it from memory.
        [Test]
        [Category("FasterKV")]
        [Category("Smoke")]
        public async Task UpsertAsyncAndRMWAsyncTest([Values] bool useRMW, [Values] bool doFlush, [Values] bool completeAsync)
        {
            using var s1 = fht1.NewSession(new SimpleFunctions<long, long>());

            async ValueTask completeRmw(FasterKV<long, long>.RmwAsyncResult<long, long, Empty> ar)
            {
                if (completeAsync)
                {
                    while (ar.Status == Status.PENDING)
                        ar = await ar.CompleteAsync(); // test async version of Upsert completion
                    return;
                }
                ar.Complete();
            }

            async ValueTask completeUpsert(FasterKV<long, long>.UpsertAsyncResult<long, long, Empty> ar)
            {
                if (completeAsync)
                {
                    while (ar.Status == Status.PENDING)
                        ar = await ar.CompleteAsync(); // test async version of Upsert completion
                    return;
                }
                ar.Complete();
            }

            for (long key = 0; key < numOps; key++)
            {
                if (useRMW)
                    await completeRmw(await s1.RMWAsync(key, key));
                else
                    await completeUpsert(await s1.UpsertAsync(key, key));
            }

            if (doFlush)
                fht1.Log.FlushAndEvict(wait: true);

            for (long key = 0; key < numOps; key++)
            {
                if (useRMW)
                    await completeRmw(await s1.RMWAsync(key, key + numOps));
                else
                    await completeUpsert(await s1.UpsertAsync(key, key + numOps));
            }
        }
    }
}
