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

            path = TestContext.CurrentContext.TestDirectory + "/SimpleAsyncTests/";
            log = Devices.CreateLogDevice(path + "Async.log", deleteOnClose: true);
            Directory.CreateDirectory(path);
            fht1 = new FasterKV<long, long>
                (1L << 10,
                logSettings: new LogSettings { LogDevice = log, MutableFraction = 1, PageSizeBits = 10, MemorySizeBits = 15 },
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

        // Test that does .ReadAsync with minimum parameters (ref key)
        [Test]
        [Category("FasterKV")]
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
                Assert.IsTrue(status == Status.OK && output == key);
            }
        }

        // Test that does .ReadAsync with minimum parameters but no default (ref key, userContext, serialNo, token)
        [Test]
        [Category("FasterKV")]
        public async Task ReadAsyncMinParamTestNoDefaultTest0()
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
                var (status, output) = (await s1.ReadAsync(ref key, Empty.Default, 99)).Complete();
                Assert.IsTrue(status == Status.OK && output == key);
            }
        }

        [Test]
        [Category("FasterKV")]
        public async Task ReadAsyncMinParamTestNoDefaultTest1()
        {
            CancellationToken cancellationToken;

            using var s1 = fht1.NewSession(new SimpleFunctions<long, long>());
            for (long key = 0; key < numOps; key++)
            {
                var r = await s1.UpsertAsync(ref key, ref key);
                r.Complete(); // test sync version of Upsert completion
            }

            //for (long key = 0; key < numOps; key++)
            //{
            //    var (status, output) = (await s1.ReadAsync(ref key, Empty.Default, 99, cancellationToken)).Complete();
            //    Assert.IsTrue(status == Status.OK && output == key);
            //}
        }

        // Test that does .ReadAsync no ref key (key)
        [Test]
        [Category("FasterKV")]
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
                Assert.IsTrue(status == Status.OK && output == key);
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
                Assert.IsTrue(status == Status.OK && output == key);
            }

            key = 0;
            input = 35;
            var t1 = s1.RMWAsync(ref key, ref input);
            var t2 = s1.RMWAsync(ref key, ref input);

            (await t1).Complete();
            (await t2).Complete(); // should trigger RMW re-do

            (status, output) = (await s1.ReadAsync(ref key, ref output)).Complete();
            Assert.IsTrue(status == Status.OK && output == key + input + input);
        }


        // Test that does .ReadAsync no ref key and no ref input (key, input)
        [Test]
        [Category("FasterKV")]
        public async Task ReadAsyncNoRefKeyNoRefInputTest()
        {
            Status status;
            long key = default, input = default, output = default;

            using var s1 = fht1.NewSession(new SimpleFunctions<long, long>((a, b) => a + b));
            for (key = 0; key < numOps; key++)
            {
                (await s1.RMWAsync(ref key, ref key,Empty.Default)).Complete();
            }

            for (key = 0; key < numOps; key++)
            {
                (status, output) = (await s1.ReadAsync(key, output)).Complete();
                Assert.IsTrue(status == Status.OK && output == key);
            }

            key = 0;
            input = 9912;
            var t1 = s1.RMWAsync(ref key, ref input);
            var t2 = s1.RMWAsync(ref key, ref input);

            (await t1).Complete();
            (await t2).Complete(); // should trigger RMW re-do

            (status, output) = (await s1.ReadAsync(key, output,Empty.Default, 129)).Complete();
            Assert.IsTrue(status == Status.OK && output == key + input + input);
        }

        // Test that does .UpsertAsync, .ReadAsync, .DeleteAsync, .ReadAsync with minimum parameters passed by reference (ref key)
        [Test]
        [Category("FasterKV")]
        public async Task UpsertReadDeleteReadAsyncMinParamByRefTest()
        {
            using var s1 = fht1.NewSession(new SimpleFunctions<long, long>());
            for (long key = 0; key < numOps; key++)
            {
                var r = await s1.UpsertAsync(ref key, ref key);
                while (r.Status == Status.PENDING)
                    r = await r.CompleteAsync(); // test async version of Upsert completion
            }

            Assert.IsTrue(numOps > 100);

            for (long key = 0; key < numOps; key++)
            {
                var (status, output) = (await s1.ReadAsync(ref key)).Complete();
                Assert.IsTrue(status == Status.OK && output == key);
            }

            {   // Scope for variables
                long deleteKey = 99;
                var r = await s1.DeleteAsync(ref deleteKey);
                while (r.Status == Status.PENDING)
                    r = await r.CompleteAsync(); // test async version of Delete completion

                var (status, _) = (await s1.ReadAsync(ref deleteKey)).Complete();
                Assert.IsTrue(status == Status.NOTFOUND);
            }
        }

        // Test that does .UpsertAsync, .ReadAsync, .DeleteAsync, .ReadAsync with minimum parameters passed by value (key)
        [Test]
        [Category("FasterKV")]
        public async Task UpsertReadDeleteReadAsyncMinParamByValueTest()
        {
            using var s1 = fht1.NewSession(new SimpleFunctions<long, long>());
            for (long key = 0; key < numOps; key++)
            {
                var status = (await s1.UpsertAsync(key, key)).Complete();   // test sync version of Upsert completion
                Assert.AreNotEqual(Status.PENDING, status);
            }

            Assert.IsTrue(numOps > 100);

            for (long key = 0; key < numOps; key++)
            {
                var (status, output) = (await s1.ReadAsync(key)).Complete();
                Assert.IsTrue(status == Status.OK && output == key);
            }

            {   // Scope for variables
                long deleteKey = 99;
                var status = (await s1.DeleteAsync(deleteKey)).Complete(); // test sync version of Delete completion
                Assert.AreNotEqual(Status.PENDING, status);

                (status, _) = (await s1.ReadAsync(deleteKey)).Complete();
                Assert.IsTrue(status == Status.NOTFOUND);
            }
        }

        /* ** TO DO: Using StartAddress in ReadAsync is now obsolete - might be design change etc but until then, commenting out test **
         * 
        // Test that uses StartAddress parameter
        // (ref key, ref input, StartAddress,  userContext, serialNo, CancellationToken)
        [Test]
        [Category("FasterKV")]
        public async Task AsyncStartAddressParamTest()
        {
            Status status;
            CancellationToken cancellationToken;
            long key = default, input = default, output = default;
            var readAtAddress = fht1.Log.BeginAddress;

            using var s1 = fht1.NewSession(new SimpleFunctions<long, long>((a, b) => a + b));
            for (key = 0; key < numOps; key++)
            {
                (await s1.RMWAsync(ref key, ref key)).Complete();
            }

            for (key = 0; key < numOps; key++)
            {
                (status, output) = (await s1.ReadAsync(ref key, ref output, readAtAddress, ReadFlags.None)).Complete();
                Assert.IsTrue(status == Status.OK && output == key);
            }

            key = 0;
            input = 22;
            var t1 = s1.RMWAsync(ref key, ref input);
            var t2 = s1.RMWAsync(ref key, ref input);

            (await t1).Complete();
            (await t2).Complete(); // should trigger RMW re-do

            (status, output) = (await s1.ReadAsync(ref key, ref output, readAtAddress, ReadFlags.None, Empty.Default, 129, cancellationToken)).Complete();
            Assert.IsTrue(status == Status.OK && output == key + input + input);
        }
        */

        // Test of RMWAsync where No ref used
        [Test]
        [Category("FasterKV")]
        public async Task ReadAsyncRMWAsyncNoRefTest()
        {
            Status status;
            long key = default, input = default, output = default;

            using var s1 = fht1.NewSession(new SimpleFunctions<long, long>((a, b) => a + b));
            for (key = 0; key < numOps; key++)
            {
                status = (await s1.RMWAsync(key, key)).Complete();
                Assert.AreNotEqual(Status.PENDING, status);
            }

            for (key = 0; key < numOps; key++)
            {
                (status, output) = (await s1.ReadAsync(ref key, ref output)).Complete();
                Assert.IsTrue(status == Status.OK && output == key);
            }

            key = 0;
            input = 35;
            var t1 = s1.RMWAsync(key, input);
            var t2 = s1.RMWAsync(key, input);

            (await t1).Complete();
            (await t2).Complete(); // should trigger RMW re-do

            (status, output) = (await s1.ReadAsync(ref key, ref output)).Complete();
            Assert.IsTrue(status == Status.OK && output == key + input + input);
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
                Assert.IsTrue(status == Status.OK && output == key);
            }

            key = 0;
            input = 35;
            var t1 = s1.RMWAsync(key, input);
            var t2 = s1.RMWAsync(key, input);

            (await t1).Complete();
            (await t2).Complete(); // should trigger RMW re-do

            (status, output) = (await s1.ReadAsync(ref key, ref output)).Complete();
            Assert.IsTrue(status == Status.OK && output == key + input + input);
        }



    }
}
