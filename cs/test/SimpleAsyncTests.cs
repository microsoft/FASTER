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
        public async Task AsyncMinParamTest()
        {
            using var s1 = fht1.NewSession(new SimpleFunctions<long, long>());
            for (long key = 0; key < numOps; key++)
            {
                s1.Upsert(ref key, ref key);
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
        public async Task AsyncMinParamTestNoDefault()
        {
            CancellationToken cancellationToken;

            using var s1 = fht1.NewSession(new SimpleFunctions<long, long>());
            for (long key = 0; key < numOps; key++)
            {
                s1.Upsert(ref key, ref key);
            }

            for (long key = 0; key < numOps; key++)
            {
                var (status, output) = (await s1.ReadAsync(ref key, Empty.Default, 99, cancellationToken)).Complete();
                Assert.IsTrue(status == Status.OK && output == key);
            }
        }

        // Test that does .ReadAsync no ref key (key)
        [Test]
        [Category("FasterKV")]
        public async Task AsyncNoRefKeyTest()
        {
            using var s1 = fht1.NewSession(new SimpleFunctions<long, long>());
            for (long key = 0; key < numOps; key++)
            {
                s1.Upsert(ref key, ref key);
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
        public async Task AsyncRefKeyRefInputTest()
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
        public async Task AsyncNoRefKeyNoRefInputTest()
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
        public async Task AsyncRMWAsyncNoRefTest()
        {
            Status status;
            long key = default, input = default, output = default;

            using var s1 = fht1.NewSession(new SimpleFunctions<long, long>((a, b) => a + b));
            for (key = 0; key < numOps; key++)
            {
                (await s1.RMWAsync(key, key)).Complete();
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
