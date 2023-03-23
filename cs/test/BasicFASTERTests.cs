// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Diagnostics;
using System.Linq;
using FASTER.core;
using NUnit.Framework;

namespace FASTER.test
{
    //** NOTE - more detailed / in depth Read tests in ReadAddressTests.cs 
    //** These tests ensure the basics are fully covered

    [TestFixture]
    internal class BasicFASTERTests
    {
        private FasterKV<KeyStruct, ValueStruct> fht;
        private ClientSession<KeyStruct, ValueStruct, InputStruct, OutputStruct, Empty, Functions> session;
        private IDevice log;
        private string path;
        TestUtils.DeviceType deviceType;

        [SetUp]
        public void Setup()
        {
            path = TestUtils.MethodTestDir + "/";

            // Clean up log files from previous test runs in case they weren't cleaned up
            TestUtils.DeleteDirectory(path, wait: true);
        }

        private void Setup(long size, LogSettings logSettings, TestUtils.DeviceType deviceType, int latencyMs = TestUtils.DefaultLocalMemoryDeviceLatencyMs)
        {
            string filename = path + TestContext.CurrentContext.Test.Name + deviceType.ToString() + ".log";
            log = TestUtils.CreateTestDevice(deviceType, filename, latencyMs: latencyMs);
            logSettings.LogDevice = log;
            fht = new FasterKV<KeyStruct, ValueStruct>(size, logSettings);
            session = fht.For(new Functions()).NewSession<Functions>();
        }

        [TearDown]
        public void TearDown()
        {
            session?.Dispose();
            session = null;
            fht?.Dispose();
            fht = null;
            log?.Dispose();
            log = null;
            TestUtils.DeleteDirectory(path);
        }

        private void AssertCompleted(Status expected, Status actual)
        {
            if (actual.IsPending)
                (actual, _) = CompletePendingResult();
            Assert.AreEqual(expected, actual);
        }

        private (Status status, OutputStruct output) CompletePendingResult()
        {
            session.CompletePendingWithOutputs(out var completedOutputs, wait: true);
            return TestUtils.GetSinglePendingResult(completedOutputs);
        }

        [Test]
        [Category("FasterKV")]
        [Category("Smoke")]
        public void NativeInMemWriteRead([Values] TestUtils.DeviceType deviceType)
        {
            Setup(128, new LogSettings { PageSizeBits = 10, MemorySizeBits = 12, SegmentSizeBits = 22 }, deviceType);

            InputStruct input = default;
            OutputStruct output = default;

            var key1 = new KeyStruct { kfield1 = 13, kfield2 = 14 };
            var value = new ValueStruct { vfield1 = 23, vfield2 = 24 };

            session.Upsert(ref key1, ref value, Empty.Default, 0);
            var status = session.Read(ref key1, ref input, ref output, Empty.Default, 0);

            AssertCompleted(new(StatusCode.Found), status);
            Assert.AreEqual(value.vfield1, output.value.vfield1);
            Assert.AreEqual(value.vfield2, output.value.vfield2);
        }

        [Test]
        [Category("FasterKV")]
        [Category("Smoke")]
        public void NativeInMemWriteReadDelete([Values] TestUtils.DeviceType deviceType)
        {
            Setup(128, new LogSettings { PageSizeBits = 10, MemorySizeBits = 12, SegmentSizeBits = 22 }, deviceType);

            InputStruct input = default;
            OutputStruct output = default;

            var key1 = new KeyStruct { kfield1 = 13, kfield2 = 14 };
            var value = new ValueStruct { vfield1 = 23, vfield2 = 24 };

            session.Upsert(ref key1, ref value, Empty.Default, 0);
            var status = session.Read(ref key1, ref input, ref output, Empty.Default, 0);
            AssertCompleted(new(StatusCode.Found), status);

            session.Delete(ref key1, Empty.Default, 0);

            status = session.Read(ref key1, ref input, ref output, Empty.Default, 0);
            AssertCompleted(new(StatusCode.NotFound), status);

            var key2 = new KeyStruct { kfield1 = 14, kfield2 = 15 };
            var value2 = new ValueStruct { vfield1 = 24, vfield2 = 25 };

            session.Upsert(ref key2, ref value2, Empty.Default, 0);
            status = session.Read(ref key2, ref input, ref output, Empty.Default, 0);

            AssertCompleted(new(StatusCode.Found), status);
            Assert.AreEqual(value2.vfield1, output.value.vfield1);
            Assert.AreEqual(value2.vfield2, output.value.vfield2);
        }


        [Test]
        [Category("FasterKV")]
        [Category("Smoke")]
        public void NativeInMemWriteReadDelete2()
        {
            // Just set this one since Write Read Delete already does all four devices
            deviceType = TestUtils.DeviceType.MLSD;

            const int count = 10;

            // Setup(128, new LogSettings { MemorySizeBits = 22, SegmentSizeBits = 22, PageSizeBits = 10 }, deviceType);
            Setup(128, new LogSettings { MemorySizeBits = 29 }, deviceType);

            InputStruct input = default;
            OutputStruct output = default;

            for (int i = 0; i < 10 * count; i++)
            {
                var key1 = new KeyStruct { kfield1 = i, kfield2 = 14 };
                var value = new ValueStruct { vfield1 = i, vfield2 = 24 };

                session.Upsert(ref key1, ref value, Empty.Default, 0);
            }

            for (int i = 0; i < 10 * count; i++)
            {
                var key1 = new KeyStruct { kfield1 = i, kfield2 = 14 };
                session.Delete(ref key1, Empty.Default, 0);
            }

            for (int i = 0; i < 10 * count; i++)
            {
                var key1 = new KeyStruct { kfield1 = i, kfield2 = 14 };
                var value = new ValueStruct { vfield1 = i, vfield2 = 24 };

                var status = session.Read(ref key1, ref input, ref output, Empty.Default, 0);
                AssertCompleted(new(StatusCode.NotFound), status);

                session.Upsert(ref key1, ref value, Empty.Default, 0);
            }

            for (int i = 0; i < 10 * count; i++)
            {
                var key1 = new KeyStruct { kfield1 = i, kfield2 = 14 };
                var status = session.Read(ref key1, ref input, ref output, Empty.Default, 0);
                AssertCompleted(new(StatusCode.Found), status);
            }
        }

        [Test]
        [Category("FasterKV")]
        [Category("Smoke")]
        public unsafe void NativeInMemWriteRead2()
        {
            // Just use this one instead of all four devices since InMemWriteRead covers all four devices
            deviceType = TestUtils.DeviceType.MLSD;

            int count = 200;

            // Setup(128, new LogSettings { MemorySizeBits = 22, SegmentSizeBits = 22, PageSizeBits = 10 }, deviceType);
            Setup(128, new LogSettings { MemorySizeBits = 29 }, deviceType);
            session = fht.For(new Functions()).NewSession<Functions>();

            InputStruct input = default;

            Random r = new(10);
            for (int c = 0; c < count; c++)
            {
                var i = r.Next(10000);
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };
                session.Upsert(ref key1, ref value, Empty.Default, 0);
            }

            r = new Random(10);

            for (int c = 0; c < count; c++)
            {
                var i = r.Next(10000);
                OutputStruct output = default;
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };

                if (session.Read(ref key1, ref input, ref output, Empty.Default, 0).IsPending)
                {
                    session.CompletePending(true);
                }

                Assert.AreEqual(value.vfield1, output.value.vfield1);
                Assert.AreEqual(value.vfield2, output.value.vfield2);
            }

            // Clean up and retry - should not find now
            fht.Log.ShiftBeginAddress(fht.Log.TailAddress, truncateLog: true);

            r = new Random(10);
            for (int c = 0; c < count; c++)
            {
                var i = r.Next(10000);
                OutputStruct output = default;
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                Assert.IsFalse(session.Read(ref key1, ref input, ref output, Empty.Default, 0).Found);
            }
        }

        [Test]
        [Category("FasterKV")]
        [Category("Smoke")]
        public unsafe void TestShiftHeadAddress([Values] TestUtils.DeviceType deviceType, [Values] TestUtils.BatchMode batchMode)
        {
            InputStruct input = default;
            const int RandSeed = 10;
            const int RandRange = 1000000;
            const int NumRecs = 2000;

            Random r = new(RandSeed);
            var sw = Stopwatch.StartNew();

            var latencyMs = batchMode == TestUtils.BatchMode.NoBatch ? 0 : TestUtils.DefaultLocalMemoryDeviceLatencyMs;
            Setup(128, new LogSettings { MemorySizeBits = 22, SegmentSizeBits = 22, PageSizeBits = 10 }, deviceType, latencyMs: latencyMs);

            for (int c = 0; c < NumRecs; c++)
            {
                var i = r.Next(RandRange);
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };
                session.Upsert(ref key1, ref value, Empty.Default, 0);
            }

            r = new Random(RandSeed);
            sw.Restart();

            for (int c = 0; c < NumRecs; c++)
            {
                var i = r.Next(RandRange);
                OutputStruct output = default;
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };

                if (session.Read(ref key1, ref input, ref output, Empty.Default, 0).IsPending)
                {
                    Assert.AreEqual(value.vfield1, output.value.vfield1);
                    Assert.AreEqual(value.vfield2, output.value.vfield2);
                }
            }
            session.CompletePending(true);

            // Shift head and retry - should not find in main memory now
            fht.Log.FlushAndEvict(true);

            r = new Random(RandSeed);
            sw.Restart();

            const int batchSize = 256;
            for (int c = 0; c < NumRecs; c++)
            {
                var i = r.Next(RandRange);
                OutputStruct output = default;
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                Status foundStatus = session.Read(ref key1, ref input, ref output, Empty.Default, 0);
                Assert.IsTrue(foundStatus.IsPending);
                if (batchMode == TestUtils.BatchMode.NoBatch)
                {
                    Status status;
                    session.CompletePendingWithOutputs(out var outputs, wait: true);
                    (status, output) = TestUtils.GetSinglePendingResult(outputs);
                    Assert.IsTrue(status.Found, status.ToString());
                    Assert.AreEqual(key1.kfield1, output.value.vfield1);
                    Assert.AreEqual(key1.kfield2, output.value.vfield2);
                    outputs.Dispose();
                }
                else if (c > 0 && (c % batchSize) == 0)
                {
                    session.CompletePendingWithOutputs(out var outputs, wait: true);
                    int count = 0;
                    while (outputs.Next())
                    {
                        count++;
                        Assert.AreEqual(outputs.Current.Key.kfield1, outputs.Current.Output.value.vfield1);
                        Assert.AreEqual(outputs.Current.Key.kfield2, outputs.Current.Output.value.vfield2);
                    }
                    outputs.Dispose();
                    Assert.AreEqual(batchSize + (c == batchSize ? 1 : 0), count);
                }
            }
        }

        [Test]
        [Category("FasterKV")]
        [Category("Smoke")]
        public unsafe void NativeInMemRMWRefKeys([Values] TestUtils.DeviceType deviceType)
        {
            InputStruct input = default;
            OutputStruct output = default;

            Setup(128, new LogSettings { MemorySizeBits = 22, SegmentSizeBits = 22, PageSizeBits = 10 }, deviceType);

            var nums = Enumerable.Range(0, 1000).ToArray();
            var rnd = new Random(11);
            for (int i = 0; i < nums.Length; ++i)
            {
                int randomIndex = rnd.Next(nums.Length);
                int temp = nums[randomIndex];
                nums[randomIndex] = nums[i];
                nums[i] = temp;
            }

            for (int j = 0; j < nums.Length; ++j)
            {
                var i = nums[j];
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                input = new InputStruct { ifield1 = i, ifield2 = i + 1 };
                session.RMW(ref key1, ref input, Empty.Default, 0);
            }
            for (int j = 0; j < nums.Length; ++j)
            {
                var i = nums[j];
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                input = new InputStruct { ifield1 = i, ifield2 = i + 1 };
                if (session.RMW(ref key1, ref input, ref output, Empty.Default, 0).IsPending)
                {
                    session.CompletePending(true);
                }
                else
                {
                    Assert.AreEqual(2 * i, output.value.vfield1);
                    Assert.AreEqual(2 * (i + 1), output.value.vfield2);
                }
            }

            Status status;
            KeyStruct key;

            for (int j = 0; j < nums.Length; ++j)
            {
                var i = nums[j];

                key = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                ValueStruct value = new() { vfield1 = i, vfield2 = i + 1 };

                status = session.Read(ref key, ref input, ref output, Empty.Default, 0);

                AssertCompleted(new(StatusCode.Found), status);
                Assert.AreEqual(2 * value.vfield1, output.value.vfield1);
                Assert.AreEqual(2 * value.vfield2, output.value.vfield2);
            }

            key = new KeyStruct { kfield1 = nums.Length, kfield2 = nums.Length + 1 };
            status = session.Read(ref key, ref input, ref output, Empty.Default, 0);
            AssertCompleted(new(StatusCode.NotFound), status);
        }

        // Tests the overload where no reference params used: key,input,userContext,serialNo
        [Test]
        [Category("FasterKV")]
        public unsafe void NativeInMemRMWNoRefKeys([Values] TestUtils.DeviceType deviceType)
        {
            InputStruct input = default;

            Setup(128, new LogSettings { MemorySizeBits = 22, SegmentSizeBits = 22, PageSizeBits = 10 }, deviceType);

            var nums = Enumerable.Range(0, 1000).ToArray();
            var rnd = new Random(11);
            for (int i = 0; i < nums.Length; ++i)
            {
                int randomIndex = rnd.Next(nums.Length);
                int temp = nums[randomIndex];
                nums[randomIndex] = nums[i];
                nums[i] = temp;
            }

            // InitialUpdater
            for (int j = 0; j < nums.Length; ++j)
            {
                var i = nums[j];
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                input = new InputStruct { ifield1 = i, ifield2 = i + 1 };
                session.RMW(ref key1, ref input, Empty.Default, 0);
            }

            // CopyUpdater
            for (int j = 0; j < nums.Length; ++j)
            {
                var i = nums[j];
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                input = new InputStruct { ifield1 = i, ifield2 = i + 1 };
                session.RMW(key1, input);  // no ref and do not set any other params
            }

            OutputStruct output = default;
            Status status;
            KeyStruct key;

            for (int j = 0; j < nums.Length; ++j)
            {
                var i = nums[j];

                key = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                ValueStruct value = new() { vfield1 = i, vfield2 = i + 1 };

                status = session.Read(ref key, ref input, ref output, Empty.Default, 0);

                AssertCompleted(new(StatusCode.Found), status);
                Assert.AreEqual(2 * value.vfield1, output.value.vfield1);
                Assert.AreEqual(2 * value.vfield2, output.value.vfield2);
            }

            key = new KeyStruct { kfield1 = nums.Length, kfield2 = nums.Length + 1 };
            status = session.Read(ref key, ref input, ref output, Empty.Default, 0);
            AssertCompleted(new(StatusCode.NotFound), status);
        }

        // Tests the overload of .Read(key, input, out output,  context, serialNo)
        [Test]
        [Category("FasterKV")]
        [Category("Smoke")]
        public void ReadNoRefKeyInputOutput([Values] TestUtils.DeviceType deviceType)
        {
            InputStruct input = default;

            Setup(128, new LogSettings { MemorySizeBits = 22, SegmentSizeBits = 22, PageSizeBits = 10 }, deviceType);

            var key1 = new KeyStruct { kfield1 = 13, kfield2 = 14 };
            var value = new ValueStruct { vfield1 = 23, vfield2 = 24 };

            session.Upsert(ref key1, ref value, Empty.Default, 0);
            var status = session.Read(key1, input, out OutputStruct output, Empty.Default, 111);
            AssertCompleted(new(StatusCode.Found), status);

            // Verify the read data
            Assert.AreEqual(value.vfield1, output.value.vfield1);
            Assert.AreEqual(value.vfield2, output.value.vfield2);
            Assert.AreEqual(key1.kfield1, 13);
            Assert.AreEqual(key1.kfield2, 14);
        }

        // Test the overload call of .Read (key, out output, userContext, serialNo)
        [Test]
        [Category("FasterKV")]
        public void ReadNoRefKey([Values] TestUtils.DeviceType deviceType)
        {
            Setup(128, new LogSettings { MemorySizeBits = 22, SegmentSizeBits = 22, PageSizeBits = 10 }, deviceType);

            var key1 = new KeyStruct { kfield1 = 13, kfield2 = 14 };
            var value = new ValueStruct { vfield1 = 23, vfield2 = 24 };

            session.Upsert(ref key1, ref value, Empty.Default, 0);
            var status = session.Read(key1, out OutputStruct output, Empty.Default, 1);
            AssertCompleted(new(StatusCode.Found), status);

            // Verify the read data
            Assert.AreEqual(value.vfield1, output.value.vfield1);
            Assert.AreEqual(value.vfield2, output.value.vfield2);
            Assert.AreEqual(key1.kfield1, 13);
            Assert.AreEqual(key1.kfield2, 14);
        }


        // Test the overload call of .Read (ref key, ref output, userContext, serialNo)
        [Test]
        [Category("FasterKV")]
        [Category("Smoke")]
        public void ReadWithoutInput([Values] TestUtils.DeviceType deviceType)
        {
            Setup(128, new LogSettings { MemorySizeBits = 22, SegmentSizeBits = 22, PageSizeBits = 10 }, deviceType);

            OutputStruct output = default;

            var key1 = new KeyStruct { kfield1 = 13, kfield2 = 14 };
            var value = new ValueStruct { vfield1 = 23, vfield2 = 24 };

            session.Upsert(ref key1, ref value, Empty.Default, 0);
            var status = session.Read(ref key1, ref output, Empty.Default, 99);
            AssertCompleted(new(StatusCode.Found), status);

            // Verify the read data
            Assert.AreEqual(value.vfield1, output.value.vfield1);
            Assert.AreEqual(value.vfield2, output.value.vfield2);
            Assert.AreEqual(key1.kfield1, 13);
            Assert.AreEqual(key1.kfield2, 14);
        }


        // Test the overload call of .Read (ref key, ref input, ref output, ref recordInfo, userContext: context)
        [Test]
        [Category("FasterKV")]
        [Category("Smoke")]
        public void ReadWithoutSerialID()
        {
            // Just checking without Serial ID so one device type is enough
            deviceType = TestUtils.DeviceType.MLSD;

            Setup(128, new LogSettings { MemorySizeBits = 29 }, deviceType);

            InputStruct input = default;
            OutputStruct output = default;

            var key1 = new KeyStruct { kfield1 = 13, kfield2 = 14 };
            var value = new ValueStruct { vfield1 = 23, vfield2 = 24 };

            session.Upsert(ref key1, ref value, Empty.Default, 0);
            var status = session.Read(ref key1, ref input, ref output, Empty.Default);
            AssertCompleted(new(StatusCode.Found), status);

            Assert.AreEqual(value.vfield1, output.value.vfield1);
            Assert.AreEqual(value.vfield2, output.value.vfield2);
            Assert.AreEqual(key1.kfield1, 13);
            Assert.AreEqual(key1.kfield2, 14);
        }

        // Test the overload call of .Read (key)
        [Test]
        [Category("FasterKV")]
        [Category("Smoke")]
        public void ReadBareMinParams([Values] TestUtils.DeviceType deviceType)
        {
            Setup(128, new LogSettings { MemorySizeBits = 22, SegmentSizeBits = 22, PageSizeBits = 10 }, deviceType);

            var key1 = new KeyStruct { kfield1 = 13, kfield2 = 14 };
            var value = new ValueStruct { vfield1 = 23, vfield2 = 24 };

            session.Upsert(ref key1, ref value, Empty.Default, 0);

            var (status, output) = session.Read(key1);
            AssertCompleted(new(StatusCode.Found), status);

            Assert.AreEqual(value.vfield1, output.value.vfield1);
            Assert.AreEqual(value.vfield2, output.value.vfield2);
            Assert.AreEqual(key1.kfield1, 13);
            Assert.AreEqual(key1.kfield2, 14);
        }

        // Test the ReadAtAddress where ReadFlags = ReadFlags.none
        [Test]
        [Category("FasterKV")]
        [Category("Smoke")]
        public void ReadAtAddressReadFlagsNone()
        {
            // Just functional test of ReadFlag so one device is enough
            deviceType = TestUtils.DeviceType.MLSD;

            Setup(128, new LogSettings { MemorySizeBits = 29 }, deviceType);

            InputStruct input = default;
            OutputStruct output = default;

            var key1 = new KeyStruct { kfield1 = 13, kfield2 = 14 };
            var value = new ValueStruct { vfield1 = 23, vfield2 = 24 };
            ReadOptions readOptions = new() { StartAddress = fht.Log.BeginAddress };

            session.Upsert(ref key1, ref value, Empty.Default, 0);
            var status = session.ReadAtAddress(ref input, ref output, ref readOptions, Empty.Default, 0);
            AssertCompleted(new(StatusCode.Found), status);

            Assert.AreEqual(value.vfield1, output.value.vfield1);
            Assert.AreEqual(value.vfield2, output.value.vfield2);
            Assert.AreEqual(key1.kfield1, 13);
            Assert.AreEqual(key1.kfield2, 14);
        }

        // Test the ReadAtAddress where ReadFlags = ReadFlags.SkipReadCache

        class SkipReadCacheFunctions : Functions
        {
            internal long expectedReadAddress;

            public override bool SingleReader(ref KeyStruct key, ref InputStruct input, ref ValueStruct value, ref OutputStruct dst, ref ReadInfo readInfo)
            {
                Assign(ref value, ref dst, ref readInfo);
                return true;
            }

            public override bool ConcurrentReader(ref KeyStruct key, ref InputStruct input, ref ValueStruct value, ref OutputStruct dst, ref ReadInfo readInfo)
            {
                Assign(ref value, ref dst, ref readInfo);
                return true;
            }

            void Assign(ref ValueStruct value, ref OutputStruct dst, ref ReadInfo readInfo)
            {
                dst.value = value;
                Assert.AreEqual(expectedReadAddress, readInfo.Address);
                expectedReadAddress = -1;   // show that the test executed
            }
            public override void ReadCompletionCallback(ref KeyStruct key, ref InputStruct input, ref OutputStruct output, Empty ctx, Status status, RecordMetadata recordMetadata)
            {
                // Do no data verifications here; they're done in the test
            }
        }

        [Test]
        [Category("FasterKV")]
        [Category("Smoke")]
        public void ReadAtAddressReadFlagsSkipReadCache()
        {
            // Another ReadFlag functional test so one device is enough
            deviceType = TestUtils.DeviceType.MLSD;

            Setup(128, new LogSettings { MemorySizeBits = 29, ReadCacheSettings = new ReadCacheSettings() }, deviceType);

            SkipReadCacheFunctions functions = new();
            using var skipReadCacheSession = fht.For(functions).NewSession<SkipReadCacheFunctions>();

            InputStruct input = default;
            OutputStruct output = default;
            var key1 = new KeyStruct { kfield1 = 13, kfield2 = 14 };
            var value = new ValueStruct { vfield1 = 23, vfield2 = 24 };
            var readAtAddress = fht.Log.BeginAddress;
            Status status;

            skipReadCacheSession.Upsert(ref key1, ref value, Empty.Default, 0);

            void VerifyOutput()
            {
                Assert.AreEqual(-1, functions.expectedReadAddress);     // make sure the test executed
                Assert.AreEqual(value.vfield1, output.value.vfield1);
                Assert.AreEqual(value.vfield2, output.value.vfield2);
                Assert.AreEqual(13, key1.kfield1);
                Assert.AreEqual(14, key1.kfield2);
            }

            void VerifyResult()
            {
                if (status.IsPending)
                {
                    skipReadCacheSession.CompletePendingWithOutputs(out var completedOutputs, wait: true);
                    (status, output) = TestUtils.GetSinglePendingResult(completedOutputs);
                }
                Assert.IsTrue(status.Found);
                VerifyOutput();
            }

            // This will just be an ordinary read, as the record is in memory.
            functions.expectedReadAddress = readAtAddress;
            status = skipReadCacheSession.Read(ref key1, ref input, ref output);
            Assert.IsTrue(status.Found);
            VerifyOutput();

            // ReadCache is used when the record is read from disk.
            fht.Log.FlushAndEvict(wait:true);

            // DisableReadCacheUpdates is primarily for indexing, so a read during index scan does not result in a readcache update.
            // Reading at a normal logical address will not use the readcache, because the "readcache" bit is not set in that logical address.
            // And we cannot get a readcache address, since reads satisfied from the readcache pass kInvalidAddress to functions.
            // Therefore, we test here simply that we do not put it in the readcache when we tell it not to.

            // Do not put it into the read cache.
            functions.expectedReadAddress = readAtAddress;
            ReadOptions readOptions = new() { StartAddress = readAtAddress, ReadFlags = ReadFlags.DisableReadCacheUpdates };
            status = skipReadCacheSession.Read(ref key1, ref input, ref output, ref readOptions, out _);
            VerifyResult();

            Assert.AreEqual(fht.ReadCache.BeginAddress, fht.ReadCache.TailAddress);

            // Put it into the read cache.
            functions.expectedReadAddress = readAtAddress;
            readOptions.ReadFlags = ReadFlags.None;
            status = skipReadCacheSession.Read(ref key1, ref input, ref output, ref readOptions, out _);
            Assert.IsTrue(status.IsPending);
            VerifyResult();

            Assert.Less(fht.ReadCache.BeginAddress, fht.ReadCache.TailAddress);

            // Now this will read from the read cache.
            functions.expectedReadAddress = Constants.kInvalidAddress;
            status = skipReadCacheSession.Read(ref key1, ref input, ref output);
            Assert.IsFalse(status.IsPending);
            Assert.IsTrue(status.Found);
            VerifyOutput();
        }

        // Simple Upsert test where ref key and ref value but nothing else set
        [Test]
        [Category("FasterKV")]
        [Category("Smoke")]
        public void UpsertDefaultsTest([Values] TestUtils.DeviceType deviceType)
        {
            Setup(128, new LogSettings { MemorySizeBits = 22, SegmentSizeBits = 22, PageSizeBits = 10 }, deviceType);

            InputStruct input = default;
            OutputStruct output = default;

            var key1 = new KeyStruct { kfield1 = 13, kfield2 = 14 };
            var value = new ValueStruct { vfield1 = 23, vfield2 = 24 };

            Assert.AreEqual(0, fht.EntryCount);

            session.Upsert(ref key1, ref value);
            var status = session.Read(ref key1, ref input, ref output, Empty.Default, 0);
            AssertCompleted(new(StatusCode.Found), status);

            Assert.AreEqual(1, fht.EntryCount);
            Assert.AreEqual(value.vfield1, output.value.vfield1);
            Assert.AreEqual(value.vfield2, output.value.vfield2);
        }

        // Simple Upsert test of overload where not using Ref for key and value and setting all parameters
        [Test]
        [Category("FasterKV")]
        [Category("Smoke")]
        public void UpsertNoRefNoDefaultsTest()
        {
            // Just checking more parameter values so one device is enough
            deviceType = TestUtils.DeviceType.MLSD;

            Setup(128, new LogSettings { MemorySizeBits = 29 }, deviceType);

            InputStruct input = default;
            OutputStruct output = default;

            var key1 = new KeyStruct { kfield1 = 13, kfield2 = 14 };
            var value = new ValueStruct { vfield1 = 23, vfield2 = 24 };

            session.Upsert(key1, value, Empty.Default, 0);
            var status = session.Read(ref key1, ref input, ref output, Empty.Default, 0);
            AssertCompleted(new(StatusCode.Found), status);

            Assert.AreEqual(value.vfield1, output.value.vfield1);
            Assert.AreEqual(value.vfield2, output.value.vfield2);
        }


        // Upsert Test using Serial Numbers ... based on the VersionedRead Sample
        [Test]
        [Category("FasterKV")]
        [Category("Smoke")]
        public void UpsertSerialNumberTest()
        {
            // Simple Upsert of Serial Number test so one device is enough
            deviceType = TestUtils.DeviceType.MLSD;

            Setup(128, new LogSettings { MemorySizeBits = 29 }, deviceType);

            int numKeys = 100;
            int keyMod = 10;
            int maxLap = numKeys / keyMod;
            InputStruct input = default;
            OutputStruct output = default;

            var value = new ValueStruct { vfield1 = 23, vfield2 = 24 };
            var key = new KeyStruct { kfield1 = 13, kfield2 = 14 };

            for (int i = 0; i < numKeys; i++)
            {
                // lap is used to illustrate the changing values
                var lap = i / keyMod;
                session.Upsert(ref key, ref value, serialNo: lap);
            }

            // Now verify 
            for (int j = 0; j < numKeys; j++)
            {
                var status = session.Read(ref key, ref input, ref output, serialNo: maxLap + 1);

                AssertCompleted(new(StatusCode.Found), status);
                Assert.AreEqual(value.vfield1, output.value.vfield1);
                Assert.AreEqual(value.vfield2, output.value.vfield2);
            }
        }

        //**** Quick End to End Sample code from help docs: https://microsoft.github.io/FASTER/docs/fasterkv-basics/  ***
        // Very minor changes to LogDevice call and type of Asserts to use but basically code from Sample code in docs
        // Also tests the overload call of .Read (ref key ref output) 
        [Test]
        [Category("FasterKV")]
        public static void KVBasicsSampleEndToEndInDocs()
        {
            string testDir = TestUtils.MethodTestDir;
            using var log = Devices.CreateLogDevice($"{testDir}/hlog.log", deleteOnClose: false);
            using var store = new FasterKV<long, long>(1L << 20, new LogSettings { LogDevice = log });
            using var s = store.NewSession(new SimpleFunctions<long, long>());
            long key = 1, value = 1, input = 10, output = 0;
            s.Upsert(ref key, ref value);
            s.Read(ref key, ref output);
            Assert.AreEqual(value, output);
            s.RMW(ref key, ref input);
            s.RMW(ref key, ref input);
            s.Read(ref key, ref output);
            Assert.AreEqual(10, output);
        }

        [Test]
        [Category("FasterKV")]
        public static void LogPathtooLong()
        {
            string testDir = new string('x', Native32.WIN32_MAX_PATH - 11);                 // As in LSD, -11 for ".<segment>"
            using var log = Devices.CreateLogDevice($"{testDir}", deleteOnClose: true);     // Should succeed
            Assert.Throws(typeof(FasterException), () => Devices.CreateLogDevice($"{testDir}y", deleteOnClose: true));
        }

        [Test]
        [Category("FasterKV")]
        public static void UshortKeyByteValueTest()
        {
            using var log = Devices.CreateLogDevice($"{TestUtils.MethodTestDir}/hlog.log", deleteOnClose: false);
            using var store = new FasterKV<ushort, byte>(1L << 20, new LogSettings { LogDevice = log });
            using var s = store.NewSession(new SimpleFunctions<ushort, byte>());
            ushort key = 1024;
            byte value = 1, input = 10, output = 0;

            // For blittable types, the records are not 8-byte aligned; RecordSize is sizeof(RecordInfo) + sizeof(ushort) + sizeof(byte)
            const int expectedRecordSize = sizeof(long) + sizeof(ushort) + sizeof(byte);
            Assert.AreEqual(11, expectedRecordSize);
            long prevTailLogicalAddress = store.hlog.GetTailAddress();
            long prevTailPhysicalAddress = store.hlog.GetPhysicalAddress(prevTailLogicalAddress);
            for (var ii = 0; ii < 5; ++ii, ++key, ++value, ++input)
            {
                output = 0;
                s.Upsert(ref key, ref value);
                s.Read(ref key, ref output);
                Assert.AreEqual(value, output);
                s.RMW(ref key, ref input);
                s.Read(ref key, ref output);
                Assert.AreEqual(input, output);

                var tailLogicalAddress = store.hlog.GetTailAddress();
                Assert.AreEqual(expectedRecordSize, tailLogicalAddress - prevTailLogicalAddress);
                long tailPhysicalAddress = store.hlog.GetPhysicalAddress(tailLogicalAddress);
                Assert.AreEqual(expectedRecordSize, tailPhysicalAddress - prevTailPhysicalAddress);

                prevTailLogicalAddress = tailLogicalAddress;
                prevTailPhysicalAddress = tailPhysicalAddress;
            }
        }
    }
}