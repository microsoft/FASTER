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
            if (actual == Status.PENDING)
                (actual, _) = CompletePendingResult();
            Assert.AreEqual(expected, actual);
        }

        private (Status status, OutputStruct output) CompletePendingResult()
        {
            session.CompletePendingWithOutputs(out var completedOutputs);
            return TestUtils.GetSinglePendingResult(completedOutputs);
        }

        private static (Status status, OutputStruct output) CompletePendingResult(CompletedOutputIterator<KeyStruct, ValueStruct, InputStruct, OutputStruct, Empty> completedOutputs)
        {
            Assert.IsTrue(completedOutputs.Next());
            var result = (completedOutputs.Current.Status, completedOutputs.Current.Output);
            Assert.IsFalse(completedOutputs.Next());
            completedOutputs.Dispose();
            return result;
        }

        [Test]
        [Category("FasterKV")]
        [Category("Smoke")]
        public void NativeInMemWriteRead([Values] TestUtils.DeviceType deviceType)
        {
            string filename = path + "NativeInMemWriteRead" + deviceType.ToString() + ".log";
            log = TestUtils.CreateTestDevice(deviceType, filename);
            fht = new FasterKV<KeyStruct, ValueStruct>
                (128, new LogSettings { LogDevice = log, PageSizeBits = 10, MemorySizeBits = 12, SegmentSizeBits = 22 });
            session = fht.For(new Functions()).NewSession<Functions>();

            InputStruct input = default;
            OutputStruct output = default;

            var key1 = new KeyStruct { kfield1 = 13, kfield2 = 14 };
            var value = new ValueStruct { vfield1 = 23, vfield2 = 24 };

            session.Upsert(ref key1, ref value, Empty.Default, 0);
            var status = session.Read(ref key1, ref input, ref output, Empty.Default, 0);

            AssertCompleted(Status.OK, status);
            Assert.AreEqual(value.vfield1, output.value.vfield1);
            Assert.AreEqual(value.vfield2, output.value.vfield2);
        }

        [Test]
        [Category("FasterKV")]
        [Category("Smoke")]
        public void NativeInMemWriteReadDelete([Values] TestUtils.DeviceType deviceType)
        {
            string filename = path + "NativeInMemWriteReadDelete" + deviceType.ToString() + ".log";
            log = TestUtils.CreateTestDevice(deviceType, filename);
            fht = new FasterKV<KeyStruct, ValueStruct>
                (128, new LogSettings { LogDevice = log, PageSizeBits = 10, MemorySizeBits = 12, SegmentSizeBits = 22 });
            session = fht.For(new Functions()).NewSession<Functions>();

            InputStruct input = default;
            OutputStruct output = default;

            var key1 = new KeyStruct { kfield1 = 13, kfield2 = 14 };
            var value = new ValueStruct { vfield1 = 23, vfield2 = 24 };

            session.Upsert(ref key1, ref value, Empty.Default, 0);
            var status = session.Read(ref key1, ref input, ref output, Empty.Default, 0);
            AssertCompleted(Status.OK, status);

            session.Delete(ref key1, Empty.Default, 0);

            status = session.Read(ref key1, ref input, ref output, Empty.Default, 0);
            AssertCompleted(Status.NOTFOUND, status);

            var key2 = new KeyStruct { kfield1 = 14, kfield2 = 15 };
            var value2 = new ValueStruct { vfield1 = 24, vfield2 = 25 };

            session.Upsert(ref key2, ref value2, Empty.Default, 0);
            status = session.Read(ref key2, ref input, ref output, Empty.Default, 0);

            AssertCompleted(Status.OK, status);
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

            string filename = path + "NativeInMemWriteReadDelete2" + deviceType.ToString() + ".log";
            log = TestUtils.CreateTestDevice(deviceType, filename);
            fht = new FasterKV<KeyStruct, ValueStruct>
                //                (128, new LogSettings { LogDevice = log, MemorySizeBits = 22, SegmentSizeBits = 22, PageSizeBits = 10 });
                (128, new LogSettings { LogDevice = log, MemorySizeBits = 29 });
            session = fht.For(new Functions()).NewSession<Functions>();

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
                AssertCompleted(Status.NOTFOUND, status);

                session.Upsert(ref key1, ref value, Empty.Default, 0);
            }

            for (int i = 0; i < 10 * count; i++)
            {
                var key1 = new KeyStruct { kfield1 = i, kfield2 = 14 };
                var status = session.Read(ref key1, ref input, ref output, Empty.Default, 0);
                AssertCompleted(Status.OK, status);
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

            string filename = path + "NativeInMemWriteRead2" + deviceType.ToString() + ".log";
            log = TestUtils.CreateTestDevice(deviceType, filename);
            fht = new FasterKV<KeyStruct, ValueStruct>
                //                (128, new LogSettings { LogDevice = log, MemorySizeBits = 22, SegmentSizeBits = 22, PageSizeBits = 10 });
                (128, new LogSettings { LogDevice = log, MemorySizeBits = 29 });
            session = fht.For(new Functions()).NewSession<Functions>();

            InputStruct input = default;

            Random r = new Random(10);
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

                if (session.Read(ref key1, ref input, ref output, Empty.Default, 0) == Status.PENDING)
                {
                    session.CompletePending(true);
                }

                Assert.AreEqual(value.vfield1, output.value.vfield1);
                Assert.AreEqual(value.vfield2, output.value.vfield2);
            }

            // Clean up and retry - should not find now
            fht.Log.ShiftBeginAddress(fht.Log.TailAddress);

            r = new Random(10);
            for (int c = 0; c < count; c++)
            {
                var i = r.Next(10000);
                OutputStruct output = default;
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                Assert.AreEqual(Status.NOTFOUND, session.Read(ref key1, ref input, ref output, Empty.Default, 0));
            }
        }

        [Test]
        [Category("FasterKV")]
        [Category("Smoke")]
        public unsafe void TestShiftHeadAddress([Values] TestUtils.DeviceType deviceType)
        {
            InputStruct input = default;
            const int RandSeed = 10;
            const int RandRange = 10000;
            const int NumRecs = 200;

            Random r = new Random(RandSeed);
            var sw = Stopwatch.StartNew();

            string filename = path + "TestShiftHeadAddress" + deviceType.ToString() + ".log";
            log = TestUtils.CreateTestDevice(deviceType, filename);
            fht = new FasterKV<KeyStruct, ValueStruct>
                (128, new LogSettings { LogDevice = log, MemorySizeBits = 22, SegmentSizeBits = 22, PageSizeBits = 10 });
            session = fht.For(new Functions()).NewSession<Functions>();


            for (int c = 0; c < NumRecs; c++)
            {
                var i = r.Next(RandRange);
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };
                session.Upsert(ref key1, ref value, Empty.Default, 0);
            }
            Console.WriteLine($"Time to insert {NumRecs} records: {sw.ElapsedMilliseconds} ms");

            r = new Random(RandSeed);
            sw.Restart();

            for (int c = 0; c < NumRecs; c++)
            {
                var i = r.Next(RandRange);
                OutputStruct output = default;
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };

                if (session.Read(ref key1, ref input, ref output, Empty.Default, 0) == Status.PENDING)
                {
                    session.CompletePending(true);
                }

                Assert.AreEqual(value.vfield1, output.value.vfield1);
                Assert.AreEqual(value.vfield2, output.value.vfield2);
            }
            Console.WriteLine($"Time to read {NumRecs} in-memory records: {sw.ElapsedMilliseconds} ms");

            // Shift head and retry - should not find in main memory now
            fht.Log.FlushAndEvict(true);

            r = new Random(RandSeed);
            sw.Restart();

            for (int c = 0; c < NumRecs; c++)
            {
                var i = r.Next(RandRange);
                OutputStruct output = default;
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };

                Status foundStatus = session.Read(ref key1, ref input, ref output, Empty.Default, 0);
                Assert.AreEqual(Status.PENDING, foundStatus);
                session.CompletePendingWithOutputs(out var outputs, wait: true);
                Assert.IsTrue(outputs.Next());
                Assert.AreEqual(value.vfield1, outputs.Current.Output.value.vfield1);
                outputs.Current.Dispose();
                Assert.IsFalse(outputs.Next());
            }
            Console.WriteLine($"Time to read {NumRecs} on-disk records: {sw.ElapsedMilliseconds} ms");
        }

        [Test]
        [Category("FasterKV")]
        [Category("Smoke")]
        public unsafe void NativeInMemRMWRefKeys([Values] TestUtils.DeviceType deviceType)
        {
            InputStruct input = default;
            OutputStruct output = default;

            string filename = path + "NativeInMemRMWRefKeys" + deviceType.ToString() + ".log";
            log = TestUtils.CreateTestDevice(deviceType, filename);
            fht = new FasterKV<KeyStruct, ValueStruct>
                (128, new LogSettings { LogDevice = log, MemorySizeBits = 22, SegmentSizeBits = 22, PageSizeBits = 10 });
            session = fht.For(new Functions()).NewSession<Functions>();

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
                if (session.RMW(ref key1, ref input, ref output, Empty.Default, 0) == Status.PENDING)
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
                ValueStruct value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };

                status = session.Read(ref key, ref input, ref output, Empty.Default, 0);

                AssertCompleted(Status.OK, status);
                Assert.AreEqual(2 * value.vfield1, output.value.vfield1);
                Assert.AreEqual(2 * value.vfield2, output.value.vfield2);
            }

            key = new KeyStruct { kfield1 = nums.Length, kfield2 = nums.Length + 1 };
            status = session.Read(ref key, ref input, ref output, Empty.Default, 0);
            AssertCompleted(Status.NOTFOUND, status);
        }

        // Tests the overload where no reference params used: key,input,userContext,serialNo
        [Test]
        [Category("FasterKV")]
        public unsafe void NativeInMemRMWNoRefKeys([Values] TestUtils.DeviceType deviceType)
        {
            InputStruct input = default;

            string filename = path + "NativeInMemRMWNoRefKeys" + deviceType.ToString() + ".log";
            log = TestUtils.CreateTestDevice(deviceType, filename);
            fht = new FasterKV<KeyStruct, ValueStruct>
                (128, new LogSettings { LogDevice = log, MemorySizeBits = 22, SegmentSizeBits = 22, PageSizeBits = 10 });
            session = fht.For(new Functions()).NewSession<Functions>();

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
                session.RMW(key1, input);  // no ref and do not set any other params
            }

            OutputStruct output = default;
            Status status;
            KeyStruct key;

            for (int j = 0; j < nums.Length; ++j)
            {
                var i = nums[j];

                key = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                ValueStruct value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };

                status = session.Read(ref key, ref input, ref output, Empty.Default, 0);

                AssertCompleted(Status.OK, status);
                Assert.AreEqual(2 * value.vfield1, output.value.vfield1);
                Assert.AreEqual(2 * value.vfield2, output.value.vfield2);
            }

            key = new KeyStruct { kfield1 = nums.Length, kfield2 = nums.Length + 1 };
            status = session.Read(ref key, ref input, ref output, Empty.Default, 0);
            AssertCompleted(Status.NOTFOUND, status);
        }

        // Tests the overload of .Read(key, input, out output,  context, serialNo)
        [Test]
        [Category("FasterKV")]
        [Category("Smoke")]
        public void ReadNoRefKeyInputOutput([Values] TestUtils.DeviceType deviceType)
        {
            InputStruct input = default;

            string filename = path + "ReadNoRefKeyInputOutput" + deviceType.ToString() + ".log";
            log = TestUtils.CreateTestDevice(deviceType, filename);
            fht = new FasterKV<KeyStruct, ValueStruct>
                (128, new LogSettings { LogDevice = log, MemorySizeBits = 22, SegmentSizeBits = 22, PageSizeBits = 10 });
            session = fht.For(new Functions()).NewSession<Functions>();

            var key1 = new KeyStruct { kfield1 = 13, kfield2 = 14 };
            var value = new ValueStruct { vfield1 = 23, vfield2 = 24 };

            session.Upsert(ref key1, ref value, Empty.Default, 0);
            var status = session.Read(key1, input, out OutputStruct output, Empty.Default, 111);
            AssertCompleted(Status.OK, status);

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
            string filename = path + "ReadNoRefKey" + deviceType.ToString() + ".log";
            log = TestUtils.CreateTestDevice(deviceType, filename);
            fht = new FasterKV<KeyStruct, ValueStruct>
                (128, new LogSettings { LogDevice = log, MemorySizeBits = 22, SegmentSizeBits = 22, PageSizeBits = 10 });
            session = fht.For(new Functions()).NewSession<Functions>();

            var key1 = new KeyStruct { kfield1 = 13, kfield2 = 14 };
            var value = new ValueStruct { vfield1 = 23, vfield2 = 24 };

            session.Upsert(ref key1, ref value, Empty.Default, 0);
            var status = session.Read(key1, out OutputStruct output, Empty.Default, 1);
            AssertCompleted(Status.OK, status);

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
            string filename = path + "ReadWithoutInput" + deviceType.ToString() + ".log";
            log = TestUtils.CreateTestDevice(deviceType, filename);
            fht = new FasterKV<KeyStruct, ValueStruct>
                (128, new LogSettings { LogDevice = log, MemorySizeBits = 22, SegmentSizeBits = 22, PageSizeBits = 10 });
            session = fht.For(new Functions()).NewSession<Functions>();

            OutputStruct output = default;

            var key1 = new KeyStruct { kfield1 = 13, kfield2 = 14 };
            var value = new ValueStruct { vfield1 = 23, vfield2 = 24 };

            session.Upsert(ref key1, ref value, Empty.Default, 0);
            var status = session.Read(ref key1, ref output, Empty.Default, 99);
            AssertCompleted(Status.OK, status);

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

            string filename = path + "ReadWithoutSerialID" + deviceType.ToString() + ".log";
            log = TestUtils.CreateTestDevice(deviceType, filename);
            fht = new FasterKV<KeyStruct, ValueStruct>
                (128, new LogSettings { LogDevice = log, MemorySizeBits = 29 });
            session = fht.For(new Functions()).NewSession<Functions>();

            InputStruct input = default;
            OutputStruct output = default;

            var key1 = new KeyStruct { kfield1 = 13, kfield2 = 14 };
            var value = new ValueStruct { vfield1 = 23, vfield2 = 24 };

            session.Upsert(ref key1, ref value, Empty.Default, 0);
            var status = session.Read(ref key1, ref input, ref output, Empty.Default);
            AssertCompleted(Status.OK, status);

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
            string filename = path + "ReadBareMinParams" + deviceType.ToString() + ".log";
            log = TestUtils.CreateTestDevice(deviceType, filename);
            fht = new FasterKV<KeyStruct, ValueStruct>
                (128, new LogSettings { LogDevice = log, MemorySizeBits = 22, SegmentSizeBits = 22, PageSizeBits = 10 });
            session = fht.For(new Functions()).NewSession<Functions>();

            var key1 = new KeyStruct { kfield1 = 13, kfield2 = 14 };
            var value = new ValueStruct { vfield1 = 23, vfield2 = 24 };

            session.Upsert(ref key1, ref value, Empty.Default, 0);

            var (status, output) = session.Read(key1);
            AssertCompleted(Status.OK, status);

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

            string filename = path + "ReadAtAddressReadFlagsNone" + deviceType.ToString() + ".log";
            log = TestUtils.CreateTestDevice(deviceType, filename);
            fht = new FasterKV<KeyStruct, ValueStruct>
                (128, new LogSettings { LogDevice = log, MemorySizeBits = 29 });
            session = fht.For(new Functions()).NewSession<Functions>();

            InputStruct input = default;
            OutputStruct output = default;

            var key1 = new KeyStruct { kfield1 = 13, kfield2 = 14 };
            var value = new ValueStruct { vfield1 = 23, vfield2 = 24 };
            var readAtAddress = fht.Log.BeginAddress;

            session.Upsert(ref key1, ref value, Empty.Default, 0);
            var status = session.ReadAtAddress(readAtAddress, ref input, ref output, ReadFlags.None, Empty.Default, 0);
            AssertCompleted(Status.OK, status);

            Assert.AreEqual(value.vfield1, output.value.vfield1);
            Assert.AreEqual(value.vfield2, output.value.vfield2);
            Assert.AreEqual(key1.kfield1, 13);
            Assert.AreEqual(key1.kfield2, 14);
        }

        // Test the ReadAtAddress where ReadFlags = ReadFlags.SkipReadCache

        class SkipReadCacheFunctions : AdvancedFunctions    // Must use AdvancedFunctions for the address parameters to the callbacks
        {
            internal long expectedReadAddress;

            public override bool SingleReader(ref KeyStruct key, ref InputStruct input, ref ValueStruct value, ref OutputStruct dst, long address)
            {
                Assign(ref value, ref dst, address);
                return true;
            }

            public override bool ConcurrentReader(ref KeyStruct key, ref InputStruct input, ref ValueStruct value, ref OutputStruct dst, ref RecordInfo recordInfo, long address)
            {
                Assign(ref value, ref dst, address);
                return true;
            }

            void Assign(ref ValueStruct value, ref OutputStruct dst, long address)
            {
                dst.value = value;
                Assert.AreEqual(expectedReadAddress, address);
                expectedReadAddress = -1;   // show that the test executed
            }
            public override void ReadCompletionCallback(ref KeyStruct key, ref InputStruct input, ref OutputStruct output, Empty ctx, Status status, RecordInfo recordInfo)
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

            string filename = path + "ReadAtAddressReadFlagsSkipReadCache" + deviceType.ToString() + ".log";
            log = TestUtils.CreateTestDevice(deviceType, filename);
            fht = new FasterKV<KeyStruct, ValueStruct>
                (128, new LogSettings { LogDevice = log, MemorySizeBits = 29, ReadCacheSettings = new ReadCacheSettings() });

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
                if (status == Status.PENDING)
                {
                    skipReadCacheSession.CompletePendingWithOutputs(out var completedOutputs, wait: true);
                    (status, output) = TestUtils.GetSinglePendingResult(completedOutputs);
                }
                Assert.AreEqual(Status.OK, status);
                VerifyOutput();
            }

            // This will just be an ordinary read, as the record is in memory.
            functions.expectedReadAddress = readAtAddress;
            status = skipReadCacheSession.Read(ref key1, ref input, ref output);
            Assert.AreEqual(Status.OK, status);
            VerifyOutput();

            // ReadCache is used when the record is read from disk.
            fht.Log.FlushAndEvict(wait:true);

            // SkipReadCache is primarily for indexing, so a read during index scan does not result in a readcache update.
            // Reading at a normal logical address will not use the readcache, because the "readcache" bit is not set in that logical address.
            // And we cannot get a readcache address, since reads satisfied from the readcache pass kInvalidAddress to functions.
            // Therefore, we test here simply that we do not put it in the readcache when we tell it not to.

            // Do not put it into the read cache.
            functions.expectedReadAddress = readAtAddress;
            RecordInfo recordInfo = new() { PreviousAddress = readAtAddress };
            status = skipReadCacheSession.Read(ref key1, ref input, ref output, ref recordInfo, ReadFlags.SkipReadCache);
            VerifyResult();

            Assert.AreEqual(fht.ReadCache.BeginAddress, fht.ReadCache.TailAddress);

            // Put it into the read cache.
            functions.expectedReadAddress = readAtAddress;
            recordInfo.PreviousAddress = readAtAddress; // Read*() sets this to the record's PreviousAddress (so caller can follow the chain), so reinitialize it.
            status = skipReadCacheSession.Read(ref key1, ref input, ref output, ref recordInfo);
            VerifyResult();

            Assert.Less(fht.ReadCache.BeginAddress, fht.ReadCache.TailAddress);

            // Now this will read from the read cache.
            functions.expectedReadAddress = Constants.kInvalidAddress;
            status = skipReadCacheSession.Read(ref key1, ref input, ref output);
            Assert.AreEqual(Status.OK, status);
            VerifyOutput();
        }

        // Simple Upsert test where ref key and ref value but nothing else set
        [Test]
        [Category("FasterKV")]
        [Category("Smoke")]
        public void UpsertDefaultsTest([Values] TestUtils.DeviceType deviceType)
        {
            string filename = path + "UpsertDefaultsTest" + deviceType.ToString() + ".log";
            log = TestUtils.CreateTestDevice(deviceType, filename);
            fht = new FasterKV<KeyStruct, ValueStruct>
                (128, new LogSettings { LogDevice = log, MemorySizeBits = 22, SegmentSizeBits = 22, PageSizeBits = 10 });
            session = fht.For(new Functions()).NewSession<Functions>();

            InputStruct input = default;
            OutputStruct output = default;

            var key1 = new KeyStruct { kfield1 = 13, kfield2 = 14 };
            var value = new ValueStruct { vfield1 = 23, vfield2 = 24 };

            Assert.AreEqual(0, fht.EntryCount);

            session.Upsert(ref key1, ref value);
            var status = session.Read(ref key1, ref input, ref output, Empty.Default, 0);
            AssertCompleted(Status.OK, status);

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

            string filename = path + "UpsertNoRefNoDefaultsTest" + deviceType.ToString() + ".log";
            log = TestUtils.CreateTestDevice(deviceType, filename);
            fht = new FasterKV<KeyStruct, ValueStruct>
              (128, new LogSettings { LogDevice = log, MemorySizeBits = 29 });
            session = fht.For(new Functions()).NewSession<Functions>();

            InputStruct input = default;
            OutputStruct output = default;

            var key1 = new KeyStruct { kfield1 = 13, kfield2 = 14 };
            var value = new ValueStruct { vfield1 = 23, vfield2 = 24 };

            session.Upsert(key1, value, Empty.Default, 0);
            var status = session.Read(ref key1, ref input, ref output, Empty.Default, 0);
            AssertCompleted(Status.OK, status);

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

            string filename = path + "UpsertSerialNumberTest" + deviceType.ToString() + ".log";
            log = TestUtils.CreateTestDevice(deviceType, filename);
            fht = new FasterKV<KeyStruct, ValueStruct>
                (128, new LogSettings { LogDevice = log, MemorySizeBits = 29 });
            session = fht.For(new Functions()).NewSession<Functions>();

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

                AssertCompleted(Status.OK, status);
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
    }
}