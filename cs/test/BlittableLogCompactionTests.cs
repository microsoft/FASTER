// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using NUnit.Framework;

namespace FASTER.test
{
    [TestFixture]
    internal class BlittableLogCompactionTests
    {
        private FasterKV<KeyStruct, ValueStruct> fht;
        private IDevice log;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait:true);
            log = Devices.CreateLogDevice(TestUtils.MethodTestDir + "/BlittableLogCompactionTests.log", deleteOnClose: true);
            fht = new FasterKV<KeyStruct, ValueStruct>
                (1L << 20, new LogSettings { LogDevice = log, MemorySizeBits = 15, PageSizeBits = 9 });
        }

        [TearDown]
        public void TearDown()
        {
            fht?.Dispose();
            fht = null;
            log?.Dispose();
            log = null;
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        [Test]
        [Category("FasterKV")]
        [Category("Compaction")]
        [Category("Smoke")]

        public void BlittableLogCompactionTest1([Values] CompactionType compactionType)
        {
            using var session = fht.For(new FunctionsCompaction()).NewSession<FunctionsCompaction>();

            InputStruct input = default;

            const int totalRecords = 2000;
            var start = fht.Log.TailAddress;
            long compactUntil = 0;

            for (int i = 0; i < totalRecords; i++)
            {
                if (i == 1000)
                    compactUntil = fht.Log.TailAddress;

                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };
                session.Upsert(ref key1, ref value, 0, 0);
            }

            compactUntil = session.Compact(compactUntil, compactionType);
            fht.Log.Truncate();

            Assert.AreEqual(compactUntil, fht.Log.BeginAddress);

            // Read 2000 keys - all should be present
            for (int i = 0; i < totalRecords; i++)
            {
                OutputStruct output = default;
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };

                var status = session.Read(ref key1, ref input, ref output, 0, 0);
                if (status.IsPending)
                {
                    Assert.IsTrue(session.CompletePendingWithOutputs(out var outputs, wait: true));
                    (status, output) = TestUtils.GetSinglePendingResult(outputs);
                }

                Assert.IsTrue(status.Found);
                Assert.AreEqual(value.vfield1, output.value.vfield1);
                Assert.AreEqual(value.vfield2, output.value.vfield2);
            }
        }


        [Test]
        [Category("FasterKV")]
        [Category("Compaction")]
        public void BlittableLogCompactionTest2([Values] CompactionType compactionType)
        {
            using var session = fht.For(new FunctionsCompaction()).NewSession<FunctionsCompaction>();

            InputStruct input = default;

            const int totalRecords = 2000;
            var start = fht.Log.TailAddress;
            long compactUntil = 0;

            for (int i = 0; i < totalRecords; i++)
            {
                if (i == 1000)
                    compactUntil = fht.Log.TailAddress;

                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };
                session.Upsert(ref key1, ref value, 0, 0);
            }

            // Put fresh entries for 1000 records
            for (int i = 0; i < 1000; i++)
            {
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };
                session.Upsert(ref key1, ref value, 0, 0);
            }

            fht.Log.Flush(true);

            var tail = fht.Log.TailAddress;
            compactUntil = session.Compact(compactUntil, compactionType);
            fht.Log.Truncate();
            Assert.AreEqual(compactUntil, fht.Log.BeginAddress);
            Assert.AreEqual(tail, fht.Log.TailAddress);

            // Read 2000 keys - all should be present
            for (int i = 0; i < totalRecords; i++)
            {
                OutputStruct output = default;
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };

                var status = session.Read(ref key1, ref input, ref output, 0, 0);
                if (status.IsPending)
                {
                    Assert.IsTrue(session.CompletePendingWithOutputs(out var outputs, wait: true));
                    (status, output) = TestUtils.GetSinglePendingResult(outputs);
                }

                Assert.IsTrue(status.Found);
                Assert.AreEqual(value.vfield1, output.value.vfield1);
                Assert.AreEqual(value.vfield2, output.value.vfield2);
            }
        }


        [Test]
        [Category("FasterKV")]
        [Category("Compaction")]
        public void BlittableLogCompactionTest3([Values] CompactionType compactionType)
        {
            using var session = fht.For(new FunctionsCompaction()).NewSession<FunctionsCompaction>();

            InputStruct input = default;

            const int totalRecords = 2000;
            var start = fht.Log.TailAddress;
            long compactUntil = 0;

            for (int i = 0; i < totalRecords; i++)
            {
                if (i == totalRecords / 2)
                    compactUntil = fht.Log.TailAddress;

                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };
                session.Upsert(ref key1, ref value, 0, 0);

                if (i % 8 == 0)
                {
                    int j = i / 4;
                    key1 = new KeyStruct { kfield1 = j, kfield2 = j + 1 };
                    session.Delete(ref key1, 0, 0);
                }
            }

            var tail = fht.Log.TailAddress;
            compactUntil = session.Compact(compactUntil, compactionType);
            fht.Log.Truncate();
            Assert.AreEqual(compactUntil, fht.Log.BeginAddress);

            // Read all keys - all should be present except those we deleted
            for (int i = 0; i < totalRecords; i++)
            {
                OutputStruct output = default;
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };

                int ctx = ((i < 500) && (i % 2 == 0)) ? 1 : 0;

                var status = session.Read(ref key1, ref input, ref output, ctx, 0);
                if (status.IsPending)
                {
                    Assert.IsTrue(session.CompletePendingWithOutputs(out var outputs, wait: true));
                    (status, output) = TestUtils.GetSinglePendingResult(outputs);
                }

                if (ctx == 0)
                {
                    Assert.IsTrue(status.Found);
                    Assert.AreEqual(value.vfield1, output.value.vfield1);
                    Assert.AreEqual(value.vfield2, output.value.vfield2);
                }
                else
                {
                    Assert.IsFalse(status.Found);
                }
            }
        }

        [Test]
        [Category("FasterKV")]
        [Category("Compaction")]
        [Category("Smoke")]

        public void BlittableLogCompactionCustomFunctionsTest1([Values] CompactionType compactionType)
        {
            using var session = fht.For(new FunctionsCompaction()).NewSession<FunctionsCompaction>();

            InputStruct input = default;

            const int totalRecords = 2000;
            var start = fht.Log.TailAddress;
            var compactUntil = 0L;

            for (var i = 0; i < totalRecords; i++)
            {
                if (i == totalRecords / 2)
                    compactUntil = fht.Log.TailAddress;

                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };
                session.Upsert(ref key1, ref value, 0, 0);
            }

            var tail = fht.Log.TailAddress;

            // Only leave records with even vfield1
            compactUntil = session.Compact(compactUntil, compactionType, default(EvenCompactionFunctions));
            fht.Log.Truncate();
            Assert.AreEqual(compactUntil, fht.Log.BeginAddress);

            // Read 2000 keys - all should be present
            for (var i = 0; i < totalRecords; i++)
            {
                OutputStruct output = default;
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };

                var ctx = (i < (totalRecords / 2) && (i % 2 != 0)) ? 1 : 0;

                var status = session.Read(ref key1, ref input, ref output, ctx, 0);
                if (status.IsPending)
                {
                    Assert.IsTrue(session.CompletePendingWithOutputs(out var outputs, wait: true));
                    (status, output) = TestUtils.GetSinglePendingResult(outputs);
                }

                if (ctx == 0)
                {
                    Assert.IsTrue(status.Found);
                    Assert.AreEqual(value.vfield1, output.value.vfield1);
                    Assert.AreEqual(value.vfield2, output.value.vfield2);
                }
                else
                {
                    Assert.IsFalse(status.Found);
                }
            }
        }

        [Test]
        [Category("FasterKV")]
        [Category("Compaction")]

        public void BlittableLogCompactionCustomFunctionsTest2([Values] CompactionType compactionType, [Values]bool flushAndEvict)
        {
            // Update: irrelevant as session compaction no longer uses Copy/CopyInPlace
            // This test checks if CopyInPlace returning false triggers call to Copy

            using var session = fht.For(new FunctionsCompaction()).NewSession<FunctionsCompaction>();

            var key = new KeyStruct { kfield1 = 100, kfield2 = 101 };
            var value = new ValueStruct { vfield1 = 10, vfield2 = 20 };

            session.Upsert(ref key, ref value, 0, 0);

            fht.Log.Flush(true);

            value = new ValueStruct { vfield1 = 11, vfield2 = 21 };
            session.Upsert(ref key, ref value, 0, 0);

            if (flushAndEvict)
                fht.Log.FlushAndEvict(true);
            else
                fht.Log.Flush(true);

            var compactUntil = session.Compact(fht.Log.TailAddress, compactionType);
            fht.Log.Truncate();

            var input = default(InputStruct);
            var output = default(OutputStruct);
            var status = session.Read(ref key, ref input, ref output, 0, 0);
            if (status.IsPending)
            {
                Assert.IsTrue(session.CompletePendingWithOutputs(out var outputs, wait: true));
                (status, output) = TestUtils.GetSinglePendingResult(outputs);
            }

            Assert.IsTrue(status.Found);
            Assert.AreEqual(value.vfield1, output.value.vfield1);
            Assert.AreEqual(value.vfield2, output.value.vfield2);
        }

        private struct EvenCompactionFunctions : ICompactionFunctions<KeyStruct, ValueStruct>
        {
            public bool IsDeleted(ref KeyStruct key, ref ValueStruct value) => value.vfield1 % 2 != 0;
        }
    }
}
