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
            log = Devices.CreateLogDevice(TestContext.CurrentContext.TestDirectory + "\\BlittableLogCompactionTests.log", deleteOnClose: true);
            fht = new FasterKV<KeyStruct, ValueStruct>
                (1L << 20, new LogSettings { LogDevice = log, MemorySizeBits = 15, PageSizeBits = 9 });
        }

        [TearDown]
        public void TearDown()
        {
            fht.Dispose();
            fht = null;
            log.Dispose();
        }

        [Test]
        public void BlittableLogCompactionTest1()
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

            compactUntil = session.Compact(compactUntil, true);
            Assert.IsTrue(fht.Log.BeginAddress == compactUntil);

            // Read 2000 keys - all should be present
            for (int i = 0; i < totalRecords; i++)
            {
                OutputStruct output = default;
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };

                var status = session.Read(ref key1, ref input, ref output, 0, 0);
                if (status == Status.PENDING)
                    session.CompletePending(true);
                else
                {
                    Assert.IsTrue(status == Status.OK);
                    Assert.IsTrue(output.value.vfield1 == value.vfield1);
                    Assert.IsTrue(output.value.vfield2 == value.vfield2);
                }
            }
        }


        [Test]
        public void BlittableLogCompactionTest2()
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
            compactUntil = session.Compact(compactUntil, true);
            Assert.IsTrue(fht.Log.BeginAddress == compactUntil);
            Assert.IsTrue(fht.Log.TailAddress == tail);

            // Read 2000 keys - all should be present
            for (int i = 0; i < totalRecords; i++)
            {
                OutputStruct output = default;
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };

                var status = session.Read(ref key1, ref input, ref output, 0, 0);
                if (status == Status.PENDING)
                    session.CompletePending(true);
                else
                {
                    Assert.IsTrue(status == Status.OK);
                    Assert.IsTrue(output.value.vfield1 == value.vfield1);
                    Assert.IsTrue(output.value.vfield2 == value.vfield2);
                }
            }
        }


        [Test]
        public void BlittableLogCompactionTest3()
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
            compactUntil = session.Compact(compactUntil, true);
            Assert.IsTrue(fht.Log.BeginAddress == compactUntil);

            // Read 2000 keys - all should be present
            for (int i = 0; i < totalRecords; i++)
            {
                OutputStruct output = default;
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };

                int ctx = ((i < 500) && (i % 2 == 0)) ? 1 : 0;

                var status = session.Read(ref key1, ref input, ref output, ctx, 0);
                if (status == Status.PENDING)
                    session.CompletePending(true);
                else
                {
                    if (ctx == 0)
                    {
                        Assert.IsTrue(status == Status.OK);
                        Assert.IsTrue(output.value.vfield1 == value.vfield1);
                        Assert.IsTrue(output.value.vfield2 == value.vfield2);
                    }
                    else
                    {
                        Assert.IsTrue(status == Status.NOTFOUND);
                    }
                }
            }
        }

        [Test]
        public void BlittableLogCompactionCustomFunctionsTest1()
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
            compactUntil = session.Compact(compactUntil, true, default(EvenCompactionFunctions));
            Assert.IsTrue(fht.Log.BeginAddress == compactUntil);

            // Read 2000 keys - all should be present
            for (var i = 0; i < totalRecords; i++)
            {
                OutputStruct output = default;
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };

                var ctx = (i < (totalRecords / 2) && (i % 2 != 0)) ? 1 : 0;

                var status = session.Read(ref key1, ref input, ref output, ctx, 0);
                if (status == Status.PENDING)
                {
                    session.CompletePending(true);
                }
                else
                {
                    if (ctx == 0)
                    {
                        Assert.IsTrue(status == Status.OK);
                        Assert.IsTrue(output.value.vfield1 == value.vfield1);
                        Assert.IsTrue(output.value.vfield2 == value.vfield2);
                    }
                    else
                    {
                        Assert.IsTrue(status == Status.NOTFOUND);
                    }
                }
            }
        }

        [Test]
        public void BlittableLogCompactionCustomFunctionsTest2()
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

            fht.Log.Flush(true);

            var compactionFunctions = new Test2CompactionFunctions();
            session.Compact(fht.Log.TailAddress, true, compactionFunctions);

            Assert.IsFalse(compactionFunctions.CopyCalled);

            var input = default(InputStruct);
            var output = default(OutputStruct);
            var status = session.Read(ref key, ref input, ref output, 0, 0);
            if (status == Status.PENDING)
            {
                session.CompletePending(true);
            }
            else
            {
                Assert.IsTrue(status == Status.OK);
                Assert.IsTrue(output.value.vfield1 == value.vfield1);
                Assert.IsTrue(output.value.vfield2 == value.vfield2);
            }
        }

        private class Test2CompactionFunctions : ICompactionFunctions<KeyStruct, ValueStruct>
        {
            public bool CopyCalled;

            public void Copy(ref ValueStruct src, ref ValueStruct dst, IVariableLengthStruct<ValueStruct> valueLength)
            {
                if (src.vfield1 == 11 && src.vfield2 == 21)
                    CopyCalled = true;
                dst = src;
            }

            public bool CopyInPlace(ref ValueStruct src, ref ValueStruct dst, IVariableLengthStruct<ValueStruct> valueLength)
            {
                return false;
            }

            public bool IsDeleted(in KeyStruct key, in ValueStruct value)
            {
                return false;
            }
        }

        private struct EvenCompactionFunctions : ICompactionFunctions<KeyStruct, ValueStruct>
        {
            public void Copy(ref ValueStruct src, ref ValueStruct dst, IVariableLengthStruct<ValueStruct> valueLength)
            {
                dst = src;
            }

            public bool CopyInPlace(ref ValueStruct src, ref ValueStruct dst, IVariableLengthStruct<ValueStruct> valueLength)
            {
                dst = src;
                return true;
            }

            public bool IsDeleted(in KeyStruct key, in ValueStruct value)
            {
                return value.vfield1 % 2 != 0;
            }
        }
    }
}
