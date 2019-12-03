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
        private FasterKV<KeyStruct, ValueStruct, InputStruct, OutputStruct, int, FunctionsCompaction> fht;
        private IDevice log;

        [SetUp]
        public void Setup()
        {
            log = Devices.CreateLogDevice(TestContext.CurrentContext.TestDirectory + "\\BlittableLogCompactionTests.log", deleteOnClose: true);
            fht = new FasterKV<KeyStruct, ValueStruct, InputStruct, OutputStruct, int, FunctionsCompaction>
                (1L << 20, new FunctionsCompaction(), new LogSettings { LogDevice = log, MemorySizeBits = 15, PageSizeBits = 7 });
            fht.StartSession();
        }

        [TearDown]
        public void TearDown()
        {
            fht.StopSession();
            fht.Dispose();
            fht = null;
            log.Close();
        }

        [Test]
        public void BlittableLogCompactionTest1()
        {
            InputStruct input = default(InputStruct);

            const int totalRecords = 2000;
            var start = fht.Log.TailAddress;
            long compactUntil = 0;

            for (int i = 0; i < totalRecords; i++)
            {
                if (i == 1000)
                    compactUntil = fht.Log.TailAddress;

                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };
                fht.Upsert(ref key1, ref value, 0, 0);
            }

            fht.Log.Compact(compactUntil);
            Assert.IsTrue(fht.Log.BeginAddress == compactUntil);

            // Read 2000 keys - all should be present
            for (int i = 0; i < totalRecords; i++)
            {
                OutputStruct output = default(OutputStruct);
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };

                var status = fht.Read(ref key1, ref input, ref output, 0, 0);
                if (status == Status.PENDING)
                    fht.CompletePending(true);
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
            InputStruct input = default(InputStruct);

            const int totalRecords = 2000;
            var start = fht.Log.TailAddress;
            long compactUntil = 0;

            for (int i = 0; i < totalRecords; i++)
            {
                if (i == 1000)
                    compactUntil = fht.Log.TailAddress;

                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };
                fht.Upsert(ref key1, ref value, 0, 0);
            }

            // Put fresh entries for 1000 records
            for (int i = 0; i < 1000; i++)
            {
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };
                fht.Upsert(ref key1, ref value, 0, 0);
            }

            fht.Log.Flush(true);

            var tail = fht.Log.TailAddress;
            fht.Log.Compact(compactUntil);
            Assert.IsTrue(fht.Log.BeginAddress == compactUntil);
            Assert.IsTrue(fht.Log.TailAddress == tail);

            // Read 2000 keys - all should be present
            for (int i = 0; i < totalRecords; i++)
            {
                OutputStruct output = default(OutputStruct);
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };

                var status = fht.Read(ref key1, ref input, ref output, 0, 0);
                if (status == Status.PENDING)
                    fht.CompletePending(true);
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
            InputStruct input = default(InputStruct);

            const int totalRecords = 2000;
            var start = fht.Log.TailAddress;
            long compactUntil = 0;

            for (int i = 0; i < totalRecords; i++)
            {
                if (i == totalRecords / 2)
                    compactUntil = fht.Log.TailAddress;

                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };
                fht.Upsert(ref key1, ref value, 0, 0);

                if (i % 8 == 0)
                {
                    int j = i / 4;
                    key1 = new KeyStruct { kfield1 = j, kfield2 = j + 1 };
                    fht.Delete(ref key1, 0, 0);
                }
            }

            var tail = fht.Log.TailAddress;
            fht.Log.Compact(compactUntil);
            Assert.IsTrue(fht.Log.BeginAddress == compactUntil);

            // Read 2000 keys - all should be present
            for (int i = 0; i < totalRecords; i++)
            {
                OutputStruct output = default(OutputStruct);
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };

                int ctx = ((i < 500) && (i % 2 == 0)) ? 1 : 0;

                var status = fht.Read(ref key1, ref input, ref output, ctx, 0);
                if (status == Status.PENDING)
                    fht.CompletePending(true);
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

    }
}
