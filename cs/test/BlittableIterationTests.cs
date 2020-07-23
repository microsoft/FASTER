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
    internal class BlittableIterationTests
    {
        private FasterKV<KeyStruct, ValueStruct, InputStruct, OutputStruct, int, FunctionsCompaction> fht;
        private IDevice log;

        [SetUp]
        public void Setup()
        {
            log = Devices.CreateLogDevice(TestContext.CurrentContext.TestDirectory + "\\BlittableIterationTests.log", deleteOnClose: true);
            fht = new FasterKV<KeyStruct, ValueStruct, InputStruct, OutputStruct, int, FunctionsCompaction>
                (1L << 20, new FunctionsCompaction(), new LogSettings { LogDevice = log, MemorySizeBits = 15, PageSizeBits = 7 });
        }

        [TearDown]
        public void TearDown()
        {
            fht.Dispose();
            fht = null;
            log.Close();
        }

        [Test]
        public void BlittableIterationTest1()
        {
            using var session = fht.NewSession();

            const int totalRecords = 2000;
            var start = fht.Log.TailAddress;

            for (int i = 0; i < totalRecords; i++)
            {
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };
                session.Upsert(ref key1, ref value, 0, 0);
            }

            int count = 0;
            var iter = fht.Iterate();
            while (iter.GetNext(out var recordInfo))
            {
                count++;
                Assert.IsTrue(iter.GetValue().vfield1 == iter.GetKey().kfield1);
            }
            iter.Dispose();

            Assert.IsTrue(count == totalRecords);


            for (int i = 0; i < totalRecords; i++)
            {
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = 2 * i, vfield2 = i + 1 };
                session.Upsert(ref key1, ref value, 0, 1);
            }

            count = 0;
            iter = fht.Iterate();
            while (iter.GetNext(out var recordInfo))
            {
                count++;
                Assert.IsTrue(iter.GetValue().vfield1 == iter.GetKey().kfield1 * 2);
            }
            iter.Dispose();

            Assert.IsTrue(count == totalRecords);


            for (int i = totalRecords/2; i < totalRecords; i++)
            {
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };
                session.Upsert(ref key1, ref value, 0, 0);
            }

            count = 0;
            iter = fht.Iterate();
            while (iter.GetNext(out var recordInfo))
            {
                count++;
            }
            iter.Dispose();

            Assert.IsTrue(count == totalRecords);

            for (int i = 0; i < totalRecords; i+=2)
            {
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };
                session.Upsert(ref key1, ref value, 0, 0);
            }

            count = 0;
            iter = fht.Iterate();
            while (iter.GetNext(out var recordInfo))
            {
                count++;
            }
            iter.Dispose();

            Assert.IsTrue(count == totalRecords);

            for (int i = 0; i < totalRecords; i += 2)
            {
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };
                session.Delete(ref key1, 0, 0);
            }

            count = 0;
            iter = fht.Iterate();
            while (iter.GetNext(out var recordInfo))
            {
                count++;
            }
            iter.Dispose();

            Assert.IsTrue(count == totalRecords / 2);


            for (int i = 0; i < totalRecords; i++)
            {
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = 3 * i, vfield2 = i + 1 };
                session.Upsert(ref key1, ref value, 0, 1);
            }

            count = 0;
            iter = fht.Iterate();
            while (iter.GetNext(out var recordInfo))
            {
                count++;
                Assert.IsTrue(iter.GetValue().vfield1 == iter.GetKey().kfield1 * 3);
            }
            iter.Dispose();

            Assert.IsTrue(count == totalRecords);

        }
    }
}
