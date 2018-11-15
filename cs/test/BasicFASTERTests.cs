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
    internal class BasicFASTERTests
    {
        private ICustomFaster fht;
        private IDevice log;

        [SetUp]
        public void Setup()
        {
            log = new LocalStorageDevice(TestContext.CurrentContext.TestDirectory + "\\hybridlog_native.log", deleteOnClose: true);
            fht = FasterFactory.Create
                <KeyStruct, ValueStruct, InputStruct, OutputStruct, Empty, Functions, ICustomFaster>
                (128, new LogSettings { LogDevice = log });
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
        public unsafe void NativeInMemWriteRead()
        {
            OutputStruct output = default(OutputStruct);

            var key1 = new KeyStruct { kfield1 = 13, kfield2 = 14 };
            var value = new ValueStruct { vfield1 = 23, vfield2 = 24 };

            fht.Upsert(&key1, &value, null, 0);
            var status = fht.Read(&key1, null, &output, null, 0);

            if (status == Status.PENDING)
            {
                fht.CompletePending(true);
            }
            else
            {
                Assert.IsTrue(status == Status.OK);
            }

            Assert.IsTrue(output.value.vfield1 == value.vfield1);
            Assert.IsTrue(output.value.vfield2 == value.vfield2);
        }

        [Test]
        public unsafe void NativeInMemWriteRead2()
        {

            Random r = new Random(10);

            for (int c = 0; c < 1000; c++)
            {
                var i = r.Next(10000);
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };
                fht.Upsert(&key1, &value, null, 0);
            }

            r = new Random(10);

            for (int c = 0; c < 1000; c++)
            {
                var i = r.Next(10000);
                OutputStruct output = default(OutputStruct);
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };

                if (fht.Read(&key1, null, &output, null, 0) == Status.PENDING)
                {
                    fht.CompletePending(true);
                }

                Assert.IsTrue(output.value.vfield1 == value.vfield1);
                Assert.IsTrue(output.value.vfield2 == value.vfield2);
            }
        }

        [Test]
        public unsafe void NativeInMemRMW1()
        {
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
                var input = new InputStruct { ifield1 = i, ifield2 = i + 1 };
                fht.RMW(&key1, &input, null, 0);
            }
            for (int j = 0; j < nums.Length; ++j)
            {
                var i = nums[j];
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var input = new InputStruct { ifield1 = i, ifield2 = i + 1 };
                fht.RMW(&key1, &input, null, 0);
            }


            KeyStruct key = default(KeyStruct);
            ValueStruct value = default(ValueStruct);
            OutputStruct output = default(OutputStruct);
            Status status = default(Status);

            for (int j = 0; j < nums.Length; ++j)
            {
                var i = nums[j];

                key = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };

                status = fht.Read(&key, null, &output, null, 0);

                if (status == Status.PENDING)
                {
                    fht.CompletePending(true);
                }
                else
                {
                    Assert.IsTrue(status == Status.OK);
                }
                Assert.IsTrue(output.value.vfield1 == 2*value.vfield1, "found " + output.value.vfield1 + ", expected " + 2 * value.vfield1);
                Assert.IsTrue(output.value.vfield2 == 2*value.vfield2);
            }

            key = new KeyStruct { kfield1 = nums.Length, kfield2 = nums.Length + 1 };
            status = fht.Read(&key, null, &output, null, 0);

            if (status == Status.PENDING)
            {
                fht.CompletePending(true);
            }
            else
            {
                Assert.IsTrue(status == Status.NOTFOUND);
            }
        }

    }
}
