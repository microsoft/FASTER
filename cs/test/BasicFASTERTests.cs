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
        private FasterKV<KeyStruct, ValueStruct, InputStruct, OutputStruct, Empty, Functions> fht;
        private IDevice log;

        [SetUp]
        public void Setup()
        {
            log = Devices.CreateLogDevice(TestContext.CurrentContext.TestDirectory + "\\hlog1.log", deleteOnClose: true);
            fht = new FasterKV<KeyStruct, ValueStruct, InputStruct, OutputStruct, Empty, Functions>
                (128, new Functions(), new LogSettings { LogDevice = log, MemorySizeBits = 29 });
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
        public void NativeInMemWriteRead()
        {
            InputStruct input = default(InputStruct);
            OutputStruct output = default(OutputStruct);

            var key1 = new KeyStruct { kfield1 = 13, kfield2 = 14 };
            var value = new ValueStruct { vfield1 = 23, vfield2 = 24 };

            fht.Upsert(ref key1, ref value, Empty.Default, 0);
            var status = fht.Read(ref key1, ref input, ref output, Empty.Default, 0);

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
            InputStruct input = default(InputStruct);

            Random r = new Random(10);
            for (int c = 0; c < 1000; c++)
            {
                var i = r.Next(10000);
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };
                fht.Upsert(ref key1, ref value, Empty.Default, 0);
            }

            r = new Random(10);

            for (int c = 0; c < 1000; c++)
            {
                var i = r.Next(10000);
                OutputStruct output = default(OutputStruct);
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };

                if (fht.Read(ref key1, ref input, ref output, Empty.Default, 0) == Status.PENDING)
                {
                    fht.CompletePending(true);
                }

                Assert.IsTrue(output.value.vfield1 == value.vfield1);
                Assert.IsTrue(output.value.vfield2 == value.vfield2);
            }

            // Clean up and retry - should not find now
            fht.ShiftBeginAddress(fht.LogTailAddress);

            r = new Random(10);
            for (int c = 0; c < 1000; c++)
            {
                var i = r.Next(10000);
                OutputStruct output = default(OutputStruct);
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };

                Assert.IsTrue(fht.Read(ref key1, ref input, ref output, Empty.Default, 0) == Status.NOTFOUND);
            }
        }

        [Test]
        public unsafe void TestShiftHeadAddress()
        {
            InputStruct input = default(InputStruct);

            Random r = new Random(10);
            for (int c = 0; c < 1000; c++)
            {
                var i = r.Next(10000);
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };
                fht.Upsert(ref key1, ref value, Empty.Default, 0);
            }

            r = new Random(10);

            for (int c = 0; c < 1000; c++)
            {
                var i = r.Next(10000);
                OutputStruct output = default(OutputStruct);
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };

                if (fht.Read(ref key1, ref input, ref output, Empty.Default, 0) == Status.PENDING)
                {
                    fht.CompletePending(true);
                }

                Assert.IsTrue(output.value.vfield1 == value.vfield1);
                Assert.IsTrue(output.value.vfield2 == value.vfield2);
            }

            // Shift head and retry - should not find in main memory now
            fht.ShiftHeadAddress(fht.LogTailAddress, true);

            r = new Random(10);
            for (int c = 0; c < 1000; c++)
            {
                var i = r.Next(10000);
                OutputStruct output = default(OutputStruct);
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };

                Assert.IsTrue(fht.Read(ref key1, ref input, ref output, Empty.Default, 0) == Status.PENDING);
                fht.CompletePending(true);
            }
        }

        [Test]
        public unsafe void NativeInMemRMW1()
        {
            InputStruct input = default(InputStruct);

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
                fht.RMW(ref key1, ref input, Empty.Default, 0);
            }
            for (int j = 0; j < nums.Length; ++j)
            {
                var i = nums[j];
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                input = new InputStruct { ifield1 = i, ifield2 = i + 1 };
                fht.RMW(ref key1, ref input, Empty.Default, 0);
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

                status = fht.Read(ref key, ref input, ref output, Empty.Default, 0);

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
            status = fht.Read(ref key, ref input, ref output, Empty.Default, 0);

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
