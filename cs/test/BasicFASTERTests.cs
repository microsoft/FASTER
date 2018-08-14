// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using FASTER.core;
using System.IO;

namespace FASTER.test
{

    [TestClass]
    public class BasicFASTERTests
    {
        private ICustomFaster fht;

        [TestInitialize]
        public void Setup()
        {
            var log = FASTERFactory.CreateLogDevice(Path.GetTempPath() + "\\hybridlog_native.log");
            fht = FASTERFactory.Create
                <KeyStruct, ValueStruct, InputStruct, OutputStruct, Empty, Functions, ICustomFaster>
                (128, log);
            fht.StartSession();
        }

        [TestCleanup]
        public void TearDown()
        {
            fht.StopSession();
            fht = null;
        }



        [TestMethod]
        public unsafe void NativeInMemWriteRead()
        {
            OutputStruct output = default(OutputStruct);

            var key1 = new KeyStruct { kfield1 = 13, kfield2 = 14 };
            var value = new ValueStruct { vfield1 = 23, vfield2 = 24 };

            fht.Upsert(&key1, &value, null, 0);
            fht.Read(&key1, null, &output, null, 0);

            Assert.IsTrue(output.value.vfield1 == value.vfield1);
            Assert.IsTrue(output.value.vfield2 == value.vfield2);
        }

        [TestMethod]
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

        [TestMethod]
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


            for (int j = 0; j < nums.Length; ++j)
            {
                var i = nums[j];

                OutputStruct output = default(OutputStruct);
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };

                if (fht.Read(&key1, null, &output, null, 0) == Status.PENDING)
                {
                    fht.CompletePending(true);
                }

                Assert.IsTrue(output.value.vfield1 == 2*value.vfield1, "found " + output.value.vfield1 + ", expected " + 2 * value.vfield1);
                Assert.IsTrue(output.value.vfield2 == 2*value.vfield2);
            }
        }

    }
}
