// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using FASTER.core;
using NUnit.Framework;
using static FASTER.test.TestUtils;

namespace FASTER.test
{
    [TestFixture]
    internal class VariableLengthStructFASTERTests
    {
        // VarLenMax is the variable-length portion; 2 is for the fixed fields
        const int VarLenMax = 10;
        const int StackAllocMax = VarLenMax + 2;

        static int GetVarLen(Random r) => r.Next(VarLenMax) + 2;

        [Test]
        [Category("FasterKV")]
        [Category("Smoke")]
        public unsafe void VariableLengthTest1()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);

            var log = Devices.CreateLogDevice(TestUtils.MethodTestDir + "/hlog1.log", deleteOnClose: true);
            var fht = new FasterKV<Key, VLValue>
                (128,
                new LogSettings { LogDevice = log, MemorySizeBits = 17, PageSizeBits = 12 },
                null, null, null, new VariableLengthStructSettings<Key, VLValue> { valueLength = new VLValue() }
                );
            var s = fht.NewSession(new VLFunctions());

            Input input = default;
            Random r = new(100);

            // Single alloc outside the loop, to the max length we'll need.
            int* val = stackalloc int[StackAllocMax];

            for (int i = 0; i < 5000; i++)
            {
                var key1 = new Key { key = i };

                var len = GetVarLen(r);
                ref VLValue value = ref *(VLValue*)val;
                for (int j = 0; j < len; j++)
                    *(val + j) = len;

                s.Upsert(ref key1, ref value, Empty.Default, 0);
            }

            r = new Random(100);
            for (int i = 0; i < 5000; i++)
            {
                var key1 = new Key { key = i };

                var len = 2 + r.Next(10);
                int[] output = null;
                var status = s.Read(ref key1, ref input, ref output, Empty.Default, 0);

                if (status.Pending)
                {
                    s.CompletePendingWithOutputs(out var outputs, wait: true);
                    (status, output) = GetSinglePendingResult(outputs);
                }

                Assert.IsTrue(status.Found);
                Assert.AreEqual(len, output.Length);
                for (int j = 0; j < len; j++)
                {
                    Assert.AreEqual(len, output[j]);
                }
            }
            s.Dispose();
            fht.Dispose();
            log.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        [Test]
        [Category("FasterKV")]
        public unsafe void VariableLengthTest2()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);

            var log = Devices.CreateLogDevice(TestUtils.MethodTestDir + "/hlog1.log", deleteOnClose: true);
            var fht = new FasterKV<VLValue, VLValue>
                (128,
                new LogSettings { LogDevice = log, MemorySizeBits = 17, PageSizeBits = 12 },
                null, null, null, new VariableLengthStructSettings<VLValue, VLValue> { keyLength = new VLValue(), valueLength = new VLValue() }
                );
            var s = fht.NewSession(new VLFunctions2());

            Input input = default;
            Random r = new(100);

            // Single alloc outside the loop, to the max length we'll need.
            int* keyval = stackalloc int[StackAllocMax];
            int* val = stackalloc int[StackAllocMax];

            for (int i = 0; i < 5000; i++)
            {
                var keylen = GetVarLen(r);
                ref VLValue key1 = ref *(VLValue*)keyval;
                key1.length = keylen;
                for (int j = 1; j < keylen; j++)
                    *(keyval + j) = i;

                var len = GetVarLen(r);
                ref VLValue value = ref *(VLValue*)val;
                for (int j = 0; j < len; j++)
                    *(val + j) = len;

                s.Upsert(ref key1, ref value, Empty.Default, 0);
            }

            r = new Random(100);
            for (int i = 0; i < 5000; i++)
            {
                var keylen = GetVarLen(r);
                ref VLValue key1 = ref *(VLValue*)keyval;
                key1.length = keylen;
                for (int j = 1; j < keylen; j++)
                    *(keyval + j) = i;

                var len = 2 + r.Next(10);

                int[] output = null;
                var status = s.Read(ref key1, ref input, ref output, Empty.Default, 0);

                if (status.Pending)
                {
                    s.CompletePendingWithOutputs(out var outputs, wait: true);
                    (status, output) = GetSinglePendingResult(outputs);
                }

                Assert.IsTrue(status.Found);
                Assert.AreEqual(len, output.Length);
                for (int j = 0; j < len; j++)
                {
                    Assert.AreEqual(len, output[j]);
                }
            }

            s.Dispose();
            fht.Dispose();
            log.Dispose();
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }
    }
}
