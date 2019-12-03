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
    internal class VariableLengthStructFASTERTests
    {
        [Test]
        public unsafe void VariableLengthTest1()
        {
            FasterKV<Key, VLValue, Input, int[], Empty, VLFunctions> fht;
            IDevice log;
            log = Devices.CreateLogDevice(TestContext.CurrentContext.TestDirectory + "\\hlog1.log", deleteOnClose: true);
            fht = new FasterKV<Key, VLValue, Input, int[], Empty, VLFunctions>
                (128, new VLFunctions(),
                new LogSettings { LogDevice = log, MemorySizeBits = 17, PageSizeBits = 12 },
                null, null, null, new VariableLengthStructSettings<Key, VLValue> { valueLength = new VLValue() }
                );
            fht.StartSession();


            Input input = default(Input);

            Random r = new Random(100);

            for (int i = 0; i < 5000; i++)
            {
                var key1 = new Key { key = i };

                var len = 2 + r.Next(10);
                int* val = stackalloc int[len];
                ref VLValue value = ref *(VLValue*)val;
                for (int j = 0; j < len; j++)
                    *(val + j) = len;

                fht.Upsert(ref key1, ref value, Empty.Default, 0);
            }

            r = new Random(100);
            for (int i = 0; i < 5000; i++)
            {
                var key1 = new Key { key = i };

                var len = 2 + r.Next(10);
                int[] output = null;
                var status = fht.Read(ref key1, ref input, ref output, Empty.Default, 0);

                if (status == Status.PENDING)
                {
                    fht.CompletePending(true);
                }
                else
                {
                    Assert.IsTrue(status == Status.OK);
                    Assert.IsTrue(output.Length == len);
                    for (int j = 0; j < len; j++)
                    {
                        Assert.IsTrue(output[j] == len);
                    }
                }
            }
            fht.StopSession();
            fht.Dispose();
            fht = null;
            log.Close();
        }

        [Test]
        public unsafe void VariableLengthTest2()
        {
            FasterKV<VLValue, VLValue, Input, int[], Empty, VLFunctions2> fht;
            IDevice log;
            log = Devices.CreateLogDevice(TestContext.CurrentContext.TestDirectory + "\\hlog1.log", deleteOnClose: true);
            fht = new FasterKV<VLValue, VLValue, Input, int[], Empty, VLFunctions2>
                (128, new VLFunctions2(),
                new LogSettings { LogDevice = log, MemorySizeBits = 17, PageSizeBits = 12 },
                null, null, null, new VariableLengthStructSettings<VLValue, VLValue> { keyLength = new VLValue(), valueLength = new VLValue() }
                );
            fht.StartSession();


            Input input = default(Input);

            Random r = new Random(100);

            for (int i = 0; i < 5000; i++)
            {
                var keylen = 2 + r.Next(10);
                int* keyval = stackalloc int[keylen];
                ref VLValue key1 = ref *(VLValue*)keyval;
                key1.length = keylen;
                for (int j = 1; j < keylen; j++)
                    *(keyval + j) = i;

                var len = 2 + r.Next(10);
                int* val = stackalloc int[len];
                ref VLValue value = ref *(VLValue*)val;
                for (int j = 0; j < len; j++)
                    *(val + j) = len;

                fht.Upsert(ref key1, ref value, Empty.Default, 0);
            }

            r = new Random(100);
            for (int i = 0; i < 5000; i++)
            {
                var keylen = 2 + r.Next(10);
                int* keyval = stackalloc int[keylen];
                ref VLValue key1 = ref *(VLValue*)keyval;
                key1.length = keylen;
                for (int j = 1; j < keylen; j++)
                    *(keyval + j) = i;

                var len = 2 + r.Next(10);

                int[] output = null;
                var status = fht.Read(ref key1, ref input, ref output, Empty.Default, 0);

                if (status == Status.PENDING)
                {
                    fht.CompletePending(true);
                }
                else
                {
                    Assert.IsTrue(status == Status.OK);
                    Assert.IsTrue(output.Length == len);
                    for (int j = 0; j < len; j++)
                    {
                        Assert.IsTrue(output[j] == len);
                    }
                }
            }
            fht.StopSession();
            fht.Dispose();
            fht = null;
            log.Close();
        }
        
    }
}
