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
            FasterKV<Key, VLValue, Input, byte[], Empty, VLFunctions> fht;
            IDevice log;
            log = Devices.CreateLogDevice(TestContext.CurrentContext.TestDirectory + "\\hlog1.log", deleteOnClose: true);
            fht = new FasterKV<Key, VLValue, Input, byte[], Empty, VLFunctions>
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
                var len = 1 + r.Next(1000);
                byte* val = stackalloc byte[4 + len];
                ref VLValue value = ref *(VLValue*)val;
                *(int*)val = len;
                for (int j = 0; j < len; j++)
                    *(val + 4 + j) = (byte)len;

                fht.Upsert(ref key1, ref value, Empty.Default, 0);
            }

            r = new Random(100);
            for (int i = 0; i < 5000; i++)
            {
                var key1 = new Key { key = i };
                var len = 1 + r.Next(1000);

                byte[] output = null;
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
                        Assert.IsTrue(output[j] == (byte)len);
                    }
                }
            }
            fht.StopSession();
            fht.Dispose();
            fht = null;
            log.Close();
        }

        /*
        public unsafe void VariableLengthTest2()
        {
            FasterKV<VLValue, VLValue, Input, byte[], Empty, VLFunctions2> fht;
            IDevice log;
            log = Devices.CreateLogDevice(TestContext.CurrentContext.TestDirectory + "\\hlog1.log", deleteOnClose: true);
            fht = new FasterKV<VLValue, VLValue, Input, byte[], Empty, VLFunctions2>
                (128, new VLFunctions2(),
                new LogSettings { LogDevice = log, MemorySizeBits = 17, PageSizeBits = 12 },
                null, null, null, new VariableLengthStructSettings<VLValue, VLValue> { keyLength = new VLValue(), valueLength = new VLValue() }
                );
            fht.StartSession();


            Input input = default(Input);

            Random r = new Random(100);

            for (int i = 0; i < 5000; i++)
            {
                var key1 = new Key { key = i };
                var len = 1 + r.Next(1000);
                byte* val = stackalloc byte[4 + len];
                ref VLValue value = ref *(VLValue*)val;
                *(int*)val = len;
                for (int j = 0; j < len; j++)
                    *(val + 4 + j) = (byte)len;

                // fht.Upsert(ref key1, ref value, Empty.Default, 0);
            }

            r = new Random(100);
            for (int i = 0; i < 5000; i++)
            {
                var key1 = new Key { key = i };
                var len = 1 + r.Next(1000);

                byte[] output = null;
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
                        Assert.IsTrue(output[j] == (byte)len);
                    }
                }
            }
            fht.StopSession();
            fht.Dispose();
            fht = null;
            log.Close();
        }
        */
    }
}
