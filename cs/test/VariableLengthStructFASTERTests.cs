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
        private FasterKV<VLKey, VLValue, VLInput, byte[], Empty, VLFunctions> fht;
        private IDevice log;

        [SetUp]
        public void Setup()
        {
            log = Devices.CreateLogDevice(TestContext.CurrentContext.TestDirectory + "\\hlog1.log", deleteOnClose: true);
            fht = new FasterKV<VLKey, VLValue, VLInput, byte[], Empty, VLFunctions>
                (128, new VLFunctions(),
                new LogSettings { LogDevice = log, MemorySizeBits = 15, PageSizeBits = 10 },
                null, null, null, new VariableLengthStructSettings<VLKey, VLValue> { keyLength = new VLKey(), valueLength = new VLValue() }
                );
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
        public unsafe void VariableLengthTest1()
        {
            VLInput input = default(VLInput);

            Random r = new Random(100);

            for (int i = 0; i < 500; i++)
            {
                var key1 = new VLKey { key = i };
                var len = 1 + r.Next(100);
                byte* val = stackalloc byte[4 + len];
                ref VLValue value = ref *(VLValue*)val;
                * (int*)val = len;
                for (int j = 0; j < len; j++)
                    *(val + 4 + j) = (byte)len;

                fht.Upsert(ref key1, ref value, Empty.Default, 0);
            }

            r = new Random(100);
            for (int i = 0; i < 500; i++)
            {
                var key1 = new VLKey { key = i };
                var len = 1 + r.Next(100);

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
                        Assert.IsTrue(output[j] == len);
                    }
                }
            }
        }
    }
}
