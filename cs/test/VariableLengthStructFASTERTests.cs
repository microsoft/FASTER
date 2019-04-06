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
        private FasterKV<VLKey, VLValue, VLInput, VLOutput, Empty, VLFunctions> fht;
        private IDevice log;

        [SetUp]
        public void Setup()
        {
            log = Devices.CreateLogDevice(TestContext.CurrentContext.TestDirectory + "\\hlog1.log", deleteOnClose: true);
            fht = new FasterKV<VLKey, VLValue, VLInput, VLOutput, Empty, VLFunctions>
                (128, new VLFunctions(),
                new LogSettings { LogDevice = log, MemorySizeBits = 29 },
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
        public void NativeInMemWriteRead()
        {
            VLInput input = default(VLInput);
            VLOutput output = default(VLOutput);

            var key1 = new VLKey { key = 13 };
            var value = new VLValue();

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

            //Assert.IsTrue(output.value.vfield1 == value.vfield1);
            //Assert.IsTrue(output.value.vfield2 == value.vfield2);
        }
    }
}
