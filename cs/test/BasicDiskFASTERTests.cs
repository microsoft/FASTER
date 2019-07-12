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
using FASTER.cloud;

namespace FASTER.test
{


    // TODO(Tianyu): Now that we are also testing device with Azure Page Blobs here, should we also rename the test?
    [TestFixture]
    internal class BasicDiskFASTERTests
    {
        private FasterKV<KeyStruct, ValueStruct, InputStruct, OutputStruct, Empty, Functions> fht;
        private IDevice log;
        public const string EMULATED_STORAGE_STRING = "UseDevelopmentStorage=true;";
        public const string TEST_CONTAINER = "test";

        void TestDeviceWriteRead(IDevice log)
        {
            this.log = log;
            fht = new FasterKV<KeyStruct, ValueStruct, InputStruct, OutputStruct, Empty, Functions>
                       (1L << 20, new Functions(), new LogSettings { LogDevice = log, MemorySizeBits = 15, PageSizeBits = 10 });
            fht.StartSession();

            InputStruct input = default(InputStruct);

            for (int i = 0; i < 2000; i++)
            {
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };
                fht.Upsert(ref key1, ref value, Empty.Default, 0);
            }
            fht.CompletePending(true);

            // Update first 100 using RMW from storage
            for (int i = 0; i < 100; i++)
            {
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                input = new InputStruct { ifield1 = 1, ifield2 = 1 };
                var status = fht.RMW(ref key1, ref input, Empty.Default, 0);
                if (status == Status.PENDING)
                    fht.CompletePending(true);
            }


            for (int i = 0; i < 2000; i++)
            {
                OutputStruct output = default(OutputStruct);
                var key1 = new KeyStruct { kfield1 = i, kfield2 = i + 1 };
                var value = new ValueStruct { vfield1 = i, vfield2 = i + 1 };

                if (fht.Read(ref key1, ref input, ref output, Empty.Default, 0) == Status.PENDING)
                {
                    fht.CompletePending(true);
                }
                else
                {
                    if (i < 100)
                    {
                        Assert.IsTrue(output.value.vfield1 == value.vfield1 + 1);
                        Assert.IsTrue(output.value.vfield2 == value.vfield2 + 1);
                    }
                    else
                    {
                        Assert.IsTrue(output.value.vfield1 == value.vfield1);
                        Assert.IsTrue(output.value.vfield2 == value.vfield2);
                    }
                }
            }
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
        public void NativeDiskWriteRead()
        {
            TestDeviceWriteRead(Devices.CreateLogDevice(TestContext.CurrentContext.TestDirectory + "\\BasicDiskFASTERTests.log", deleteOnClose: true));
        }

        [Test]
        public void PageBlobWriteRead()
        {
            TestDeviceWriteRead(new AzurePageBlobDevice(EMULATED_STORAGE_STRING, TEST_CONTAINER, "BasicDiskFASTERTests", false));
        }
    }
}
