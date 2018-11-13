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

namespace FASTER.test.largeobjects
{

    [TestFixture]
    internal class LargeObjectTests
    {
        private IManagedFasterKV<MyKey, MyLargeValue, MyInput, MyLargeOutput, MyContext> fht1;
        private IManagedFasterKV<MyKey, MyLargeValue, MyInput, MyLargeOutput, MyContext> fht2;
        private IDevice log, objlog;
        
        [SetUp]
        public void Setup()
        {
            log = FasterFactory.CreateLogDevice(TestContext.CurrentContext.TestDirectory + "\\hlog", deleteOnClose: true);
            objlog = FasterFactory.CreateObjectLogDevice(TestContext.CurrentContext.TestDirectory + "\\hlog", deleteOnClose: true);

            Directory.CreateDirectory(TestContext.CurrentContext.TestDirectory + "\\checkpoints");

            fht1 = FasterFactory.Create
                <MyKey, MyLargeValue, MyInput, MyLargeOutput, MyContext, MyLargeFunctions>
                (indexSizeBuckets: 128, functions: new MyLargeFunctions(),
                logSettings: new LogSettings { LogDevice = log, ObjectLogDevice = objlog, MutableFraction = 0.1, PageSizeBits = 9, MemorySizeBits = 13 },
                checkpointSettings: new CheckpointSettings { CheckpointDir = TestContext.CurrentContext.TestDirectory + "\\checkpoints", CheckPointType = CheckpointType.Snapshot }
                );

            fht2 = FasterFactory.Create
                <MyKey, MyLargeValue, MyInput, MyLargeOutput, MyContext, MyLargeFunctions>
                (indexSizeBuckets: 128, functions: new MyLargeFunctions(),
                logSettings: new LogSettings { LogDevice = log, ObjectLogDevice = objlog, MutableFraction = 0.1, PageSizeBits = 9, MemorySizeBits = 13 },
                checkpointSettings: new CheckpointSettings { CheckpointDir = TestContext.CurrentContext.TestDirectory + "\\checkpoints", CheckPointType = CheckpointType.Snapshot }
                );
        }

        [TearDown]
        public void TearDown()
        {
            log.Close();
            objlog.Close();
            new DirectoryInfo(TestContext.CurrentContext.TestDirectory + "\\checkpoints").Delete(true);
        }


        [Test]
        public void LargeObjectTest1()
        {
            int maxSize = 100;
            int numOps = 5000;
            //var value = new MyLargeValue(size);

            fht1.StartSession();
            Random r = new Random(33);
            for (int key = 0; key < numOps; key++)
            {
                var value = new MyLargeValue(1+r.Next(maxSize));
                fht1.Upsert(new MyKey { key = key }, value, null, 0);
            }
            fht1.TakeFullCheckpoint(out Guid token);
            fht1.CompleteCheckpoint(true);
            fht1.StopSession();
            fht1.Dispose();

            MyLargeOutput output = new MyLargeOutput();
            fht2.Recover(token);
            fht2.StartSession();
            for (int key = 0; key < numOps; key++)
            {
                var status = fht2.Read(new MyKey { key = key }, new MyInput(), ref output, null, 0);

                if (status == Status.PENDING)
                    fht2.CompletePending(true);
                else
                {
                    for (int i = 0; i < output.value.value.Length; i++)
                    {
                        Assert.IsTrue(output.value.value[i] == (byte)(output.value.value.Length+i));
                    }
                }
            }
            fht2.StopSession();
            fht2.Dispose();

        }
    }
}
