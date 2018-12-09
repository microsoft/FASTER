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
        private FasterKV<MyKey, MyLargeValue, MyInput, MyLargeOutput, MyContext, MyLargeFunctions> fht1;
        private FasterKV<MyKey, MyLargeValue, MyInput, MyLargeOutput, MyContext, MyLargeFunctions> fht2;
        private IDevice log, objlog;
        
        [Test]
        public void LargeObjectTest1()
        {
            MyInput input = default(MyInput);
            MyLargeOutput output = new MyLargeOutput();
            MyContext context = default(MyContext);

            log = FasterFactory.CreateLogDevice(TestContext.CurrentContext.TestDirectory + "\\hlog", deleteOnClose: true);
            objlog = FasterFactory.CreateObjectLogDevice(TestContext.CurrentContext.TestDirectory + "\\hlog", deleteOnClose: true);

            Directory.CreateDirectory(TestContext.CurrentContext.TestDirectory + "\\checkpoints");

            fht1 = new FasterKV<MyKey, MyLargeValue, MyInput, MyLargeOutput, MyContext, MyLargeFunctions>
                (128, new MyLargeFunctions(),
                new LogSettings { LogDevice = log, ObjectLogDevice = objlog, MutableFraction = 0.1, MemorySizeBits = 29 },
                new CheckpointSettings { CheckpointDir = TestContext.CurrentContext.TestDirectory + "\\checkpoints", CheckPointType = CheckpointType.Snapshot }
                );

            fht2 = new FasterKV<MyKey, MyLargeValue, MyInput, MyLargeOutput, MyContext, MyLargeFunctions>
                (128, new MyLargeFunctions(),
                logSettings: new LogSettings { LogDevice = log, ObjectLogDevice = objlog, MutableFraction = 0.1, MemorySizeBits = 29 },
                checkpointSettings: new CheckpointSettings { CheckpointDir = TestContext.CurrentContext.TestDirectory + "\\checkpoints", CheckPointType = CheckpointType.Snapshot }
                );

            int maxSize = 1000;
            int numOps = 5000;

            fht1.StartSession();
            Random r = new Random(33);
            for (int key = 0; key < numOps; key++)
            {
                var mykey = new MyKey { key = key };
                var value = new MyLargeValue(1+r.Next(maxSize));
                fht1.Upsert(ref mykey, ref value, ref context, 0);
            }
            fht1.TakeFullCheckpoint(out Guid token);
            fht1.CompleteCheckpoint(true);
            fht1.StopSession();
            fht1.Dispose();

            fht2.Recover(token);
            fht2.StartSession();
            for (int keycnt = 0; keycnt < numOps; keycnt++)
            {
                var key = new MyKey { key = keycnt };
                var status = fht2.Read(ref key, ref input, ref output, ref context, 0);

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

            log.Close();
            objlog.Close();
            new DirectoryInfo(TestContext.CurrentContext.TestDirectory + "\\checkpoints").Delete(true);
        }

        [Test]
        public void LargeObjectTest2()
        {
            MyInput input = default(MyInput);
            MyLargeOutput output = new MyLargeOutput();
            MyContext context = default(MyContext);

            log = FasterFactory.CreateLogDevice(TestContext.CurrentContext.TestDirectory + "\\hlog", deleteOnClose: true);
            objlog = FasterFactory.CreateObjectLogDevice(TestContext.CurrentContext.TestDirectory + "\\hlog", deleteOnClose: true);

            Directory.CreateDirectory(TestContext.CurrentContext.TestDirectory + "\\checkpoints");

            fht1 = new FasterKV<MyKey, MyLargeValue, MyInput, MyLargeOutput, MyContext, MyLargeFunctions>
                (128, new MyLargeFunctions(),
                new LogSettings { LogDevice = log, ObjectLogDevice = objlog, MutableFraction = 0.1, MemorySizeBits = 29 },
                new CheckpointSettings { CheckpointDir = TestContext.CurrentContext.TestDirectory + "\\checkpoints", CheckPointType = CheckpointType.FoldOver }
                );

            fht2 = new FasterKV<MyKey, MyLargeValue, MyInput, MyLargeOutput, MyContext, MyLargeFunctions>
                (128, new MyLargeFunctions(),
                new LogSettings { LogDevice = log, ObjectLogDevice = objlog, MutableFraction = 0.1, MemorySizeBits = 29 },
                new CheckpointSettings { CheckpointDir = TestContext.CurrentContext.TestDirectory + "\\checkpoints", CheckPointType = CheckpointType.FoldOver }
                );

            int maxSize = 1000;
            int numOps = 5000;

            fht1.StartSession();
            Random r = new Random(33);
            for (int key = 0; key < numOps; key++)
            {
                var mykey = new MyKey { key = key };
                var value = new MyLargeValue(1 + r.Next(maxSize));
                fht1.Upsert(ref mykey, ref value, ref context, 0);
            }
            fht1.TakeFullCheckpoint(out Guid token);
            fht1.CompleteCheckpoint(true);
            fht1.StopSession();
            fht1.Dispose();

            fht2.Recover(token);
            fht2.StartSession();
            for (int keycnt = 0; keycnt < numOps; keycnt++)
            {
                var key = new MyKey { key = keycnt };
                var status = fht2.Read(ref key, ref input, ref output, ref context, 0);

                if (status == Status.PENDING)
                    fht2.CompletePending(true);
                else
                {
                    for (int i = 0; i < output.value.value.Length; i++)
                    {
                        Assert.IsTrue(output.value.value[i] == (byte)(output.value.value.Length + i));
                    }
                }
            }
            fht2.StopSession();
            fht2.Dispose();

            log.Close();
            objlog.Close();
            new DirectoryInfo(TestContext.CurrentContext.TestDirectory + "\\checkpoints").Delete(true);
        }
    }
}
