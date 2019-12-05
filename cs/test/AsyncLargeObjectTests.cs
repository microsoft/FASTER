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

namespace FASTER.test.async
{

    [TestFixture]
    internal class LargeObjectTests
    {
        private FasterKV<MyKey, MyLargeValue, MyInput, MyLargeOutput, Empty, MyLargeFunctions> fht1;
        private FasterKV<MyKey, MyLargeValue, MyInput, MyLargeOutput, Empty, MyLargeFunctions> fht2;
        private IDevice log, objlog;
        private string test_path;

        [SetUp]
        public void Setup()
        {
            if (test_path == null)
            {
                test_path = TestContext.CurrentContext.TestDirectory + "\\" + Path.GetRandomFileName();
                if (!Directory.Exists(test_path))
                    Directory.CreateDirectory(test_path);
            }
        }

        [TearDown]
        public void TearDown()
        {
            DeleteDirectory(test_path);
        }

        public static void DeleteDirectory(string path)
        {
            foreach (string directory in Directory.GetDirectories(path))
            {
                DeleteDirectory(directory);
            }

            try
            {
                Directory.Delete(path, true);
            }
            catch (IOException)
            {
                Directory.Delete(path, true);
            }
            catch (UnauthorizedAccessException)
            {
                Directory.Delete(path, true);
            }
        }

        [TestCase(CheckpointType.FoldOver)]
        [TestCase(CheckpointType.Snapshot)]
        public async Task LargeObjectTest(CheckpointType checkpointType)
        {
            MyInput input = default;
            MyLargeOutput output = new MyLargeOutput();

            log = Devices.CreateLogDevice(test_path + "\\LargeObjectTest.log");
            objlog = Devices.CreateLogDevice(test_path + "\\LargeObjectTest.obj.log");

            fht1 = new FasterKV<MyKey, MyLargeValue, MyInput, MyLargeOutput, Empty, MyLargeFunctions>
                (128, new MyLargeFunctions(),
                new LogSettings { LogDevice = log, ObjectLogDevice = objlog, MutableFraction = 0.1, PageSizeBits = 21, MemorySizeBits = 26 },
                new CheckpointSettings { CheckpointDir = test_path, CheckPointType = checkpointType },
                new SerializerSettings<MyKey, MyLargeValue> { keySerializer = () => new MyKeySerializer(), valueSerializer = () => new MyLargeValueSerializer() }
                );

            int maxSize = 100;
            int numOps = 5000;

            using (var s = fht1.StartClientSession())
            {
                Random r = new Random(33);
                for (int key = 0; key < numOps; key++)
                {
                    var mykey = new MyKey { key = key };
                    var value = new MyLargeValue(1 + r.Next(maxSize));
                    s.Upsert(ref mykey, ref value, Empty.Default, 0);
                }
            }

            fht1.TakeFullCheckpoint(out Guid token);
            await fht1.CompleteCheckpointAsync();

            fht1.Dispose();
            log.Close();
            objlog.Close();

            log = Devices.CreateLogDevice(test_path + "\\LargeObjectTest.log");
            objlog = Devices.CreateLogDevice(test_path + "\\LargeObjectTest.obj.log");

            fht2 = new FasterKV<MyKey, MyLargeValue, MyInput, MyLargeOutput, Empty, MyLargeFunctions>
                (128, new MyLargeFunctions(),
                new LogSettings { LogDevice = log, ObjectLogDevice = objlog, MutableFraction = 0.1, PageSizeBits = 21, MemorySizeBits = 26 },
                new CheckpointSettings { CheckpointDir = test_path, CheckPointType = checkpointType },
                new SerializerSettings<MyKey, MyLargeValue> { keySerializer = () => new MyKeySerializer(), valueSerializer = () => new MyLargeValueSerializer() }
                );

            fht2.Recover(token);
            using (var s2 = fht2.StartClientSession())
            {
                for (int keycnt = 0; keycnt < numOps; keycnt++)
                {
                    var key = new MyKey { key = keycnt };
                    var status = s2.Read(ref key, ref input, ref output, Empty.Default, 0);

                    if (status == Status.PENDING)
                        await s2.CompletePendingAsync();
                    else
                    {
                        for (int i = 0; i < output.value.value.Length; i++)
                        {
                            Assert.IsTrue(output.value.value[i] == (byte)(output.value.value.Length + i));
                        }
                    }
                }
            }

            fht2.Dispose();

            log.Close();
            objlog.Close();
        }
    }
}
