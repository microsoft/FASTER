// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;
using System.IO;
using NUnit.Framework;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace FASTER.test.recovery.objects
{
    [TestFixture]
    public class ObjectRecoveryTests3
    {
        int iterations;
        string FasterFolderPath { get; set; }

        [SetUp]
        public void Setup()
        {
            FasterFolderPath = TestUtils.MethodTestDir;
            TestUtils.RecreateDirectory(FasterFolderPath);
        }

        [TearDown]
        public void TearDown()
        {
            TestUtils.DeleteDirectory(FasterFolderPath);
        }

        [Test]
        [Category("FasterKV"), Category("CheckpointRestore")]
        public async ValueTask ObjectRecoveryTest3(
            [Values]CheckpointType checkpointType,
            [Values(1000)] int iterations,
            [Values]bool isAsync)
        {
            this.iterations = iterations;
            Prepare(out _, out _, out IDevice log, out IDevice objlog, out FasterKV<MyKey, MyValue> h, out MyContext context);

            var session1 = h.For(new MyFunctions()).NewSession<MyFunctions>();
            var tokens = Write(session1, context, h, checkpointType);
            Read(session1, context, false, iterations);
            session1.Dispose();

            h.TryInitiateHybridLogCheckpoint(out Guid token, checkpointType);
            h.CompleteCheckpointAsync().AsTask().GetAwaiter().GetResult();
            tokens.Add((iterations, token));
            Destroy(log, objlog, h);

            foreach (var item in tokens)
            {
                Prepare(out _, out _, out log, out objlog, out h, out context);

                if (isAsync)
                    await h.RecoverAsync(default, item.Item2);
                else
                    h.Recover(default, item.Item2);

                var session2 = h.For(new MyFunctions()).NewSession<MyFunctions>();
                Read(session2, context, false, item.Item1);
                session2.Dispose();

                Destroy(log, objlog, h);
            }
        }

        private void Prepare(out string logPath, out string objPath, out IDevice log, out IDevice objlog, out FasterKV<MyKey, MyValue> h, out MyContext context)
        {
            logPath = Path.Combine(FasterFolderPath, $"FasterRecoverTests.log");
            objPath = Path.Combine(FasterFolderPath, $"FasterRecoverTests_HEAP.log");
            log = Devices.CreateLogDevice(logPath);
            objlog = Devices.CreateLogDevice(objPath);
            h = new FasterKV
                <MyKey, MyValue>
                (1L << 20,
                new LogSettings
                {
                    LogDevice = log,
                    ObjectLogDevice = objlog,
                    SegmentSizeBits = 12,
                    MemorySizeBits = 12,
                    PageSizeBits = 9
                },
                new CheckpointSettings()
                {
                    CheckpointDir = Path.Combine(FasterFolderPath, "check-points")
                },
                new SerializerSettings<MyKey, MyValue> { keySerializer = () => new MyKeySerializer(), valueSerializer = () => new MyValueSerializer() }
             );
            context = new MyContext();
        }

        private static void Destroy(IDevice log, IDevice objlog, FasterKV<MyKey, MyValue> h)
        {
            // Dispose FASTER instance and log
            h.Dispose();
            log.Dispose();
            objlog.Dispose();
        }

        private List<(int, Guid)> Write(ClientSession<MyKey, MyValue, MyInput, MyOutput, MyContext, MyFunctions, DefaultStoreFunctions<MyKey, MyValue>> session,
                                        MyContext context, FasterKV<MyKey, MyValue> fht, CheckpointType checkpointType)
        {
            var tokens = new List<(int, Guid)>();
            for (int i = 0; i < iterations; i++)
            {
                var _key = new MyKey { key = i, name = string.Concat(Enumerable.Repeat(i.ToString(), 100)) };
                var value = new MyValue { value = i.ToString() };
                session.Upsert(ref _key, ref value, context, 0);

                if (i % 1000 == 0 && i > 0)
                {
                    fht.TryInitiateHybridLogCheckpoint(out Guid token, checkpointType);
                    fht.CompleteCheckpointAsync().AsTask().GetAwaiter().GetResult();
                    tokens.Add((i, token));
                }
            }
            return tokens;
        }

        private void Read(ClientSession<MyKey, MyValue, MyInput, MyOutput, MyContext, MyFunctions, DefaultStoreFunctions<MyKey, MyValue>> session,
                        MyContext context, bool delete, int iter)
        {
            for (int i = 0; i < iter; i++)
            {
                var key = new MyKey { key = i, name = string.Concat(Enumerable.Repeat(i.ToString(), 100)) };
                MyInput input = default;
                MyOutput g1 = new();
                var status = session.Read(ref key, ref input, ref g1, context, 0);

                if (status.IsPending)
                {
                    session.CompletePending(true);
                    context.FinalizeRead(ref status, ref g1);
                }

                Assert.IsTrue(status.Found);
                Assert.AreEqual(i.ToString(), g1.value.value);
            }

            if (delete)
            {
                var key = new MyKey { key = 1, name = "1" };
                var input = default(MyInput);
                var output = new MyOutput();
                session.Delete(ref key, context, 0);
                var status = session.Read(ref key, ref input, ref output, context, 0);

                if (status.IsPending)
                {
                    session.CompletePending(true);
                    context.FinalizeRead(ref status, ref output);
                }

                Assert.IsFalse(status.Found);
            }
        }
    }
}
