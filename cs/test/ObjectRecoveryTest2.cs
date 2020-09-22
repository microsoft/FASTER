// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;
using System.IO;
using NUnit.Framework;

namespace FASTER.test.recovery.objects
{

    [TestFixture]
    public class ObjectRecoveryTest
    {
        int iterations;
        string FasterFolderPath { get; set; }

        [SetUp]
        public void Setup()
        {
            FasterFolderPath = TestContext.CurrentContext.TestDirectory + "\\" + Path.GetRandomFileName();
            if (!Directory.Exists(FasterFolderPath))
                Directory.CreateDirectory(FasterFolderPath);
        }

        [TearDown]
        public void TearDown()
        {
            DeleteDirectory(FasterFolderPath);
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


        [Test]
        public void ObjectRecoveryTest1(
            [Values]CheckpointType checkpointType,
            [Range(100, 1500, 600)] int iterations)
        {
            this.iterations = iterations;
            Prepare(checkpointType, out _, out _, out IDevice log, out IDevice objlog, out FasterKV<MyKey, MyValue> h, out MyContext context);

            var session1 = h.For(new MyFunctions()).NewSession<MyFunctions>();
            Write(session1, context, h);
            Read(session1, context, false);
            session1.Dispose();

            h.TakeFullCheckpoint(out _);
            h.CompleteCheckpointAsync().GetAwaiter().GetResult();

            Destroy(log, objlog, h);

            Prepare(checkpointType, out _, out _, out log, out objlog, out h, out context);

            h.Recover();

            var session2 = h.For(new MyFunctions()).NewSession<MyFunctions>();
            Read(session2, context, true);
            session2.Dispose();

            Destroy(log, objlog, h);
        }

        private void Prepare(CheckpointType checkpointType, out string logPath, out string objPath, out IDevice log, out IDevice objlog, out FasterKV<MyKey, MyValue> h, out MyContext context)
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
                    CheckpointDir = Path.Combine(FasterFolderPath, "check-points"),
                    CheckPointType = checkpointType
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

        private void Write(ClientSession<MyKey, MyValue, MyInput, MyOutput, MyContext, MyFunctions> session, MyContext context, FasterKV<MyKey, MyValue> fht)
        {
            for (int i = 0; i < iterations; i++)
            {
                var _key = new MyKey { key = i, name = i.ToString() };
                var value = new MyValue { value = i.ToString() };
                session.Upsert(ref _key, ref value, context, 0);

                if (i % 100 == 0)
                {
                    fht.TakeFullCheckpoint(out _);
                    fht.CompleteCheckpointAsync().GetAwaiter().GetResult();
                }
            }
        }

        private void Read(ClientSession<MyKey, MyValue, MyInput, MyOutput, MyContext, MyFunctions> session, MyContext context, bool delete)
        {
            for (int i = 0; i < iterations; i++)
            {
                var key = new MyKey { key = i, name = i.ToString() };
                var input = default(MyInput);
                MyOutput g1 = new MyOutput();
                var status = session.Read(ref key, ref input, ref g1, context, 0);

                if (status == Status.PENDING)
                {
                    session.CompletePending(true);
                    context.FinalizeRead(ref status, ref g1);
                }

                Assert.IsTrue(status == Status.OK);
                Assert.IsTrue(g1.value.value == i.ToString());
            }

            if (delete)
            {
                var key = new MyKey { key = 1, name = "1" };
                var input = default(MyInput);
                var output = new MyOutput();
                session.Delete(ref key, context, 0);
                var status = session.Read(ref key, ref input, ref output, context, 0);

                if (status == Status.PENDING)
                {
                    session.CompletePending(true);
                    context.FinalizeRead(ref status, ref output);
                }

                Assert.IsTrue(status == Status.NOTFOUND);
            }
        }
    }

    public class MyKeySerializer : BinaryObjectSerializer<MyKey>
    {
        public override void Serialize(ref MyKey key)
        {
            var bytes = System.Text.Encoding.UTF8.GetBytes(key.name);
            writer.Write(4 + bytes.Length);
            writer.Write(key.key);
            writer.Write(bytes);
        }

        public override void Deserialize(out MyKey key)
        {
            key = new MyKey();
            var size = reader.ReadInt32();
            key.key = reader.ReadInt32();
            var bytes = new byte[size - 4];
            reader.Read(bytes, 0, size - 4);
            key.name = System.Text.Encoding.UTF8.GetString(bytes);

        }
    }

    public class MyValueSerializer : BinaryObjectSerializer<MyValue>
    {
        public override void Serialize(ref MyValue value)
        {
            var bytes = System.Text.Encoding.UTF8.GetBytes(value.value);
            writer.Write(bytes.Length);
            writer.Write(bytes);
        }

        public override void Deserialize(out MyValue value)
        {
            value = new MyValue();
            var size = reader.ReadInt32();
            var bytes = new byte[size];
            reader.Read(bytes, 0, size);
            value.value = System.Text.Encoding.UTF8.GetString(bytes);
        }
    }

    public class MyKey : IFasterEqualityComparer<MyKey>
    {
        public int key;
        public string name;

        public long GetHashCode64(ref MyKey key) => Utility.GetHashCode(key.key);
        public bool Equals(ref MyKey key1, ref MyKey key2) => key1.key == key2.key && key1.name == key2.name;
    }


    public class MyValue { public string value; }
    public class MyInput { public string value; }
    public class MyOutput { public MyValue value; }

    public class MyContext
    {
        private Status _status;
        private MyOutput _g1;

        internal void Populate(ref Status status, ref MyOutput g1)
        {
            _status = status;
            _g1 = g1;
        }
        internal void FinalizeRead(ref Status status, ref MyOutput g1)
        {
            status = _status;
            g1 = _g1;
        }
    }


    public class MyFunctions : IFunctions<MyKey, MyValue, MyInput, MyOutput, MyContext>
    {
        public void InitialUpdater(ref MyKey key, ref MyInput input, ref MyValue value, ref MyContext ctx) => value.value = input.value;
        public void CopyUpdater(ref MyKey key, ref MyInput input, ref MyValue oldValue, ref MyValue newValue, ref MyContext ctx) => newValue = oldValue;
        public bool InPlaceUpdater(ref MyKey key, ref MyInput input, ref MyValue value, ref MyContext ctx)
        {
            if (value.value.Length < input.value.Length)
                return false;
            value.value = input.value;
            return true;
        }


        public void SingleReader(ref MyKey key, ref MyInput input, ref MyValue value, ref MyOutput dst, ref MyContext ctx) => dst.value = value;
        public void SingleWriter(ref MyKey key, ref MyValue src, ref MyValue dst, ref MyContext ctx) => dst = src;
        public void ConcurrentReader(ref MyKey key, ref MyInput input, ref MyValue value, ref MyOutput dst, ref MyContext ctx) => dst.value = value;
        public bool ConcurrentWriter(ref MyKey key, ref MyValue src, ref MyValue dst, ref MyContext ctx)
        {
            if (src == null)
                return false;

            if (dst.value.Length != src.value.Length)
                return false;

            dst = src;
            return true;
        }

        public void ReadCompletionCallback(ref MyKey key, ref MyInput input, ref MyOutput output, MyContext ctx, Status status) => ctx.Populate(ref status, ref output);
        public void UpsertCompletionCallback(ref MyKey key, ref MyValue value, MyContext ctx) { }
        public void RMWCompletionCallback(ref MyKey key, ref MyInput input, MyContext ctx, Status status) { }
        public void DeleteCompletionCallback(ref MyKey key, MyContext ctx) { }
        public void CheckpointCompletionCallback(string sessionId, CommitPoint commitPoint) { }
    }
}
