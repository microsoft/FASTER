// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using NUnit.Framework;
using System;
using System.Linq;

namespace FASTER.test
{

    [TestFixture]
    internal class GenericByteArrayTests
    {
        private FasterKV<byte[], byte[]> fht;
        private ClientSession<byte[], byte[], byte[], byte[], Empty, MyByteArrayFuncs> session;
        private IDevice log, objlog;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait:true);
            log = Devices.CreateLogDevice(TestUtils.MethodTestDir + "/GenericStringTests.log", deleteOnClose: true);
            objlog = Devices.CreateLogDevice(TestUtils.MethodTestDir + "/GenericStringTests.obj.log", deleteOnClose: true);

            fht = new FasterKV<byte[], byte[]>(
                    1L << 20, // size of hash table in #cache lines; 64 bytes per cache line
                    new LogSettings { LogDevice = log, ObjectLogDevice = objlog, MutableFraction = 0.1, MemorySizeBits = 14, PageSizeBits = 9 }, // log device
                    comparer: new ByteArrayEC()
                    );

            session = fht.For(new MyByteArrayFuncs()).NewSession<MyByteArrayFuncs>();
        }

        [TearDown]
        public void TearDown()
        {
            session?.Dispose();
            session = null;
            fht?.Dispose();
            fht = null;
            log?.Dispose();
            log = null;
            objlog?.Dispose();
            objlog = null;

            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        private byte[] GetByteArray(int i)
        {
            return BitConverter.GetBytes(i);
        }

        [Test]
        [Category("FasterKV")]
        [Category("Smoke")]
        public void ByteArrayBasicTest()
        {
            const int totalRecords = 2000;
            for (int i = 0; i < totalRecords; i++)
            {
                var _key = GetByteArray(i);
                var _value = GetByteArray(i);
                session.Upsert(ref _key, ref _value, Empty.Default, 0);
            }
            session.CompletePending(true);

            for (int i = 0; i < totalRecords; i++)
            {
                byte[] input = default;
                byte[] output = default;
                var key = GetByteArray(i);
                var value = GetByteArray(i);

                if (session.Read(ref key, ref input, ref output, Empty.Default, 0) == Status.PENDING)
                {
                    session.CompletePending(true);
                }
                else
                {
                    Assert.IsTrue(output.SequenceEqual(value));
                }
            }
        }

        class MyByteArrayFuncs : SimpleFunctions<byte[], byte[]>
        {
            public override void ReadCompletionCallback(ref byte[] key, ref byte[] input, ref byte[] output, Empty ctx, Status status, RecordMetadata recordMetadata)
            {
                Assert.IsTrue(output.SequenceEqual(key));
            }
        }
    }

    class ByteArrayEC : IFasterEqualityComparer<byte[]>
    {
        public bool Equals(ref byte[] k1, ref byte[] k2)
        {
            return k1.SequenceEqual(k2);
        }

        public unsafe long GetHashCode64(ref byte[] k)
        {
            fixed (byte *b = k)
            {
                return Utility.HashBytes(b, k.Length);
            }
        }
    }
}
