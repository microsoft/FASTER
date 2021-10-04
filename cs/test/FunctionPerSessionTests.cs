// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Threading;
using System.Threading.Tasks;
using FASTER.core;
using NUnit.Framework;

namespace FASTER.test
{
    public struct RefCountedValue
    {
        public int ReferenceCount;
        public long Value;
    }

    public class RefCountedAdder : FunctionsBase<int, RefCountedValue, long, Empty, Empty>
    {
        public int InitialCount;
        public int InPlaceCount;
        public int CopyCount;

        public override void InitialUpdater(ref int key, ref long input, ref RefCountedValue value, ref Empty output)
        {
            Interlocked.Increment(ref InitialCount);

            value.Value = input;
            value.ReferenceCount = 1;
        }

        public override bool InPlaceUpdater(ref int key, ref long input, ref RefCountedValue value, ref Empty outpu, ref RecordInfo recordInfo, long addresst)
        {
            Interlocked.Increment(ref InPlaceCount);

            value.Value = input;
            value.ReferenceCount++;
            return true;
        }

        public override void CopyUpdater(ref int key, ref long input, ref RefCountedValue oldValue, ref RefCountedValue newValue, ref Empty output, ref RecordInfo recordInfo, long address)
        {
            Interlocked.Increment(ref CopyCount);

            newValue.Value = input;
            newValue.ReferenceCount = oldValue.ReferenceCount + 1;
        }
    }

    public class RefCountedRemover : FunctionsBase<int, RefCountedValue, Empty, Empty, Empty>
    {
        public int InitialCount;
        public int InPlaceCount;
        public int CopyCount;

        public override void InitialUpdater(ref int key, ref Empty input, ref RefCountedValue value, ref Empty output)
        {
            Interlocked.Increment(ref InitialCount);

            value.Value = 0;
            value.ReferenceCount = 0;
        }

        public override bool InPlaceUpdater(ref int key, ref Empty input, ref RefCountedValue value, ref Empty output, ref RecordInfo recordInfo, long address)
        {
            Interlocked.Increment(ref InPlaceCount);

            if (value.ReferenceCount > 0)
                value.ReferenceCount--;

            return true;
        }

        public override void CopyUpdater(ref int key, ref Empty input, ref RefCountedValue oldValue, ref RefCountedValue newValue, ref Empty output, ref RecordInfo recordInfo, long address)
        {
            Interlocked.Increment(ref CopyCount);

            newValue.ReferenceCount = oldValue.ReferenceCount;
            if (newValue.ReferenceCount > 0)
                newValue.ReferenceCount--;
            newValue.Value = oldValue.Value;
        }
    }

    public class RefCountedReader : FunctionsBase<int, RefCountedValue, Empty, RefCountedValue, Empty>
    {
        public override bool SingleReader(ref int key, ref Empty input, ref RefCountedValue value, ref RefCountedValue dst, long address)
        {
            dst = value;
            return true;
        }

        public override bool ConcurrentReader(ref int key, ref Empty input, ref RefCountedValue value, ref RefCountedValue dst, ref RecordInfo recordInfo, long address)
        {
            dst = value;
            return true;
        }
    }

    [TestFixture]
    public class FunctionPerSessionTests
    {
        private IDevice _log;
        private FasterKV<int, RefCountedValue> _faster;
        private RefCountedAdder _adder;
        private RefCountedRemover _remover;
        private RefCountedReader _reader;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait:true);
            _log = Devices.CreateLogDevice(TestUtils.MethodTestDir + "/FunctionPerSessionTests1.log", deleteOnClose: true);

            _faster = new FasterKV<int, RefCountedValue>(128, new LogSettings()
            {
                LogDevice = _log,
            });

            _adder = new RefCountedAdder();
            _remover = new RefCountedRemover();
            _reader = new RefCountedReader();
        }

        [TearDown]
        public void TearDown()
        {
            _faster?.Dispose();
            _faster = null;
            _log?.Dispose();
            _log = null;
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        [Test]
        [Category("FasterKV")]
        public async Task Should_create_multiple_sessions_with_different_callbacks()
        {
            using var adderSession = _faster.NewSession(_adder);
            using var removerSession = _faster.NewSession(_remover);
            using var readerSession = _faster.NewSession(_reader);
            var key = 101;
            var input = 1000L;

            (await adderSession.RMWAsync(ref key, ref input)).Complete();
            (await adderSession.RMWAsync(ref key, ref input)).Complete();
            (await adderSession.RMWAsync(ref key, ref input)).Complete();

            Assert.AreEqual(1, _adder.InitialCount);
            Assert.AreEqual(2, _adder.InPlaceCount);

            var empty = default(Empty);
            (await removerSession.RMWAsync(ref key, ref empty)).Complete();

            Assert.AreEqual(1, _remover.InPlaceCount);

            var read = await readerSession.ReadAsync(ref key, ref empty);
            var result = read.Complete();

            var actual = result.output;
            Assert.AreEqual(2, actual.ReferenceCount);
            Assert.AreEqual(1000L, actual.Value);

            _faster.Log.FlushAndEvict(true);

            (await removerSession.RMWAsync(ref key, ref empty)).Complete();
            read = await readerSession.ReadAsync(ref key, ref empty);
            result = read.Complete();

            actual = result.output;
            Assert.AreEqual(1, actual.ReferenceCount);
            Assert.AreEqual(1000L, actual.Value);
            Assert.AreEqual(1, _remover.CopyCount);
        }
    }
}
