// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using FASTER.core;
using NuGet.Frameworks;
using NUnit.Framework;
using static FASTER.test.TestUtils;

namespace FASTER.test.Dispose
{
    [TestFixture]
    internal class DisposeTests
    {
        // MyKey and MyValue are classes; we want to be sure we are getting the right Keys and Values to Dispose().
        private FasterKV<MyKey, MyValue, DisposeTestStoreFunctions> fht;
        private IDevice log, objlog;
        private DisposeTestStoreFunctions storeFunctions;

        // Events to coordinate forcing CAS failure (by appending a new item), etc.
        private SemaphoreSlim sutGate;      // Session Under Test
        private SemaphoreSlim otherGate;    // Other session that inserts a colliding value

        [SetUp]
        public void Setup()
        {
            DeleteDirectory(MethodTestDir, wait: true);

            sutGate = new(0);
            otherGate = new(0);

            log = Devices.CreateLogDevice(MethodTestDir + "/ObjectFASTERTests.log", deleteOnClose: true);
            objlog = Devices.CreateLogDevice(MethodTestDir + "/ObjectFASTERTests.obj.log", deleteOnClose: true);
            storeFunctions = new DisposeTestStoreFunctions();

            LogSettings logSettings = new () { LogDevice = log, ObjectLogDevice = objlog, MutableFraction = 0.1, MemorySizeBits = 15, PageSizeBits = 10 };
            foreach (var arg in TestContext.CurrentContext.Test.Arguments)
            {
                if (arg is ReadCopyDestination dest)
                {
                    if (dest == ReadCopyDestination.ReadCache)
                        logSettings.ReadCacheSettings = new() { PageSizeBits = 12, MemorySizeBits = 22 };
                    break;
                }
            }

            fht = new FasterKV<MyKey, MyValue, DisposeTestStoreFunctions>(128, logSettings: logSettings, storeFunctions, comparer: new MyKeyComparer(),
                serializerSettings: new SerializerSettings<MyKey, MyValue> { keySerializer = () => new MyKeySerializer(), valueSerializer = () => new MyValueSerializer() }
                );
        }

        [TearDown]
        public void TearDown()
        {
            fht?.Dispose();
            fht = null;
            log?.Dispose();
            log = null;
            objlog?.Dispose();
            objlog = null;
            DeleteDirectory(MethodTestDir);
        }

        class DisposeTestStoreFunctions : IStoreFunctions<MyKey, MyValue>
        {
            internal ConcurrentQueue<DisposeReason> handlerQueue = new();

            public bool DisposeOnPageEviction => true;

            public void Dispose(ref MyKey key, ref MyValue value, DisposeReason reason)
            {
                Assert.AreNotEqual(DisposeReason.None, reason);
                VerifyKeyValueCombo(ref key, ref value, reason);
                handlerQueue.Enqueue(reason);
            }

            internal DisposeReason Dequeue()
            {
                Assert.IsTrue(handlerQueue.TryDequeue(out DisposeReason reason));
                return reason;
            }
        }

        // This is passed to the FasterKV ctor to override the default one. This lets us use a different key for the colliding
        // CAS; we can't use the same key because Readonly-region handling in the first session Seals the to-be-transferred record,
        // so the second session would loop forever while the first session waits for the collision to be written.
        class MyKeyComparer : IFasterEqualityComparer<MyKey>
        {
            public long GetHashCode64(ref MyKey key) => Utility.GetHashCode(key.key % TestKey);
            
            public bool Equals(ref MyKey k1, ref MyKey k2) => k1.key == k2.key;
        }

        const int TestKey = 111;
        const int TestCollidingKey = TestKey * 2;
        const int TestCollidingKey2 = TestKey * 3;
        const int TestInitialValue = 3333;
        const int TestUpdatedValue = 5555;
        const int TestCollidingValue = 7777;
        const int TestCollidingValue2 = 9999;

        public class DisposeFunctions : FunctionsBase<MyKey, MyValue, MyInput, MyOutput, Empty>
        {
            private readonly DisposeTests tester;
            internal readonly bool isSUT; // IsSessionUnderTest
            private bool isRetry;
            private readonly bool isSplice;
            bool isPendingInitialUpdate;

            internal DisposeFunctions(DisposeTests tester, bool sut, bool splice = false, bool pendingInitialUpdate = false)
            {
                this.tester = tester;
                isSUT = sut;
                isSplice = splice;
                isPendingInitialUpdate = pendingInitialUpdate;
            }

            void WaitForEvent()
            {
                if (isSUT)
                {
                    MyKey key = new() { key = TestKey };
                    tester.fht.FindKey(ref key, out var entry);
                    var address = entry.Address;
                    if (isSplice)
                    {
                        // There should be one readcache entry for this test.
                        Assert.IsTrue(new HashBucketEntry() { word = entry.Address }.ReadCache);
                        Assert.GreaterOrEqual(address, tester.fht.ReadCache.BeginAddress);
                        var physicalAddress = tester.fht.readcache.GetPhysicalAddress(entry.Address & ~Constants.kReadCacheBitMask);
                        ref RecordInfo recordInfo = ref tester.fht.readcache.GetInfo(physicalAddress);
                        address = recordInfo.PreviousAddress;

                        // There should be only one readcache entry for this test.
                        Assert.IsFalse(new HashBucketEntry() { word = address }.ReadCache);
                    }
                    tester.otherGate.Release();
                    tester.sutGate.Wait();

                    // There's a little race where the SUT session could still beat the other session to the CAS
                    if (!isRetry)
                    {
                        if (!isSplice)
                        {
                            while (entry.Address == address)
                            {
                                Thread.Yield();
                                tester.fht.FindKey(ref key, out entry);
                            }
                        }
                        else
                        {
                            var physicalAddress = tester.fht.readcache.GetPhysicalAddress(entry.Address & ~Constants.kReadCacheBitMask);
                            ref RecordInfo recordInfo = ref tester.fht.readcache.GetInfo(physicalAddress);
                            while (recordInfo.PreviousAddress == address)
                            {
                                // Wait for the splice to happen
                                Thread.Yield();
                            }
                        }
                    }
                    isRetry = true;     // the next call will be from RETRY_NOW
                }
            }

            void SignalEvent()
            {
                // Let the SUT proceed, which will trigger a RETRY_NOW due to the failed CAS, so we need to release for the second wait as well.
                if (!isSUT)
                    tester.sutGate.Release(2);
            }

            public override bool SingleWriter(ref MyKey key, ref MyInput input, ref MyValue src, ref MyValue dst, ref MyOutput output, ref UpsertInfo upsertInfo, WriteReason reason)
            {
                WaitForEvent();
                dst = src;
                SignalEvent();
                return true;
            }

            public override bool InitialUpdater(ref MyKey key, ref MyInput input, ref MyValue value, ref MyOutput output, ref RMWInfo rmwInfo)
            {
                WaitForEvent();
                value = new MyValue { value = input.value };
                SignalEvent();
                return true;
            }

            public override bool CopyUpdater(ref MyKey key, ref MyInput input, ref MyValue oldValue, ref MyValue newValue, ref MyOutput output, ref RMWInfo rmwInfo)
            {
                WaitForEvent();
                newValue = new MyValue { value = oldValue.value + input.value };
                SignalEvent();
                return true;
            }

            public override bool SingleDeleter(ref MyKey key, ref MyValue value, ref DeleteInfo deleteInfo)
            {
                WaitForEvent();
                base.SingleDeleter(ref key, ref value, ref deleteInfo);
                SignalEvent();
                return true;
            }

            public override bool InPlaceUpdater(ref MyKey key, ref MyInput input, ref MyValue value, ref MyOutput output, ref RMWInfo rmwInfo)
            {
                value.value += input.value;
                return true;
            }

            public override bool NeedCopyUpdate(ref MyKey key, ref MyInput input, ref MyValue oldValue, ref MyOutput output, ref RMWInfo rmwInfo) => true;

            public override bool ConcurrentReader(ref MyKey key, ref MyInput input, ref MyValue value, ref MyOutput dst, ref ReadInfo readInfo)
            {
                Assert.Fail("ConcurrentReader should not be called for this test");
                return true;
            }

            public override bool ConcurrentWriter(ref MyKey key, ref MyInput input, ref MyValue src, ref MyValue dst, ref MyOutput output, ref UpsertInfo upsertInfo)
            {
                dst.value = src.value;
                return true;
            }

            public override void RMWCompletionCallback(ref MyKey key, ref MyInput input, ref MyOutput output, Empty ctx, Status status, RecordMetadata recordMetadata)
            {
                if (isSUT && !isPendingInitialUpdate)
                {
                    Assert.IsTrue(status.Found, status.ToString());
                    Assert.IsTrue(status.Record.CopyUpdated, status.ToString());
                }
                else
                {
                    Assert.IsTrue(status.NotFound, status.ToString());
                    Assert.IsTrue(status.Record.Created, status.ToString());
                }
            }

            public override bool SingleReader(ref MyKey key, ref MyInput input, ref MyValue value, ref MyOutput dst, ref ReadInfo readInfo)
            {
                dst.value = value;
                return true;
            }
        }

        static void VerifyKeyValueCombo(ref MyKey key, ref MyValue value, DisposeReason reason)
        {
            if (reason == DisposeReason.SingleDeleterCASFailed)
                return; // no value for Delete
            switch (key.key)
            {
                case TestKey:
                    if (reason == DisposeReason.CopyUpdaterCASFailed)
                        Assert.AreEqual(TestInitialValue + TestUpdatedValue, value.value);
                    else
                        Assert.AreEqual(TestInitialValue, value.value);
                    break;
                case TestCollidingKey:
                    Assert.AreEqual(TestCollidingValue, value.value);
                    break;
                case TestCollidingKey2:
                    Assert.AreEqual(TestCollidingValue2, value.value);
                    break;
                default:
                    Assert.Fail($"Unexpected key: {key.key}");
                    break;
            }
        }

        // Override some things from MyFunctions for our purposes here
        class DisposeFunctionsNoSync : MyFunctions
        {
            public override bool CopyUpdater(ref MyKey key, ref MyInput input, ref MyValue oldValue, ref MyValue newValue, ref MyOutput output, ref RMWInfo rmwInfo)
            {
                newValue = new MyValue { value = oldValue.value + input.value };
                output.value = newValue;
                return true;
            }

            public override void ReadCompletionCallback(ref MyKey key, ref MyInput input, ref MyOutput output, Empty ctx, Status status, RecordMetadata recordMetadata)
            {
                Assert.IsTrue(status.Found);
            }
        }

        void DoFlush(FlushMode flushMode)
        {
            switch (flushMode)
            {
                case FlushMode.NoFlush:
                    return;
                case FlushMode.ReadOnly:
                    fht.Log.ShiftReadOnlyAddress(fht.Log.TailAddress, wait: true);
                    return;
                case FlushMode.OnDisk:
                    fht.Log.FlushAndEvict(wait: true);
                    return;
            }
        }

        [Test]
        [Category("FasterKV")]
        [Category("Smoke")]
        public void DisposeSingleWriterTest()
        {
            var functions1 = new DisposeFunctions(this, sut: true);
            var functions2 = new DisposeFunctions(this, sut: false);

            MyKey key = new() { key = TestKey };
            MyKey collidingKey = new() { key = TestCollidingKey };
            MyValue value = new() { value = TestInitialValue };
            MyValue collidingValue = new() { value = TestCollidingValue };

            void DoUpsert(DisposeFunctions functions)
            {
                using var innerSession = fht.NewSession(functions);
                if (functions.isSUT)
                    innerSession.Upsert(ref key, ref value);
                else
                {
                    otherGate.Wait();
                    innerSession.Upsert(ref collidingKey, ref collidingValue);
                }
            }

            var tasks = new[]
            {
                Task.Factory.StartNew(() => DoUpsert(functions1)),
                Task.Factory.StartNew(() => DoUpsert(functions2))
            };

            Task.WaitAll(tasks);

            Assert.AreEqual(DisposeReason.SingleWriterCASFailed, storeFunctions.Dequeue());
            Assert.IsEmpty(storeFunctions.handlerQueue);
        }

        [Test]
        [Category("FasterKV")]
        [Category("Smoke")]
        public void DisposeInitialUpdaterTest([Values(FlushMode.NoFlush, FlushMode.OnDisk)] FlushMode flushMode)
        {
            var functions1 = new DisposeFunctions(this, sut: true, pendingInitialUpdate: flushMode == FlushMode.OnDisk);
            var functions2 = new DisposeFunctions(this, sut: false);

            Status status;

            if (flushMode == FlushMode.OnDisk)
            {
                // Use colliding2 so the insert will still use InitialUpdater (not CopyUpdateR)
                using var session = fht.NewSession(new DisposeFunctionsNoSync());
                MyKey collidingKey2 = new() { key = TestCollidingKey2 };
                MyInput collidingInput2 = new() { value = TestCollidingValue2 };
                status = session.RMW(ref collidingKey2, ref collidingInput2);
                Assert.IsTrue(status.Record.Created);

                // Make it immutable so CopyUpdater is called.
                DoFlush(flushMode);
                Assert.AreEqual(DisposeReason.PageEviction, storeFunctions.Dequeue());
                Assert.IsEmpty(storeFunctions.handlerQueue);
            }

            void DoInsert(DisposeFunctions functions)
            {
                using var session = fht.NewSession(functions);
                if (functions.isSUT)
                {
                    MyKey key = new() { key = TestKey };
                    MyInput input = new() { value = TestInitialValue };
                    status = session.RMW(ref key, ref input);

                    if (flushMode == FlushMode.OnDisk)
                    {
                        Assert.IsTrue(status.IsPending, status.ToString());
                        session.CompletePendingWithOutputs(out var completedOutputs, wait: true);
                        (status, _) = GetSinglePendingResult(completedOutputs);
                    }
                }
                else
                {
                    otherGate.Wait();

                    MyKey collidingKey = new() { key = TestCollidingKey };
                    MyValue collidingValue = new() { value = TestCollidingValue };
                    status = session.Upsert(ref collidingKey, ref collidingValue);
                }
                Assert.IsTrue(status.Record.Created);
            }

            var tasks = new[]
            {
                Task.Factory.StartNew(() => DoInsert(functions1)),
                Task.Factory.StartNew(() => DoInsert(functions2))
            };
            Task.WaitAll(tasks);

            Assert.AreEqual(DisposeReason.InitialUpdaterCASFailed, storeFunctions.Dequeue());
            if (flushMode == FlushMode.OnDisk)
                Assert.AreEqual(DisposeReason.DeserializedFromDisk, storeFunctions.Dequeue());
            Assert.IsEmpty(storeFunctions.handlerQueue);
        }

        [Test]
        [Category("FasterKV")]
        [Category("Smoke")]
        public void DisposeCopyUpdaterTest([Values(FlushMode.ReadOnly, FlushMode.OnDisk)] FlushMode flushMode)
        {
            var functions1 = new DisposeFunctions(this, sut: true);
            var functions2 = new DisposeFunctions(this, sut: false);

            MyKey key = new() { key = TestKey };
            {
                using var session = fht.NewSession(new DisposeFunctionsNoSync());
                MyValue value = new() { value = TestInitialValue };
                session.Upsert(ref key, ref value);
            }

            // Make it immutable so CopyUpdater is called.
            DoFlush(flushMode);
            if (flushMode == FlushMode.OnDisk)
            {
                Assert.AreEqual(DisposeReason.PageEviction, storeFunctions.Dequeue());
                Assert.IsEmpty(storeFunctions.handlerQueue);
            }

            void DoUpdate(DisposeFunctions functions)
            {
                using var session = fht.NewSession(functions);
                if (functions.isSUT)
                {
                    MyInput input = new() { value = TestUpdatedValue };
                    session.RMW(ref key, ref input);
                }
                else
                {
                    otherGate.Wait();
                    MyKey collidingKey = new() { key = TestCollidingKey };
                    MyValue collidingValue = new() { value = TestCollidingValue };
                    session.Upsert(ref collidingKey, ref collidingValue);
                }
            }

            var tasks = new[]
            {
                Task.Factory.StartNew(() => DoUpdate(functions1)),
                Task.Factory.StartNew(() => DoUpdate(functions2))
            };
            Task.WaitAll(tasks);

            Assert.AreEqual(DisposeReason.CopyUpdaterCASFailed, storeFunctions.Dequeue());
            if (flushMode == FlushMode.OnDisk)
                Assert.AreEqual(DisposeReason.DeserializedFromDisk, storeFunctions.Dequeue());
            Assert.IsEmpty(storeFunctions.handlerQueue);
        }

        [Test]
        [Category("FasterKV")]
        [Category("Smoke")]
        public void DisposeSingleDeleterTest([Values(FlushMode.ReadOnly, FlushMode.OnDisk)] FlushMode flushMode)
        {
            var functions1 = new DisposeFunctions(this, sut: true);
            var functions2 = new DisposeFunctions(this, sut: false);

            MyKey key = new() { key = TestKey };
            MyKey collidingKey = new() { key = TestCollidingKey };

            {
                using var session = fht.NewSession(new DisposeFunctionsNoSync());
                MyValue value = new() { value = TestInitialValue };
                session.Upsert(ref key, ref value);
                MyValue collidingValue = new() { value = TestCollidingValue };
                session.Upsert(ref collidingKey, ref collidingValue);
            }

            // Make it immutable so we don't simply set Tombstone.
            DoFlush(flushMode);
            if (flushMode == FlushMode.OnDisk)
            {
                // Two records were upserted.
                Assert.AreEqual(DisposeReason.PageEviction, storeFunctions.Dequeue());
                Assert.AreEqual(DisposeReason.PageEviction, storeFunctions.Dequeue());
                Assert.IsEmpty(storeFunctions.handlerQueue);
            }

            // This is necessary for FlushMode.ReadOnly to test the readonly range in Delete() (otherwise we can't test SingleDeleter there)
            using var luc = fht.NewSession(new DisposeFunctionsNoSync()).GetLockableUnsafeContext();

            void DoDelete(DisposeFunctions functions)
            {
                using var innerSession = fht.NewSession(functions);
                if (functions.isSUT)
                    innerSession.Delete(ref key);
                else
                {
                    otherGate.Wait();
                    innerSession.Delete(ref collidingKey);
                }
            }

            var tasks = new[]
            {
                Task.Factory.StartNew(() => DoDelete(functions1)),
                Task.Factory.StartNew(() => DoDelete(functions2))
            };

            Task.WaitAll(tasks);

            Assert.AreEqual(DisposeReason.SingleDeleterCASFailed, storeFunctions.Dequeue());
            Assert.IsEmpty(storeFunctions.handlerQueue);
        }

        [Test]
        [Category("FasterKV")]
        [Category("Smoke")]
        public void DisposePendingReadTest([Values] ReadCopyDestination copyDest)
        {
            DoPendingReadInsertTest(copyDest, initialReadCacheInsert: false);
        }

        [Test]
        [Category("FasterKV")]
        [Category("Smoke")]
        public void DisposeCopyToTailWithInitialReadCacheTest([Values(ReadCopyDestination.ReadCache)] ReadCopyDestination _)
        {
            // We use the ReadCopyDestination.ReadCache parameter so Setup() knows to set up the readcache, but
            // for the actual test it is used only for setup; we execute CopyToTail.
            DoPendingReadInsertTest(ReadCopyDestination.Tail, initialReadCacheInsert: true);
        }

        void DoPendingReadInsertTest(ReadCopyDestination copyDest, bool initialReadCacheInsert)
        {
            var functions1 = new DisposeFunctions(this, sut: true, splice: initialReadCacheInsert);
            var functions2 = new DisposeFunctions(this, sut: false);

            MyKey key = new() { key = TestKey };
            MyKey collidingKey2 = new() { key = TestCollidingKey2 };
            MyValue collidingValue2 = new() { value = TestCollidingValue2 };

            // Do initial insert(s) to set things up
            {
                using var session = fht.NewSession(new DisposeFunctionsNoSync());
                MyValue value = new() { value = TestInitialValue };
                session.Upsert(ref key, ref value);
                if (initialReadCacheInsert)
                    session.Upsert(ref collidingKey2, ref collidingValue2);
            }

            // FlushAndEvict so we go pending
            DoFlush(FlushMode.OnDisk);
            Assert.AreEqual(DisposeReason.PageEviction, storeFunctions.Dequeue());
            if (initialReadCacheInsert)
                Assert.AreEqual(DisposeReason.PageEviction, storeFunctions.Dequeue());
            Assert.IsEmpty(storeFunctions.handlerQueue);

            if (initialReadCacheInsert)
            {
                using var session = fht.NewSession(new DisposeFunctionsNoSync());
                MyOutput output = new();
                var status = session.Read(ref collidingKey2, ref output);
                session.CompletePending(wait: true);
            }

            void DoRead(DisposeFunctions functions)
            {
                using var session = fht.NewSession(functions);
                if (functions.isSUT)
                {
                    MyOutput output = new();
                    MyInput input = new();
                    ReadOptions readOptions = default;
                    if (copyDest == ReadCopyDestination.Tail)
                        readOptions.ReadFlags = ReadFlags.CopyReadsToTail;
                    var status = session.Read(ref key, ref input, ref output, ref readOptions, out _);
                    Assert.IsTrue(status.IsPending, status.ToString());
                    session.CompletePendingWithOutputs(out var completedOutputs, wait: true);
                    (status, output) = GetSinglePendingResult(completedOutputs);
                    Assert.AreEqual(TestInitialValue, output.value.value);
                }
                else
                {
                    // Do an upsert here to cause the collision (it will blindly insert)
                    otherGate.Wait();
                    MyKey collidingKey = new() { key = TestCollidingKey };
                    MyValue collidingValue = new() { value = TestCollidingValue };
                    session.Upsert(ref collidingKey, ref collidingValue);
                }
            }

            var tasks = new[]
            {
                Task.Factory.StartNew(() => DoRead(functions1)),
                Task.Factory.StartNew(() => DoRead(functions2))
            };
            Task.WaitAll(tasks);

            if (initialReadCacheInsert)
                Assert.AreEqual(DisposeReason.DeserializedFromDisk, storeFunctions.Dequeue());
            Assert.AreEqual(DisposeReason.SingleWriterCASFailed, storeFunctions.Dequeue());
            Assert.AreEqual(DisposeReason.DeserializedFromDisk, storeFunctions.Dequeue());
            Assert.IsEmpty(storeFunctions.handlerQueue);
        }

        [Test]
        [Category("FasterKV")]
        [Category("Smoke")]
        public void DisposePendingReadWithNoInsertTest()
        {
            var functions = new DisposeFunctionsNoSync();

            MyKey key = new() { key = TestKey };
            MyValue value = new() { value = TestInitialValue };

            // Do initial insert
            using var session = fht.NewSession(functions);
            session.Upsert(ref key, ref value);

            // FlushAndEvict so we go pending
            DoFlush(FlushMode.OnDisk);
            Assert.AreEqual(DisposeReason.PageEviction, storeFunctions.Dequeue());
            Assert.IsEmpty(storeFunctions.handlerQueue);

            MyOutput output = new();
            var status = session.Read(ref key, ref output);
            Assert.IsTrue(status.IsPending, status.ToString());
            session.CompletePendingWithOutputs(out var completedOutputs, wait: true);
            (status, output) = GetSinglePendingResult(completedOutputs);
            Assert.AreEqual(TestInitialValue, output.value.value);

            Assert.AreEqual(DisposeReason.DeserializedFromDisk, storeFunctions.Dequeue());
            Assert.IsEmpty(storeFunctions.handlerQueue);
        }

        [Test]
        [Category("FasterKV")]
        [Category("Smoke")]
        public void DisposePendingRmwWithNoConflictTest()
        {
            var functions = new DisposeFunctionsNoSync();

            MyKey key = new() { key = TestKey };
            MyValue value = new() { value = TestInitialValue };

            // Do initial insert
            using var session = fht.NewSession(functions);
            session.Upsert(ref key, ref value);

            // FlushAndEvict so we go pending
            DoFlush(FlushMode.OnDisk);
            Assert.AreEqual(DisposeReason.PageEviction, storeFunctions.Dequeue());
            Assert.IsEmpty(storeFunctions.handlerQueue);

            MyInput input = new() { value = TestUpdatedValue };
            MyOutput output = new();
            var status = session.RMW(ref key, ref input, ref output);
            Assert.IsTrue(status.IsPending, status.ToString());
            session.CompletePendingWithOutputs(out var completedOutputs, wait: true);
            (status, output) = GetSinglePendingResult(completedOutputs);
            Assert.AreEqual(TestInitialValue + TestUpdatedValue, output.value.value);

            Assert.AreEqual(DisposeReason.DeserializedFromDisk, storeFunctions.Dequeue());
            Assert.IsEmpty(storeFunctions.handlerQueue);
        }

        [Test]
        [Category("FasterKV")]
        [Category("Smoke")]
        public void DisposeMainLogPageEvictionTest()
        {
            var functions = new DisposeFunctionsNoSync();

            MyKey key = new() { key = TestKey };
            MyValue value = new() { value = TestInitialValue };

            // Do initial insert
            using var session = fht.NewSession(functions);
            session.Upsert(ref key, ref value);

            // FlushAndEvict to cause the eviction
            DoFlush(FlushMode.OnDisk);
            Assert.AreEqual(DisposeReason.PageEviction, storeFunctions.Dequeue());
            Assert.IsEmpty(storeFunctions.handlerQueue);
        }

        [Test]
        [Category("FasterKV")]
        [Category("Smoke")]
        public void DisposeReadCachePageEvictionTest([Values(ReadCopyDestination.ReadCache)] ReadCopyDestination _)
        {
            // We use the ReadCopyDestination.ReadCache parameter so Setup() knows to set up the readcache.
            var functions = new DisposeFunctionsNoSync();

            MyKey key = new() { key = TestKey };
            MyValue value = new() { value = TestInitialValue };

            // Do initial insert
            using var session = fht.NewSession(functions);
            session.Upsert(ref key, ref value);

            // FlushAndEvict so we go pending
            DoFlush(FlushMode.OnDisk);
            Assert.AreEqual(DisposeReason.PageEviction, storeFunctions.Dequeue());
            Assert.IsEmpty(storeFunctions.handlerQueue);

            // Read will go to the ReadCache
            MyOutput output = new();
            var status = session.Read(ref key, ref output);
            Assert.IsTrue(status.IsPending, status.ToString());
            session.CompletePendingWithOutputs(out var completedOutputs, wait: true);
            (status, output) = GetSinglePendingResult(completedOutputs);
            Assert.AreEqual(TestInitialValue, output.value.value);

            fht.ReadCache.FlushAndEvict(wait: true);
            Assert.AreEqual(DisposeReason.DeserializedFromDisk, storeFunctions.Dequeue());
            Assert.AreEqual(DisposeReason.PageEviction, storeFunctions.Dequeue());
            Assert.IsEmpty(storeFunctions.handlerQueue);
        }
    }
}
