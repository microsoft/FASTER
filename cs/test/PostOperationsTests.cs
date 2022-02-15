// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using NUnit.Framework;

namespace FASTER.test
{
    [TestFixture]
    internal class PostOperationsTests
    {
        class PostFunctions : SimpleFunctions<int, int>
        {
            internal long pswAddress;
            internal long piuAddress;
            internal long pcuAddress;
            internal long psdAddress;

            internal void Clear()
            {
                pswAddress = Constants.kInvalidAddress;
                piuAddress = Constants.kInvalidAddress;
                pcuAddress = Constants.kInvalidAddress;
                psdAddress = Constants.kInvalidAddress;
            }

            internal PostFunctions() : base() { }

            public override void PostSingleWriter(ref int key, ref int input, ref int src, ref int dst, ref int output, ref RecordInfo recordInfo, ref UpdateInfo updateInfo, WriteReason reason) { this.pswAddress = updateInfo.Address; }

            public override void InitialUpdater(ref int key, ref int input, ref int value, ref int output, ref RecordInfo recordInfo, ref UpdateInfo updateInfo) { value = input; }
            /// <inheritdoc/>
            public override void PostInitialUpdater(ref int key, ref int input, ref int value, ref int output, ref RecordInfo recordInfo, ref UpdateInfo updateInfo) { this.piuAddress = updateInfo.Address; }

            public override bool InPlaceUpdater(ref int key, ref int input, ref int value, ref int output, ref RecordInfo recordInfo, ref UpdateInfo updateInfo) => false; // For this test, we want this to fail and lead to InitialUpdater

            /// <inheritdoc/>
            public override void CopyUpdater(ref int key, ref int input, ref int oldValue, ref int newValue, ref int output, ref RecordInfo recordInfo, ref UpdateInfo updateInfo) { newValue = oldValue; }
            /// <inheritdoc/>
            public override bool PostCopyUpdater(ref int key, ref int input, ref int oldValue, ref int newValue, ref int output, ref RecordInfo recordInfo, ref UpdateInfo updateInfo) { this.pcuAddress = updateInfo.Address; return true; }

            public override void PostSingleDeleter(ref int key, ref RecordInfo recordInfo, ref UpdateInfo updateInfo) { this.psdAddress = updateInfo.Address; }
            public override bool ConcurrentDeleter(ref int key, ref int value, ref RecordInfo recordInfo, ref UpdateInfo updateInfo) => false;
        }

        private FasterKV<int, int> fht;
        private ClientSession<int, int, int, int, Empty, PostFunctions> session;
        private IDevice log;

        const int numRecords = 100;
        const int targetKey = 42;
        long expectedAddress;

        [SetUp]
        public void Setup()
        {
            // Clean up log files from previous test runs in case they weren't cleaned up
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);

            this.log = Devices.CreateLogDevice($"{TestUtils.MethodTestDir}/PostOperations.log", deleteOnClose: true);
            this.fht = new FasterKV<int, int>
                       (1L << 20, new LogSettings { LogDevice = log, MemorySizeBits = 15, PageSizeBits = 10 });
            this.session = fht.For(new PostFunctions()).NewSession<PostFunctions>();
            Populate();
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
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
        }

        void Populate()
        {
            for (var key = 0; key < numRecords; ++key)
            {
                this.expectedAddress = this.fht.Log.TailAddress;
                this.session.Upsert(key, key * 100);
                Assert.AreEqual(this.expectedAddress, session.functions.pswAddress);
            }

            session.functions.Clear();
            this.expectedAddress = this.fht.Log.TailAddress;
        }

        internal void CompletePendingAndVerifyInsertedAddress()
        {
            // Note: Only Read and RMW have Pending results.
            this.session.CompletePendingWithOutputs(out var completedOutputs, wait: true);
            TestUtils.GetSinglePendingResult(completedOutputs, out var recordMetadata);
            Assert.AreEqual(this.expectedAddress, recordMetadata.Address);
        }

        [Test]
        [Category("FasterKV")]
        [Category("Smoke")]
        public void PostSingleWriterTest()
        {
            // Populate has already executed the not-found test (InternalInsert) as part of its normal insert.

            // Execute the ReadOnly (InternalInsert) test
            this.fht.Log.FlushAndEvict(wait: true);
            this.session.Upsert(targetKey, targetKey * 1000);
            this.session.CompletePending(wait: true);
            Assert.AreEqual(this.expectedAddress, session.functions.pswAddress);
        }

        [Test]
        [Category("FasterKV")]
        [Category("Smoke")]
        public void PostInitialUpdaterTest()
        {
            // Execute the not-found test (InternalRMW).
            this.session.RMW(numRecords + 1, (numRecords + 1) * 1000);
            Assert.AreEqual(this.expectedAddress, session.functions.piuAddress);
            session.functions.Clear();

            // Now cause an attempt at InPlaceUpdater, which we've set to fail, so CopyUpdater is done (InternalInsert).
            this.expectedAddress = this.fht.Log.TailAddress;
            this.session.RMW(targetKey, targetKey * 1000);
            Assert.AreEqual(this.expectedAddress, session.functions.pcuAddress);

            // Execute the not-in-memory test (InternalContinuePendingRMW). First delete the record so it has a tombstone; this will go to InitialUpdater.
            this.session.Delete(targetKey);
            this.fht.Log.FlushAndEvict(wait: true);
            this.expectedAddress = this.fht.Log.TailAddress;

            this.session.RMW(targetKey, targetKey * 1000);
            CompletePendingAndVerifyInsertedAddress();
            Assert.AreEqual(this.expectedAddress, session.functions.piuAddress);
        }

        [Test]
        [Category("FasterKV")]
        [Category("Smoke")]
        public void PostCopyUpdaterTest()
        {
            // First try to modify in-memory, readonly (InternalRMW).
            this.fht.Log.ShiftReadOnlyAddress(fht.Log.ReadOnlyAddress, wait: true);
            this.session.RMW(targetKey, targetKey * 1000);
            Assert.AreEqual(this.expectedAddress, session.functions.pcuAddress);

            // Execute the not-in-memory test (InternalContinuePendingRMW).
            this.fht.Log.FlushAndEvict(wait: true);
            this.expectedAddress = this.fht.Log.TailAddress;
            this.session.RMW(targetKey, targetKey * 1000);
            CompletePendingAndVerifyInsertedAddress();
            Assert.AreEqual(this.expectedAddress, session.functions.pcuAddress);
        }

        [Test]
        [Category("FasterKV")]
        [Category("Smoke")]
        public void PostSingleDeleterTest()
        {
            // Execute the not-in-memory test (InternalDelete); ConcurrentDeleter returns false to force a new record to be added.
            this.session.Delete(targetKey);
            Assert.AreEqual(this.expectedAddress, session.functions.psdAddress);

            // Execute the not-in-memory test (InternalDelete).
            this.fht.Log.FlushAndEvict(wait: true);
            this.expectedAddress = this.fht.Log.TailAddress;
            this.session.Delete(targetKey + 1);
            Assert.AreEqual(this.expectedAddress, session.functions.psdAddress);
        }
    }
}
