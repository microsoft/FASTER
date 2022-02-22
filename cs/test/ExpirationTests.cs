// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using FASTER.core;
using NUnit.Framework;

namespace FASTER.test.Expiration
{
    [TestFixture]
    internal class ExpirationTests
    {
        // VarLenMax is the variable-length portion; 2 is for the fixed fields
        const int VarLenMax = 10;
        const int StackAllocMax = VarLenMax + 2;
        const int NumRecs = 5000;
        const int RandSeed = 100;
        const int ModifyKey = NumRecs / 2 + 3;
        const int NoValue = -1;
        const int NoKey = -100;
        const int SetIncrement = 1000000;

        static int TestValue => GetValue(ModifyKey);

        static int GetVarLen(Random rng) => rng.Next(VarLenMax) + 2;

        static int GetValue(int key) => key + NumRecs * 10;

        [Flags] internal enum Funcs { NeedInitialUpdate = 0x0001, NeedCopyUpdate = 0x0002, InPlaceUpdater = 0x0004, InitialUpdater, CopyUpdater = 0x0008, 
                                      SingleReader = 0x0010, ConcurrentReader = 0x0020,
                                      RMWCompletionCallback = 0x0100, ReadCompletionCallback = 0x0200,
                                      SkippedCopyUpdate = NeedCopyUpdate | RMWCompletionCallback,
                                      DidCopyUpdate = NeedCopyUpdate | CopyUpdater | RMWCompletionCallback };

        internal enum ExpirationResult { None, Incremented, ExpireDelete, ExpireRollover, Updated, NotUpdated, Deleted, NotDeleted };

        public struct ExpirationInput
        {
            internal int value;
            internal int comparisonValue;
            internal TestOp testOp;
        }

        public struct ExpirationOutput
        {
            internal int retrievedValue;
            internal Funcs functionsCalled;
            internal ExpirationResult result;

            internal void AddFunc(Funcs func) => this.functionsCalled |= func;

            public override string ToString() => $"value {retrievedValue}, funcs [{functionsCalled}], result {result}";
        }

        // The operations here describe intentions by the app. They are implemented entirely in Functions and do not have first-class support in FASTER
        // other than handling returns from Functions methods appropriately.
        internal enum TestOp {
            None,                               // Default value; not otherwise used
            Increment,                          // Increment a counter
            PassiveExpire,                      // Increment a counter but do not expire the record explicitly; let Read encounter it as expired.
                                                //  This simulates a timeout-based (rather than counter-based) expiration.
            ExpireDelete,                       // Increment a counter and expire the record by deleting it
                                                //  Mutable:
                                                //      IPU sets tombstone and returns true; we see this and TryRemoveDeletedHashEntry and return SUCCESS
                                                //  OnDisk:
                                                //      CU sets tombstone; operation proceeds as normal otherwise
            ExpireRollover,                     // Increment a counter and expire the record by rolling it over (back to 1)
                                                //  Mutable
                                                //      IPU - InPlace (input len <= record len): Execute IU logic in current space; return true from IPU
                                                //          - (TODO with revivication) NewRecord (input len > record len): Return false from IPU, true from NCU, set in CU
                                                //  OnDisk:
                                                //      CU  - executes IU logic
            SetIfKeyExists,                     // Update a record if the key exists, but do not create a new record
                                                //  Mutable:
                                                //      Exists: update and return true from IPU
                                                //      NotExists: return false from NIU
                                                //  OnDisk:
                                                //      Exists: return true from NCU, update in CU
                                                //      NotExists: return false from NIU
            SetIfKeyNotExists,                  // Create a new record if the key does not exist, but do not update an existing record
                                                //  Mutable:
                                                //      Exists: no-op and return true from IPU
                                                //      NotExists: return true from NIU, set in IU
                                                //  OnDisk:
                                                //      Exists: return false from NCU
                                                //      NotExists: return true from NIU, set in IU
            SetIfValueEquals,                   // Update the record for a key if the current value equals a specified value
                                                //  Mutable:
                                                //      Equals: update and return true from IPU
                                                //      NotEquals: no-op and return true from IPU
                                                //  OnDisk:
                                                //      Equals: return true from NCU, update in CU
                                                //      NotEquals: return false from NCU
                                                //  NotExists: Return false from NIU
            SetIfValueNotEquals,                // Update the record for a key if the current value does not equal a specified value
                                                //  Mutable:
                                                //      Equals: no-op and return true from IPU
                                                //      NotEquals: update and return true from IPU
                                                //  OnDisk:
                                                //      Equals: return false from NCU
                                                //      NotEquals: return true from NCU, update in CU
                                                //  NotExists: Return false from NIU
            DeleteIfValueEquals,                // Delete the record for a key if the current value equals a specified value
                                                //  Mutable:
                                                //      Equals: Same as ExpireDelete
                                                //      NotEquals: no-op and return true from IPU
                                                //  OnDisk:
                                                //      Equals: Same as ExpireDelete
                                                //      NotEquals: return false from NCU
                                                //  NotExists: Return false from NIU
            DeleteIfValueNotEquals,             // Delete the record for a key if the current value does not equal a specified value
                                                //  Mutable:
                                                //      Equals: no-op and return true from IPU
                                                //      NotEquals: Same as ExpireDelete
                                                //  Mutable:
                                                //      Equals: return false from NCU
                                                //      NotEquals: Same as ExpireDelete
                                                //  NotExists: Return false from NIU
            Revivify                            // TODO - NYI: An Update or RMW operation encounters a tombstoned record of >= size of the new value, so the record is updated.
                                                //      Test with newsize < space, then again with newsize == original space
                                                //          Verify tombstone is revivified on later insert (SingleWriter called within FASTER-acquired RecordInfo.SpinLock)
                                                //          Verify tombstone is revivified on later simple RMW (IU called within FASTER-acquired RecordInfo.SpinLock)
        };

        public class ExpirationFunctions : FunctionsBase<int, VLValue, ExpirationInput, ExpirationOutput, Empty>
        {
            private static unsafe void VerifyValue(int key, ref VLValue value)
            {
                int* output = (int*)Unsafe.AsPointer(ref value);
                for (int j = 0; j < value.length; j++)
                    Assert.AreEqual(key, output[j]);
            }

            static bool IsExpired(int key, int value) => value == GetValue(key) + 2;

            public override bool NeedInitialUpdate(ref int key, ref ExpirationInput input, ref ExpirationOutput output, ref RMWInfo rmwInfo)
            {
                output.AddFunc(Funcs.NeedInitialUpdate);
                switch (input.testOp)
                {
                    case TestOp.Increment:
                    case TestOp.PassiveExpire:
                    case TestOp.ExpireDelete:
                    case TestOp.ExpireRollover:
                        Assert.Fail($"{input.testOp} should not get here");
                        return false;
                    case TestOp.SetIfKeyExists:
                        return false;
                    case TestOp.SetIfKeyNotExists:
                        return true;
                    case TestOp.SetIfValueEquals:
                    case TestOp.SetIfValueNotEquals:
                        return false;
                    case TestOp.DeleteIfValueEquals:
                    case TestOp.DeleteIfValueNotEquals:
                        return false;
                    case TestOp.Revivify:
                        Assert.Fail($"{input.testOp} should not get here");
                        return false;
                    default:
                        Assert.Fail($"Unexpected testOp: {input.testOp}");
                        return false;
                }
            }

            public override bool NeedCopyUpdate(ref int key, ref ExpirationInput input, ref VLValue oldValue, ref ExpirationOutput output, ref RMWInfo rmwInfo)
            {
                output.AddFunc(Funcs.NeedCopyUpdate);
                switch (input.testOp)
                {
                    case TestOp.Increment:
                    case TestOp.PassiveExpire:
                    case TestOp.ExpireDelete:
                    case TestOp.ExpireRollover:
                    case TestOp.SetIfKeyExists:
                        return true;
                    case TestOp.SetIfKeyNotExists:
                        return false;
                    case TestOp.SetIfValueEquals:
                        return oldValue.field1 == input.comparisonValue;
                    case TestOp.SetIfValueNotEquals:
                        return oldValue.field1 != input.comparisonValue;
                    case TestOp.DeleteIfValueEquals:
                        return oldValue.field1 == input.comparisonValue;
                    case TestOp.DeleteIfValueNotEquals:
                        return oldValue.field1 != input.comparisonValue;
                    case TestOp.Revivify:
                        Assert.Fail($"{input.testOp} should not get here");
                        return false;
                    default:
                        Assert.Fail($"Unexpected testOp: {input.testOp}");
                        return false;
                }
            }

            /// <inheritdoc/>
            public override void CopyUpdater(ref int key, ref ExpirationInput input, ref VLValue oldValue, ref VLValue newValue, ref ExpirationOutput output, ref RecordInfo recordInfo, ref RMWInfo rmwInfo)
            {
                output.AddFunc(Funcs.CopyUpdater);
                switch (input.testOp)
                {
                    case TestOp.Increment:
                        Assert.AreEqual(GetValue(key), oldValue.field1);
                        goto case TestOp.PassiveExpire;
                    case TestOp.PassiveExpire:
                        newValue.field1 = oldValue.field1 + 1;
                        output.result = ExpirationResult.Incremented;
                        return;
                    case TestOp.ExpireDelete:
                        Assert.AreEqual(GetValue(key) + 1, oldValue.field1);    // For this test we only call this operation when the value will expire
                        newValue.field1 = oldValue.field1 + 1;
                        recordInfo.Tombstone = true;
                        output.result = ExpirationResult.ExpireDelete;
                        return;
                    case TestOp.ExpireRollover:
                        Assert.AreEqual(GetValue(key) + 1, oldValue.field1);    // For this test we only call this operation when the value will expire
                        newValue.field1 = GetValue(key);
                        output.result = ExpirationResult.ExpireRollover;
                        output.retrievedValue = newValue.field1;
                        return;
                    case TestOp.SetIfKeyExists:
                        newValue.field1 = input.value;
                        output.result = ExpirationResult.Updated;
                        output.retrievedValue = newValue.field1;
                        return;
                    case TestOp.SetIfKeyNotExists:
                        Assert.Fail($"{input.testOp} should not get here");
                        return;
                    case TestOp.SetIfValueEquals:
                        if (oldValue.field1 == input.comparisonValue)
                        {
                            newValue.field1 = input.value;
                            output.result = ExpirationResult.Updated;
                            output.retrievedValue = newValue.field1;
                        }
                        else
                        {
                            output.result = ExpirationResult.NotUpdated;
                            output.retrievedValue = oldValue.field1;
                        }

                        return;
                    case TestOp.SetIfValueNotEquals:
                        if (oldValue.field1 != input.comparisonValue)
                        {
                            newValue.field1 = input.value;
                            output.result = ExpirationResult.Updated;
                            output.retrievedValue = newValue.field1;
                        }
                        else
                        {
                            output.result = ExpirationResult.NotUpdated;
                            output.retrievedValue = oldValue.field1;
                        }
                        return;
                    case TestOp.DeleteIfValueEquals:
                        if (oldValue.field1 == input.comparisonValue)
                        {
                            recordInfo.Tombstone = true;
                            output.result = ExpirationResult.Deleted;
                        }
                        else
                        {
                            Assert.Fail("Should have returned false from NeedCopyUpdate");
                        }
                        return;
                    case TestOp.DeleteIfValueNotEquals:
                        if (oldValue.field1 != input.comparisonValue)
                        { 
                            recordInfo.Tombstone = true;
                            output.result = ExpirationResult.Deleted;
                        }
                        else
                        {
                            Assert.Fail("Should have returned false from NeedCopyUpdate");
                        }
                        return;
                    case TestOp.Revivify:
                        Assert.Fail($"{input.testOp} should not get here");
                        return;
                    default:
                        Assert.Fail($"Unexpected testOp: {input.testOp}");
                        return;
                }
            }

            public override void InitialUpdater(ref int key, ref ExpirationInput input, ref VLValue value, ref ExpirationOutput output, ref RecordInfo recordInfo, ref RMWInfo rmwInfo)
            {
                output.AddFunc(Funcs.InitialUpdater);
                value.field1 = input.value;
                output.result = ExpirationResult.Updated;
                output.retrievedValue = value.field1;
            }

            public override bool InPlaceUpdater(ref int key, ref ExpirationInput input, ref VLValue value, ref ExpirationOutput output, ref RecordInfo recordInfo, ref RMWInfo rmwInfo)
            {
                output.AddFunc(Funcs.InPlaceUpdater);
                switch (input.testOp)
                {
                    case TestOp.Increment:
                        Assert.AreEqual(GetValue(key), value.field1);
                        goto case TestOp.PassiveExpire;
                    case TestOp.PassiveExpire:
                        ++value.field1;
                        output.result = ExpirationResult.Incremented;
                        return true;
                    case TestOp.ExpireDelete:
                        Assert.AreEqual(GetValue(key) + 1, value.field1);       // For this test we only call this operation when the value will expire
                        ++value.field1;
                        recordInfo.Tombstone = true;
                        output.result = ExpirationResult.ExpireDelete;
                        return true;
                    case TestOp.ExpireRollover:
                        Assert.AreEqual(GetValue(key) + 1, value.field1);       // For this test we only call this operation when the value will expire
                        value.field1 = GetValue(key);
                        output.result = ExpirationResult.ExpireRollover;
                        output.retrievedValue = value.field1;
                        return true;
                    case TestOp.SetIfKeyExists:
                        value.field1 = input.value;
                        output.result = ExpirationResult.Updated;
                        output.retrievedValue = value.field1;
                        return true;
                    case TestOp.SetIfKeyNotExists:
                        // No-op
                        return true;
                    case TestOp.SetIfValueEquals:
                        if (value.field1 == input.comparisonValue)
                        { 
                            value.field1 = input.value;
                            output.result = ExpirationResult.Updated;
                        }
                        else
                            output.result = ExpirationResult.NotUpdated;
                        output.retrievedValue = value.field1;
                        return true;
                    case TestOp.SetIfValueNotEquals:
                        if (value.field1 != input.comparisonValue)
                        { 
                            value.field1 = input.value;
                            output.result = ExpirationResult.Updated;
                        }
                        else
                            output.result = ExpirationResult.NotUpdated;
                        output.retrievedValue = value.field1;
                        return true;
                    case TestOp.DeleteIfValueEquals:
                        if (value.field1 == input.comparisonValue)
                        {
                            recordInfo.Tombstone = true;
                            output.result = ExpirationResult.Deleted;
                        }
                        else
                        {
                            output.result = ExpirationResult.NotDeleted;
                            output.retrievedValue = value.field1;
                        }
                        return true;
                    case TestOp.DeleteIfValueNotEquals:
                        if (value.field1 != input.comparisonValue)
                        {
                            recordInfo.Tombstone = true;
                            output.result = ExpirationResult.Deleted;
                        }
                        else
                        {
                            output.result = ExpirationResult.NotDeleted;
                            output.retrievedValue = value.field1;
                        }
                        return true;
                    case TestOp.Revivify:
                        Assert.Fail($"{input.testOp} should not get here");
                        return false;
                    default:
                        Assert.Fail($"Unexpected testOp: {input.testOp}");
                        return false;
                }
            }

            public override void RMWCompletionCallback(ref int key, ref ExpirationInput input, ref ExpirationOutput output, Empty ctx, Status status, RecordMetadata recordMetadata)
            {
                output.AddFunc(Funcs.RMWCompletionCallback);
            }

            public override void ReadCompletionCallback(ref int key, ref ExpirationInput input, ref ExpirationOutput output, Empty ctx, Status status, RecordMetadata recordMetadata)
            {
                output.AddFunc(Funcs.ReadCompletionCallback);
            }

            // Read functions
            public override bool SingleReader(ref int key, ref ExpirationInput input, ref VLValue value, ref ExpirationOutput output, ref RecordInfo recordInfo, ref ReadInfo readInfo)
            {
                output.AddFunc(Funcs.SingleReader);
                if (IsExpired(key, value.field1))
                    return false;
                output.retrievedValue = value.field1;
                return true;
            }

            public override bool ConcurrentReader(ref int key, ref ExpirationInput input, ref VLValue value, ref ExpirationOutput output, ref RecordInfo recordInfo, ref ReadInfo readInfo)
            {
                output.AddFunc(Funcs.ConcurrentReader);
                if (IsExpired(key, value.field1))
                    return false;
                output.retrievedValue = value.field1;
                return true;
            }

            // Upsert functions
            public override void SingleWriter(ref int key, ref ExpirationInput input, ref VLValue src, ref VLValue dst, ref ExpirationOutput output, ref RecordInfo recordInfo, ref UpsertInfo upsertInfo, WriteReason reason)
            {
                src.CopyTo(ref dst);
            }

            public override bool ConcurrentWriter(ref int key, ref ExpirationInput input, ref VLValue src, ref VLValue dst, ref ExpirationOutput output, ref RecordInfo recordInfo, ref UpsertInfo upsertInfo)
            {
                src.CopyTo(ref dst);
                return true;
            }
        }

        private string path;
        IDevice log;
        ExpirationFunctions functions;
        FasterKV<int, VLValue> fht;
        ClientSession<int, VLValue, ExpirationInput, ExpirationOutput, Empty, ExpirationFunctions> session;

        [SetUp]
        public void Setup()
        {
            path = TestUtils.MethodTestDir + "/";
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);

            log = Devices.CreateLogDevice(TestUtils.MethodTestDir + "/hlog.log", deleteOnClose: true);
            fht = new FasterKV<int, VLValue>
                (128,
                new LogSettings { LogDevice = log, MemorySizeBits = 17, PageSizeBits = 12 },
                null, null, null, new VariableLengthStructSettings<int, VLValue> { valueLength = new VLValue() }
                );

            this.functions = new ExpirationFunctions();
            session = fht.For(this.functions).NewSession(this.functions);
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
            TestUtils.DeleteDirectory(path);
        }

        private unsafe void Populate(Random rng)
        {
            // Single alloc outside the loop, to the max length we'll need.
            int* val = stackalloc int[StackAllocMax];

            for (int i = 0; i < NumRecs; i++)
            {
                var len = GetVarLen(rng);
                ref VLValue value = ref *(VLValue*)val;
                value.length = len;
                for (int j = 1; j < len; j++)
                    *(val + j) = GetValue(i);

                session.Upsert(ref i, ref value, Empty.Default, 0);
            }
        }

        private ExpirationOutput GetRecord(int key, Status expectedStatus, bool isOnDisk)
        {
            ExpirationInput input = default;
            ExpirationOutput output = new();

            var status = session.Read(ref key, ref input, ref output, Empty.Default, 0);
            if (status.IsPending)
            {
                Assert.IsTrue(isOnDisk);
                session.CompletePendingWithOutputs(out var completedOutputs, wait:true);
                (status, output) = TestUtils.GetSinglePendingResult(completedOutputs);
            }

            Assert.AreEqual(expectedStatus, status);
            return output;
        }

        private ExpirationOutput ExecuteRMW(int key, ref ExpirationInput input, bool isOnDisk, Status expectedStatus = default)
        {
            ExpirationOutput output = new ();
            var status = session.RMW(ref key, ref input, ref output);
            if (status.IsPending)
            {
                Assert.IsTrue(isOnDisk);
                session.CompletePendingWithOutputs(out var completedOutputs, wait: true);
                (status, output) = TestUtils.GetSinglePendingResult(completedOutputs);
            }

            Assert.AreEqual(expectedStatus, status);
            return output;
        }

        private Status GetMutableVsOnDiskStatus(bool isOnDisk)
        {
            // The behavior is different for OnDisk vs. mutable:
            //  - OnDisk results in a call to NeedCopyUpdate which returns false, so RMW returns OK
            //  - Mutable results in a call to IPU which returns true, so RMW returns InPlaceUpdatedRecord.
            return new(isOnDisk ? StatusCode.OK : StatusCode.InPlaceUpdatedRecord);
        }

        void InitialIncrement()
        {
            Populate(new Random(RandSeed));
            InitialRead(isOnDisk: false, afterIncrement: false);
            IncrementValue(TestOp.Increment, isOnDisk: false);
        }

        private void InitialRead(bool isOnDisk, bool afterIncrement)
        {
            var output = GetRecord(ModifyKey, new(StatusCode.OK), isOnDisk);
            Assert.AreEqual(GetValue(ModifyKey) + (afterIncrement ? 1 : 0), output.retrievedValue);
            Assert.AreEqual(isOnDisk ? (Funcs.SingleReader | Funcs.ReadCompletionCallback) : Funcs.ConcurrentReader, output.functionsCalled);
        }

        private void IncrementValue(TestOp testOp, bool isOnDisk)
        {
            var key = ModifyKey;
            ExpirationInput input = new() { testOp = testOp };
            Status expectedFoundRmwStatus = isOnDisk ? new(StatusCode.CopyUpdatedRecord) : new(StatusCode.InPlaceUpdatedRecord);
            ExpirationOutput output = ExecuteRMW(key, ref input, isOnDisk, expectedFoundRmwStatus);
            Assert.AreEqual(isOnDisk ? Funcs.DidCopyUpdate : Funcs.InPlaceUpdater, output.functionsCalled);
            Assert.AreEqual(ExpirationResult.Incremented, output.result);
        }

        private void MaybeEvict(bool isOnDisk)
        {
            if (isOnDisk)
            {
                fht.Log.FlushAndEvict(wait: true);
                InitialRead(isOnDisk, afterIncrement: true);
            }
        }

        private void VerifyKeyNotCreated(TestOp testOp, bool isOnDisk)
        {
            var key = NoKey;
            // Key doesn't exist - no-op
            ExpirationInput input = new() { testOp = testOp, value = NoValue, comparisonValue = GetValue(key) + 1 };

            ExpirationOutput output = ExecuteRMW(key, ref input, isOnDisk, new(StatusCode.NotFound));
            Assert.AreEqual(Funcs.NeedInitialUpdate, output.functionsCalled);
            Assert.AreEqual(ExpirationResult.None, output.result);
            Assert.AreEqual(0, output.retrievedValue);

            // Verify it's not there
            GetRecord(key, new(StatusCode.NotFound), isOnDisk);
        }

        [Test]
        [Category("FasterKV")]
        [Category("Smoke"), Category("RMW")]
        public void PassiveExpireTest([Values]bool isOnDisk)
        {
            InitialIncrement();
            MaybeEvict(isOnDisk);
            IncrementValue(TestOp.PassiveExpire, isOnDisk);
            GetRecord(ModifyKey, new(StatusCode.NotFound), isOnDisk);
        }

        [Test]
        [Category("FasterKV")]
        [Category("Smoke"), Category("RMW")]
        public void ExpireDeleteTest([Values] bool isOnDisk)
        {
            InitialIncrement();
            MaybeEvict(isOnDisk);
            const TestOp testOp = TestOp.ExpireDelete;
            var key = ModifyKey;
            Status expectedFoundRmwStatus = isOnDisk ? new(StatusCode.CopyUpdatedRecord) : new(StatusCode.InPlaceUpdatedRecord);

            // Increment/Delete it
            ExpirationInput input = new() { testOp = testOp };
            ExpirationOutput output = ExecuteRMW(key, ref input, isOnDisk, expectedFoundRmwStatus);
            Assert.AreEqual(isOnDisk ? Funcs.DidCopyUpdate : Funcs.InPlaceUpdater, output.functionsCalled);
            Assert.AreEqual(ExpirationResult.ExpireDelete, output.result);

            // Verify it's not there
            GetRecord(key, new(StatusCode.NotFound), isOnDisk);
        }

        [Test]
        [Category("FasterKV")]
        [Category("Smoke"), Category("RMW")]
        public void ExpireRolloverTest([Values] bool isOnDisk)
        {
            InitialIncrement();
            MaybeEvict(isOnDisk);
            const TestOp testOp = TestOp.ExpireRollover;
            var key = ModifyKey;
            Status expectedFoundRmwStatus = isOnDisk ? new(StatusCode.CopyUpdatedRecord) : new(StatusCode.InPlaceUpdatedRecord);

            // Increment/Rollover to initial state
            ExpirationInput input = new() { testOp = testOp };
            ExpirationOutput output = ExecuteRMW(key, ref input, isOnDisk, expectedFoundRmwStatus);
            Assert.AreEqual(isOnDisk ? Funcs.DidCopyUpdate : Funcs.InPlaceUpdater, output.functionsCalled);
            Assert.AreEqual(ExpirationResult.ExpireRollover, output.result);
            Assert.AreEqual(GetValue(key), output.retrievedValue);

            // Verify it's there with initial state
            output = GetRecord(key, new(StatusCode.OK), isOnDisk:false /* update was appended */);
            Assert.AreEqual(GetValue(key), output.retrievedValue);
        }

        [Test]
        [Category("FasterKV")]
        [Category("Smoke"), Category("RMW")]
        public void SetIfKeyExistsTest([Values] bool isOnDisk)
        {
            InitialIncrement();
            MaybeEvict(isOnDisk);
            const TestOp testOp = TestOp.SetIfKeyExists;
            var key = ModifyKey;
            Status expectedFoundRmwStatus = isOnDisk ? new(StatusCode.CopyUpdatedRecord) : new(StatusCode.InPlaceUpdatedRecord);

            // Key exists - update it
            ExpirationInput input = new() { testOp = testOp, value = GetValue(key) + SetIncrement };
            ExpirationOutput output = ExecuteRMW(key, ref input, isOnDisk, expectedFoundRmwStatus);
            Assert.AreEqual(isOnDisk ? Funcs.DidCopyUpdate : Funcs.InPlaceUpdater, output.functionsCalled);
            Assert.AreEqual(ExpirationResult.Updated, output.result);
            Assert.AreEqual(input.value, output.retrievedValue);

            // Verify it's there with updated value
            output = GetRecord(key, new(StatusCode.OK), isOnDisk: false /* update was appended */);
            Assert.AreEqual(input.value, output.retrievedValue);

            // Key doesn't exist - no-op
            key += SetIncrement;
            input = new() { testOp = testOp, value = GetValue(key) };
            output = ExecuteRMW(key, ref input, isOnDisk, new(StatusCode.NotFound));
            Assert.AreEqual(Funcs.NeedInitialUpdate, output.functionsCalled);

            // Verify it's not there
            GetRecord(key, new(StatusCode.NotFound), isOnDisk);
        }

        [Test]
        [Category("FasterKV")]
        [Category("Smoke"), Category("RMW")]
        public void SetIfKeyNotExistsTest([Values] bool isOnDisk)
        {
            InitialIncrement();
            MaybeEvict(isOnDisk);
            const TestOp testOp = TestOp.SetIfKeyNotExists;
            var key = ModifyKey;

            // Key exists - no-op
            ExpirationInput input = new() { testOp = testOp, value = GetValue(key) + SetIncrement };
            ExpirationOutput output = ExecuteRMW(key, ref input, isOnDisk, GetMutableVsOnDiskStatus(isOnDisk));
            Assert.AreEqual(isOnDisk ? Funcs.SkippedCopyUpdate : Funcs.InPlaceUpdater, output.functionsCalled);

            // Verify it's there with unchanged value
            output = GetRecord(key, new(StatusCode.OK), isOnDisk);
            Assert.AreEqual(GetValue(key) + 1, output.retrievedValue);

            // Key doesn't exist - create it
            key += SetIncrement;
            input = new() { testOp = testOp, value = GetValue(key) };
            output = ExecuteRMW(key, ref input, isOnDisk, new(StatusCode.NotFound | StatusCode.CreatedRecord));
            Assert.AreEqual(Funcs.InitialUpdater, output.functionsCalled);
            Assert.AreEqual(ExpirationResult.Updated, output.result);
            Assert.AreEqual(input.value, output.retrievedValue);

            // Verify it's there with specified value
            output = GetRecord(key, new(StatusCode.OK), isOnDisk: false /* was just added */);
            Assert.AreEqual(input.value, output.retrievedValue);
        }

        [Test]
        [Category("FasterKV")]
        [Category("Smoke"), Category("RMW")]
        public void SetIfValueEqualsTest([Values] bool isOnDisk)
        {
            InitialIncrement();
            MaybeEvict(isOnDisk);
            const TestOp testOp = TestOp.SetIfValueEquals;
            var key = ModifyKey;
            Status expectedFoundRmwStatus = isOnDisk ? new(StatusCode.CopyUpdatedRecord) : new(StatusCode.InPlaceUpdatedRecord);

            VerifyKeyNotCreated(testOp, isOnDisk);

            // Value equals - update it
            ExpirationInput input = new() { testOp = testOp, value = GetValue(key) + SetIncrement, comparisonValue = GetValue(key) + 1 };
            ExpirationOutput output = ExecuteRMW(key, ref input, isOnDisk, expectedFoundRmwStatus);
            Assert.AreEqual(isOnDisk ? Funcs.DidCopyUpdate : Funcs.InPlaceUpdater, output.functionsCalled);
            Assert.AreEqual(ExpirationResult.Updated, output.result);
            Assert.AreEqual(input.value, output.retrievedValue);

            // Verify it's there with updated value
            output = GetRecord(key, new(StatusCode.OK), isOnDisk);
            Assert.AreEqual(input.value, output.retrievedValue);

            // Value doesn't equal - no-op
            key += 1;   // We modified ModifyKey so get the next-higher key
            input = new() { testOp = testOp, value = -2, comparisonValue = -1 };
            output = ExecuteRMW(key, ref input, isOnDisk, GetMutableVsOnDiskStatus(isOnDisk));
            Assert.AreEqual(isOnDisk ? Funcs.SkippedCopyUpdate : Funcs.InPlaceUpdater, output.functionsCalled);
            Assert.AreEqual(isOnDisk ? ExpirationResult.None : ExpirationResult.NotUpdated, output.result);
            Assert.AreEqual(isOnDisk ? 0 : GetValue(key), output.retrievedValue);

            // Verify it's there with unchanged value; note that it has not been InitialIncrement()ed
            output = GetRecord(key, new(StatusCode.OK), isOnDisk);
            Assert.AreEqual(GetValue(key), output.retrievedValue);
        }

        [Test]
        [Category("FasterKV")]
        [Category("Smoke"), Category("RMW")]
        public void SetIfValueNotEqualsTest([Values] bool isOnDisk)
        {
            InitialIncrement();
            MaybeEvict(isOnDisk);
            const TestOp testOp = TestOp.SetIfValueNotEquals;
            var key = ModifyKey;
            Status expectedFoundRmwStatus = isOnDisk ? new(StatusCode.CopyUpdatedRecord) : new(StatusCode.InPlaceUpdatedRecord);

            VerifyKeyNotCreated(testOp, isOnDisk);

            // Value equals
            ExpirationInput input = new() { testOp = testOp, value = -2, comparisonValue = GetValue(key) + 1 };
            ExpirationOutput output = ExecuteRMW(key, ref input, isOnDisk, GetMutableVsOnDiskStatus(isOnDisk));
            Assert.AreEqual(isOnDisk ? Funcs.SkippedCopyUpdate : Funcs.InPlaceUpdater, output.functionsCalled);
            Assert.AreEqual(isOnDisk ? ExpirationResult.None : ExpirationResult.NotUpdated, output.result);
            Assert.AreEqual(isOnDisk ? 0 : GetValue(key) + 1, output.retrievedValue);

            output = GetRecord(key, new(StatusCode.OK), isOnDisk);
            Assert.AreEqual(GetValue(key) + 1, output.retrievedValue);

            // Value doesn't equal
            input = new() { testOp = testOp, value = GetValue(key) + SetIncrement, comparisonValue = -1 };
            output = ExecuteRMW(key, ref input, isOnDisk, expectedFoundRmwStatus);
            Assert.AreEqual(isOnDisk ? Funcs.DidCopyUpdate : Funcs.InPlaceUpdater, output.functionsCalled);
            Assert.AreEqual(ExpirationResult.Updated, output.result);
            Assert.AreEqual(GetValue(key) + SetIncrement, output.retrievedValue);

            output = GetRecord(key, new(StatusCode.OK), isOnDisk);
            Assert.AreEqual(GetValue(key) + SetIncrement, output.retrievedValue);
        }

        [Test]
        [Category("FasterKV")]
        [Category("Smoke"), Category("RMW")]
        public void DeleteIfValueEqualsTest([Values] bool isOnDisk)
        {
            InitialIncrement();
            MaybeEvict(isOnDisk);
            const TestOp testOp = TestOp.DeleteIfValueEquals;
            var key = ModifyKey;
            Status expectedFoundRmwStatus = isOnDisk ? new(StatusCode.CopyUpdatedRecord) : new(StatusCode.InPlaceUpdatedRecord);

            VerifyKeyNotCreated(testOp, isOnDisk);

            // Value equals - delete it
            ExpirationInput input = new() { testOp = testOp, comparisonValue = GetValue(key) + 1 };
            ExpirationOutput output = ExecuteRMW(key, ref input, isOnDisk, expectedFoundRmwStatus);
            Assert.AreEqual(isOnDisk ? Funcs.DidCopyUpdate : Funcs.InPlaceUpdater, output.functionsCalled);
            Assert.AreEqual(ExpirationResult.Deleted, output.result);

            // Verify it's not there
            GetRecord(key, new(StatusCode.NotFound), isOnDisk);

            // Value doesn't equal - no-op
            key += 1;   // We deleted ModifyKey so get the next-higher key
            input = new() { testOp = testOp, comparisonValue = -1 };
            output = ExecuteRMW(key, ref input, isOnDisk, GetMutableVsOnDiskStatus(isOnDisk));
            Assert.AreEqual(isOnDisk ? Funcs.SkippedCopyUpdate : Funcs.InPlaceUpdater, output.functionsCalled);
            Assert.AreEqual(isOnDisk ? ExpirationResult.None : ExpirationResult.NotDeleted, output.result);
            Assert.AreEqual(isOnDisk ? 0 : GetValue(key), output.retrievedValue);

            // Verify it's there with unchanged value; note that it has not been InitialIncrement()ed
            output = GetRecord(key, new(StatusCode.OK), isOnDisk);
            Assert.AreEqual(GetValue(key), output.retrievedValue);
        }

        [Test]
        [Category("FasterKV")]
        [Category("Smoke"), Category("RMW")]
        public void DeleteIfValueNotEqualsTest([Values] bool isOnDisk)
        {
            InitialIncrement();
            MaybeEvict(isOnDisk);
            const TestOp testOp = TestOp.DeleteIfValueNotEquals;
            var key = ModifyKey;
            Status expectedFoundRmwStatus = isOnDisk ? new(StatusCode.CopyUpdatedRecord) : new(StatusCode.InPlaceUpdatedRecord);

            VerifyKeyNotCreated(testOp, isOnDisk);

            // Value equals - no-op
            ExpirationInput input = new() { testOp = testOp, comparisonValue = GetValue(key) + 1 };
            ExpirationOutput output = ExecuteRMW(key, ref input, isOnDisk, GetMutableVsOnDiskStatus(isOnDisk));
            Assert.AreEqual(isOnDisk ? Funcs.SkippedCopyUpdate : Funcs.InPlaceUpdater, output.functionsCalled);
            Assert.AreEqual(isOnDisk ? ExpirationResult.None : ExpirationResult.NotDeleted, output.result);
            Assert.AreEqual(isOnDisk ? 0 : GetValue(key) + 1, output.retrievedValue);

            // Verify it's there with unchanged value
            output = GetRecord(key, new(StatusCode.OK), isOnDisk);
            Assert.AreEqual(GetValue(key) + 1, output.retrievedValue);

            // Value doesn't equal - delete it
            input = new() { testOp = testOp, comparisonValue = -1 };
            output = ExecuteRMW(key, ref input, isOnDisk, expectedFoundRmwStatus);
            Assert.AreEqual(isOnDisk ? Funcs.DidCopyUpdate : Funcs.InPlaceUpdater, output.functionsCalled);
            Assert.AreEqual(ExpirationResult.Deleted, output.result);

            // Verify it's not there
            GetRecord(key, new(StatusCode.NotFound), isOnDisk);
        }
    }
}
