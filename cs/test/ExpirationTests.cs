// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Runtime.CompilerServices;
using FASTER.core;
using NUnit.Framework;
using static FASTER.test.TestUtils;

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

        [Flags] internal enum Funcs { Invalid = 0, NeedInitialUpdate = 0x0001, NeedCopyUpdate = 0x0002, InPlaceUpdater = 0x0004, InitialUpdater = 0x0008, CopyUpdater = 0x0010, 
                                      SingleReader = 0x0020, ConcurrentReader = 0x0040,
                                      RMWCompletionCallback = 0x0100, ReadCompletionCallback = 0x0200,
                                      SkippedCopyUpdate = NeedCopyUpdate | RMWCompletionCallback,
                                      DidCopyUpdate = NeedCopyUpdate | CopyUpdater,
                                      DidCopyUpdateCC = DidCopyUpdate | RMWCompletionCallback,
                                      DidInitialUpdate = NeedInitialUpdate | InitialUpdater};

        internal enum ExpirationResult
        { 
            None,                       // Default value
            Incremented,                // Initial increment was done
            ExpireDelete,               // Record was expired so deleted
            ExpireRollover,             // Record was expired and reinitialized within the IFunctions call
            Updated,                    // Record was updated normally
            NotUpdated,                 // Record was not updated
            Deleted,                    // Record was expired with AndStop (no reinitialization done)
            DeletedThenUpdated,         // Record was expired then InitialUpdate'd within the original record space
            DeletedThenUpdateRejected,  // Record was expired then InitialUpdate within the original record space was rejected
            DeletedThenInserted,        // Record was expired and not InitialUpdate'd within the original record space, so RMW inserted a record with the reinitialized value
            NotDeleted
        };

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
                                                //      IPU sets rwmInfo.DeleteRecord and returns false; we see this and TryRemoveDeletedHashEntry and return SUCCESS
                                                //  OnDisk:
                                                //      CU sets rmwInfo.DeleteRecord and returns false; FASTER sets the Tombstone and the operation proceeds as normal otherwise
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

            DeleteIfValueEqualsThenUpdate,      // Delete the record for a key if the current value equals a specified value, then update in the same record space
            DeleteIfValueEqualsThenInsert,      // Delete the record for a key if the current value equals a specified value, then insert a new record 
            DeleteIfValueEqualsAndStop,         // Delete the record for a key if the current value equals a specified value, then stop (leave the tombstoned record there)
                                                //  Mutable:
                                                //      Equals: Same as ExpireDelete
                                                //      NotEquals: no-op and return true from IPU
                                                //  OnDisk:
                                                //      Equals: Same as ExpireDelete
                                                //      NotEquals: return false from NCU
                                                //  NotExists: Return false from NIU

            DeleteIfValueNotEqualsThenUpdate,   // Delete the record for a key if the current value does not equal a specified value, then update in the same record space
            DeleteIfValueNotEqualsThenInsert,   // Delete the record for a key if the current value does not equal a specified value, then insert a new record 
            DeleteIfValueNotEqualsAndStop,      // Delete the record for a key if the current value does not equal a specified value, then stop (leave the tombstoned record there)
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
                    case TestOp.DeleteIfValueEqualsThenInsert:
                    case TestOp.DeleteIfValueNotEqualsThenInsert:
                        // This means we are on the "handle expiration" sequence after IPU/CU
                        if (output.result == ExpirationResult.Deleted)
                        {
                            // Reject the update-in-original-record
                            output.result = ExpirationResult.DeletedThenUpdateRejected;
                            // return false;
                        }
                        return output.result == ExpirationResult.DeletedThenUpdateRejected;
                    case TestOp.DeleteIfValueEqualsThenUpdate:
                    case TestOp.DeleteIfValueNotEqualsThenUpdate:
                        // This means we are on the "handle expiration" sequence after IPU/CU
                        return output.result == ExpirationResult.Deleted;
                    case TestOp.DeleteIfValueEqualsAndStop:
                    case TestOp.DeleteIfValueNotEqualsAndStop:
                        // AndStop should have returned from RMW instead during the test, but this is legitimately called from VerifyKeyNotCreated
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
                    case TestOp.DeleteIfValueEqualsThenUpdate:
                    case TestOp.DeleteIfValueEqualsThenInsert:
                    case TestOp.DeleteIfValueEqualsAndStop:
                        return oldValue.field1 == input.comparisonValue;
                    case TestOp.DeleteIfValueNotEqualsThenUpdate:
                    case TestOp.DeleteIfValueNotEqualsThenInsert:
                    case TestOp.DeleteIfValueNotEqualsAndStop:
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
            public override bool CopyUpdater(ref int key, ref ExpirationInput input, ref VLValue oldValue, ref VLValue newValue, ref ExpirationOutput output, ref RMWInfo rmwInfo)
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
                        return true;
                    case TestOp.ExpireDelete:
                        Assert.AreEqual(GetValue(key) + 1, oldValue.field1);    // For this test we only call this operation when the value will expire
                        newValue.field1 = oldValue.field1 + 1;
                        rmwInfo.Action = RMWAction.ExpireAndResume;
                        output.result = ExpirationResult.ExpireDelete;
                        return true;
                    case TestOp.ExpireRollover:
                        Assert.AreEqual(GetValue(key) + 1, oldValue.field1);    // For this test we only call this operation when the value will expire
                        newValue.field1 = GetValue(key);
                        output.result = ExpirationResult.ExpireRollover;
                        output.retrievedValue = newValue.field1;
                        return true;
                    case TestOp.SetIfKeyExists:
                        newValue.field1 = input.value;
                        output.result = ExpirationResult.Updated;
                        output.retrievedValue = newValue.field1;
                        return true;
                    case TestOp.SetIfKeyNotExists:
                        Assert.Fail($"{input.testOp} should not get here");
                        return false;
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
                        return true;
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
                        return true;
                    case TestOp.DeleteIfValueEqualsThenUpdate:
                    case TestOp.DeleteIfValueEqualsThenInsert:
                    case TestOp.DeleteIfValueEqualsAndStop:
                        if (oldValue.field1 == input.comparisonValue)
                        {
                            // Both "ThenXxx" options will go to InitialUpdater; that will decide whether to return false
                            rmwInfo.Action = input.testOp == TestOp.DeleteIfValueEqualsAndStop ? RMWAction.ExpireAndStop : RMWAction.ExpireAndResume;
                            output.result = ExpirationResult.Deleted;
                            return false;
                        }
                        Assert.Fail("Should have returned false from NeedCopyUpdate");
                        return false;
                    case TestOp.DeleteIfValueNotEqualsThenUpdate:
                    case TestOp.DeleteIfValueNotEqualsThenInsert:
                    case TestOp.DeleteIfValueNotEqualsAndStop:
                        if (oldValue.field1 != input.comparisonValue)
                        {
                            // Both "ThenXxx" options will go to InitialUpdater; that will decide whether to return false
                            rmwInfo.Action = input.testOp == TestOp.DeleteIfValueNotEqualsAndStop ? RMWAction.ExpireAndStop : RMWAction.ExpireAndResume;
                            output.result = ExpirationResult.Deleted;
                            return false;
                        }
                        Assert.Fail("Should have returned false from NeedCopyUpdate");
                        return false;
                    case TestOp.Revivify:
                        Assert.Fail($"{input.testOp} should not get here");
                        return false;
                    default:
                        Assert.Fail($"Unexpected testOp: {input.testOp}");
                        return false;
                }
            }

            public override bool InitialUpdater(ref int key, ref ExpirationInput input, ref VLValue value, ref ExpirationOutput output, ref RMWInfo rmwInfo)
            {
                output.AddFunc(Funcs.InitialUpdater);
                value.length = 2;
                value.field1 = input.value;

                // If InPlaceUpdater returned Delete, let the caller know both operations happened. Similarly, we may be 
                output.result = output.result switch
                {
                    ExpirationResult.Deleted => ExpirationResult.DeletedThenUpdated,
                    ExpirationResult.DeletedThenUpdated => ExpirationResult.DeletedThenInserted,
                    ExpirationResult.DeletedThenUpdateRejected => ExpirationResult.DeletedThenInserted,
                    _ => ExpirationResult.Updated
                };
                output.retrievedValue = value.field1;

                // If this is the first InitialUpdater after a Delete and the testOp is *ThenInsert, we have to fail the first InitialUpdater
                // (which is the InitialUpdater call on the deleted record's space) and will pass the second InitialUpdater (which is into a new record).
                if (output.result == ExpirationResult.DeletedThenUpdated 
                    && (input.testOp == TestOp.DeleteIfValueEqualsThenInsert || input.testOp == TestOp.DeleteIfValueNotEqualsThenInsert))
                    return false;
                return true;
            }

            public override bool InPlaceUpdater(ref int key, ref ExpirationInput input, ref VLValue value, ref ExpirationOutput output, ref RMWInfo rmwInfo)
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
                        rmwInfo.Action = RMWAction.ExpireAndStop;
                        output.result = ExpirationResult.ExpireDelete;
                        return false;
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
                    case TestOp.DeleteIfValueEqualsThenUpdate:
                    case TestOp.DeleteIfValueEqualsThenInsert:
                    case TestOp.DeleteIfValueEqualsAndStop:
                        if (value.field1 == input.comparisonValue)
                        {
                            // Both "ThenXxx" options will go to InitialUpdater; that will decide whether to return false
                            rmwInfo.Action = input.testOp == TestOp.DeleteIfValueEqualsAndStop ? RMWAction.ExpireAndStop : RMWAction.ExpireAndResume;
                            output.result = ExpirationResult.Deleted;
                            return false;
                        }
                        output.result = ExpirationResult.NotDeleted;
                        output.retrievedValue = value.field1;
                        return true;

                    case TestOp.DeleteIfValueNotEqualsThenUpdate:
                    case TestOp.DeleteIfValueNotEqualsThenInsert:
                    case TestOp.DeleteIfValueNotEqualsAndStop:
                        if (value.field1 != input.comparisonValue)
                        {
                            // Both "ThenXxx" options will go to InitialUpdater; that will decide whether to return false
                            rmwInfo.Action = input.testOp == TestOp.DeleteIfValueNotEqualsAndStop ? RMWAction.ExpireAndStop : RMWAction.ExpireAndResume;
                            output.result = ExpirationResult.Deleted;
                            return false;
                        }
                        output.result = ExpirationResult.NotDeleted;
                        output.retrievedValue = value.field1;
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
            public override bool SingleReader(ref int key, ref ExpirationInput input, ref VLValue value, ref ExpirationOutput output, ref ReadInfo readInfo)
            {
                output.AddFunc(Funcs.SingleReader);
                if (IsExpired(key, value.field1))
                    return false;
                output.retrievedValue = value.field1;
                return true;
            }

            public override bool ConcurrentReader(ref int key, ref ExpirationInput input, ref VLValue value, ref ExpirationOutput output, ref ReadInfo readInfo)
            {
                output.AddFunc(Funcs.ConcurrentReader);
                if (IsExpired(key, value.field1))
                    return false;
                output.retrievedValue = value.field1;
                return true;
            }

            // Upsert functions
            public override bool SingleWriter(ref int key, ref ExpirationInput input, ref VLValue src, ref VLValue dst, ref ExpirationOutput output, ref UpsertInfo upsertInfo, WriteReason reason)
            {
                src.CopyTo(ref dst);
                return true;
            }

            public override bool ConcurrentWriter(ref int key, ref ExpirationInput input, ref VLValue src, ref VLValue dst, ref ExpirationOutput output, ref UpsertInfo upsertInfo)
            {
                src.CopyTo(ref dst);
                return true;
            }
        }

        private string path;
        IDevice log;
        ExpirationFunctions functions;
        FasterKV<int, VLValue> fht;
        ClientSession<int, VLValue, ExpirationInput, ExpirationOutput, Empty, ExpirationFunctions, DefaultStoreFunctions<int, VLValue>> session;

        [SetUp]
        public void Setup()
        {
            path = MethodTestDir + "/";
            DeleteDirectory(MethodTestDir, wait: true);

            log = Devices.CreateLogDevice(MethodTestDir + "/hlog.log", deleteOnClose: true);
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
            DeleteDirectory(path);
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

        private ExpirationOutput GetRecord(int key, Status expectedStatus, FlushMode flushMode)
        {
            ExpirationInput input = default;
            ExpirationOutput output = new();

            var status = session.Read(ref key, ref input, ref output, Empty.Default, 0);
            if (status.IsPending)
            {
                Assert.AreNotEqual(FlushMode.NoFlush, flushMode);
                session.CompletePendingWithOutputs(out var completedOutputs, wait:true);
                (status, output) = GetSinglePendingResult(completedOutputs);
            }

            Assert.AreEqual(expectedStatus, status);
            return output;
        }

        private ExpirationOutput ExecuteRMW(int key, ref ExpirationInput input, FlushMode flushMode, Status expectedStatus = default)
        {
            ExpirationOutput output = new ();
            var status = session.RMW(ref key, ref input, ref output);
            if (status.IsPending)
            {
                Assert.AreNotEqual(FlushMode.NoFlush, flushMode);
                session.CompletePendingWithOutputs(out var completedOutputs, wait: true);
                (status, output) = GetSinglePendingResult(completedOutputs);
            }

            Assert.AreEqual(expectedStatus, status);
            return output;
        }

        private static Status GetMutableVsOnDiskStatus(FlushMode flushMode)
        {
            // The behavior is different for OnDisk vs. mutable:
            //  - Mutable results in a call to IPU which returns true, so RMW returns InPlaceUpdatedRecord.
            //  - Otherwise it results in a call to NeedCopyUpdate which returns false, so RMW returns Found
            return new(flushMode == FlushMode.NoFlush ? StatusCode.InPlaceUpdatedRecord : StatusCode.Found);
        }

        void InitialIncrement()
        {
            Populate(new Random(RandSeed));
            InitialRead(FlushMode.NoFlush, afterIncrement: false);
            IncrementValue(TestOp.Increment, FlushMode.NoFlush);
        }

        private void InitialRead(FlushMode flushMode, bool afterIncrement)
        {
            var output = GetRecord(ModifyKey, new(StatusCode.Found), flushMode);
            Assert.AreEqual(GetValue(ModifyKey) + (afterIncrement ? 1 : 0), output.retrievedValue);
            Funcs expectedFuncs = flushMode switch
            {
                FlushMode.NoFlush => Funcs.ConcurrentReader,
                FlushMode.ReadOnly => Funcs.SingleReader,
                FlushMode.OnDisk => Funcs.SingleReader | Funcs.ReadCompletionCallback,
                _ => Funcs.Invalid
            };
            Assert.AreNotEqual(expectedFuncs, Funcs.Invalid, $"Unexpected flushmode {flushMode}");
            Assert.AreEqual(expectedFuncs, output.functionsCalled);
        }

        private void IncrementValue(TestOp testOp, FlushMode flushMode)
        {
            var key = ModifyKey;
            ExpirationInput input = new() { testOp = testOp };
            Status expectedFoundRmwStatus = flushMode == FlushMode.NoFlush ? new(StatusCode.InPlaceUpdatedRecord) : new(StatusCode.CopyUpdatedRecord);
            ExpirationOutput output = ExecuteRMW(key, ref input, flushMode, expectedFoundRmwStatus);
            var expectedFuncs = GetExpectedFuncs(flushMode,
                                            noFlush: Funcs.InPlaceUpdater,
                                            readOnly: Funcs.DidCopyUpdate,
                                            onDisk: Funcs.DidCopyUpdateCC);
            Assert.AreEqual(expectedFuncs, output.functionsCalled);
            Assert.AreEqual(ExpirationResult.Incremented, output.result);
        }

        private void MaybeEvict(FlushMode flushMode)
        {
            if (flushMode == FlushMode.NoFlush)
                return;
            if (flushMode == FlushMode.ReadOnly)
                fht.Log.ShiftReadOnlyAddress(fht.Log.TailAddress, wait: true);
            else
                fht.Log.FlushAndEvict(wait: true);
            InitialRead(flushMode, afterIncrement: true);
        }

        private void VerifyKeyNotCreated(TestOp testOp, FlushMode flushMode)
        {
            var key = NoKey;
#if false // TODO - Add to ExpirationFunctions the knowledge of whether we're calling from here or the main test part
            // Key doesn't exist - no-op
            ExpirationInput input = new() { testOp = testOp, value = NoValue, comparisonValue = GetValue(key) + 1 };

            ExpirationOutput output = ExecuteRMW(key, ref input, flushMode, new(StatusCode.NotFound));
            Assert.AreEqual(Funcs.NeedInitialUpdate, output.functionsCalled);
            Assert.AreEqual(ExpirationResult.None, output.result);
            Assert.AreEqual(0, output.retrievedValue);
#endif

            // Verify it's not there
            GetRecord(key, new(StatusCode.NotFound), flushMode);
        }

        private static Funcs GetExpectedFuncs(FlushMode flushMode, Funcs noFlush, Funcs readOnly, Funcs onDisk)
        {
            var expectedFuncs = flushMode switch
            {
                FlushMode.NoFlush => noFlush,
                FlushMode.ReadOnly => readOnly,
                FlushMode.OnDisk => onDisk,
                _ => Funcs.Invalid
            };
            Assert.AreNotEqual(expectedFuncs, Funcs.Invalid, $"Unexpected flushmode {flushMode}");
            return expectedFuncs;
        }

        [Test]
        [Category("FasterKV")]
        [Category("Smoke"), Category("RMW")]
        public void PassiveExpireTest([Values] FlushMode flushMode, [Values(Phase.REST, Phase.INTERMEDIATE)] Phase phase)
        {
            InitialIncrement();
            MaybeEvict(flushMode);
            IncrementValue(TestOp.PassiveExpire, flushMode);
            session.ctx.phase = phase;
            GetRecord(ModifyKey, new(StatusCode.NotFound), flushMode);
        }

        [Test]
        [Category("FasterKV")]
        [Category("Smoke"), Category("RMW")]
        public void ExpireDeleteTest([Values] FlushMode flushMode, [Values(Phase.REST, Phase.INTERMEDIATE)] Phase phase)
        {
            InitialIncrement();
            MaybeEvict(flushMode);
            const TestOp testOp = TestOp.ExpireDelete;
            var key = ModifyKey;
            Status expectedFoundRmwStatus = flushMode == FlushMode.NoFlush ? new(StatusCode.InPlaceUpdatedRecord | StatusCode.Expired) : new(StatusCode.CopyUpdatedRecord);

            session.ctx.phase = phase;

            // Increment/Delete it
            ExpirationInput input = new() { testOp = testOp };
            ExpirationOutput output = ExecuteRMW(key, ref input, flushMode, expectedFoundRmwStatus);
            var expectedFuncs = GetExpectedFuncs(flushMode,
                                            noFlush: Funcs.InPlaceUpdater,
                                            readOnly: Funcs.DidCopyUpdate,
                                            onDisk: Funcs.DidCopyUpdateCC);
            Assert.AreEqual(expectedFuncs, output.functionsCalled);
            Assert.AreEqual(ExpirationResult.ExpireDelete, output.result);

            // Verify it's not there
            GetRecord(key, new(StatusCode.NotFound), flushMode);
        }

        [Test]
        [Category("FasterKV")]
        [Category("Smoke"), Category("RMW")]
        public void ExpireRolloverTest([Values] FlushMode flushMode, [Values(Phase.REST, Phase.INTERMEDIATE)] Phase phase)
        {
            InitialIncrement();
            MaybeEvict(flushMode);
            const TestOp testOp = TestOp.ExpireRollover;
            var key = ModifyKey;
            Status expectedFoundRmwStatus = flushMode == FlushMode.NoFlush ? new(StatusCode.InPlaceUpdatedRecord) : new(StatusCode.CopyUpdatedRecord);

            session.ctx.phase = phase;

            // Increment/Rollover to initial state
            ExpirationInput input = new() { testOp = testOp };
            ExpirationOutput output = ExecuteRMW(key, ref input, flushMode, expectedFoundRmwStatus);
            var expectedFuncs = GetExpectedFuncs(flushMode,
                                            noFlush: Funcs.InPlaceUpdater,
                                            readOnly: Funcs.DidCopyUpdate,
                                            onDisk: Funcs.DidCopyUpdateCC);
            Assert.AreEqual(expectedFuncs, output.functionsCalled);
            Assert.AreEqual(ExpirationResult.ExpireRollover, output.result);
            Assert.AreEqual(GetValue(key), output.retrievedValue);

            // Verify it's there with initial state
            output = GetRecord(key, new(StatusCode.Found), FlushMode.NoFlush /* update was appended */);
            Assert.AreEqual(GetValue(key), output.retrievedValue);
        }

        [Test]
        [Category("FasterKV")]
        [Category("Smoke"), Category("RMW")]
        public void SetIfKeyExistsTest([Values] FlushMode flushMode, [Values(Phase.REST, Phase.INTERMEDIATE)] Phase phase)
        {
            InitialIncrement();
            MaybeEvict(flushMode);
            const TestOp testOp = TestOp.SetIfKeyExists;
            var key = ModifyKey;
            Status expectedFoundRmwStatus = flushMode == FlushMode.NoFlush ? new(StatusCode.InPlaceUpdatedRecord) : new(StatusCode.CopyUpdatedRecord);

            session.ctx.phase = phase;

            // Key exists - update it
            ExpirationInput input = new() { testOp = testOp, value = GetValue(key) + SetIncrement };
            ExpirationOutput output = ExecuteRMW(key, ref input, flushMode, expectedFoundRmwStatus);
            var expectedFuncs = GetExpectedFuncs(flushMode,
                                            noFlush: Funcs.InPlaceUpdater,
                                            readOnly: Funcs.DidCopyUpdate,
                                            onDisk: Funcs.DidCopyUpdateCC);
            Assert.AreEqual(expectedFuncs, output.functionsCalled);
            Assert.AreEqual(ExpirationResult.Updated, output.result);
            Assert.AreEqual(input.value, output.retrievedValue);

            // Verify it's there with updated value
            output = GetRecord(key, new(StatusCode.Found), FlushMode.NoFlush /* update was appended */);
            Assert.AreEqual(input.value, output.retrievedValue);

            // Key doesn't exist - no-op
            key += SetIncrement;
            input = new() { testOp = testOp, value = GetValue(key) };
            output = ExecuteRMW(key, ref input, flushMode, new(StatusCode.NotFound));
            Assert.AreEqual(Funcs.NeedInitialUpdate, output.functionsCalled);

            // Verify it's not there
            GetRecord(key, new(StatusCode.NotFound), flushMode);
        }

        [Test]
        [Category("FasterKV")]
        [Category("Smoke"), Category("RMW")]
        public void SetIfKeyNotExistsTest([Values] FlushMode flushMode, [Values(Phase.REST, Phase.INTERMEDIATE)] Phase phase)
        {
            InitialIncrement();
            MaybeEvict(flushMode);
            const TestOp testOp = TestOp.SetIfKeyNotExists;
            var key = ModifyKey;

            session.ctx.phase = phase;

            // Key exists - no-op
            ExpirationInput input = new() { testOp = testOp, value = GetValue(key) + SetIncrement };
            ExpirationOutput output = ExecuteRMW(key, ref input, flushMode, GetMutableVsOnDiskStatus(flushMode));
            var expectedFuncs = GetExpectedFuncs(flushMode,
                                            noFlush: Funcs.InPlaceUpdater,
                                            readOnly: Funcs.NeedCopyUpdate,
                                            onDisk: Funcs.SkippedCopyUpdate);
            Assert.AreEqual(expectedFuncs, output.functionsCalled);

            // Verify it's there with unchanged value
            output = GetRecord(key, new(StatusCode.Found), flushMode);
            Assert.AreEqual(GetValue(key) + 1, output.retrievedValue);

            // Key doesn't exist - create it
            key += SetIncrement;
            input = new() { testOp = testOp, value = GetValue(key) };
            output = ExecuteRMW(key, ref input, flushMode, new(StatusCode.NotFound | StatusCode.CreatedRecord));
            Assert.AreEqual(Funcs.DidInitialUpdate, output.functionsCalled);
            Assert.AreEqual(ExpirationResult.Updated, output.result);
            Assert.AreEqual(input.value, output.retrievedValue);

            // Verify it's there with specified value
            output = GetRecord(key, new(StatusCode.Found), FlushMode.NoFlush /* was just added */);
            Assert.AreEqual(input.value, output.retrievedValue);
        }

        [Test]
        [Category("FasterKV")]
        [Category("Smoke"), Category("RMW")]
        public void SetIfValueEqualsTest([Values] FlushMode flushMode, [Values(Phase.REST, Phase.INTERMEDIATE)] Phase phase)
        {
            InitialIncrement();
            MaybeEvict(flushMode);
            const TestOp testOp = TestOp.SetIfValueEquals;
            var key = ModifyKey;
            Status expectedFoundRmwStatus = flushMode == FlushMode.NoFlush ? new(StatusCode.InPlaceUpdatedRecord) : new(StatusCode.CopyUpdatedRecord);

            VerifyKeyNotCreated(testOp, flushMode);
            session.ctx.phase = phase;

            // Value equals - update it
            ExpirationInput input = new() { testOp = testOp, value = GetValue(key) + SetIncrement, comparisonValue = GetValue(key) + 1 };
            ExpirationOutput output = ExecuteRMW(key, ref input, flushMode, expectedFoundRmwStatus);
            var expectedFuncs = GetExpectedFuncs(flushMode,
                                            noFlush: Funcs.InPlaceUpdater,
                                            readOnly: Funcs.DidCopyUpdate,
                                            onDisk: Funcs.DidCopyUpdateCC);
            Assert.AreEqual(expectedFuncs, output.functionsCalled);
            Assert.AreEqual(ExpirationResult.Updated, output.result);
            Assert.AreEqual(input.value, output.retrievedValue);

            // Verify it's there with updated value
            output = GetRecord(key, new(StatusCode.Found), flushMode);
            Assert.AreEqual(input.value, output.retrievedValue);

            // Value doesn't equal - no-op
            key += 1;   // We modified ModifyKey so get the next-higher key
            input = new() { testOp = testOp, value = -2, comparisonValue = -1 };
            output = ExecuteRMW(key, ref input, flushMode, GetMutableVsOnDiskStatus(flushMode));
            expectedFuncs = GetExpectedFuncs(flushMode,
                                            noFlush: Funcs.InPlaceUpdater,
                                            readOnly: Funcs.NeedCopyUpdate,
                                            onDisk: Funcs.SkippedCopyUpdate);
            Assert.AreEqual(expectedFuncs, output.functionsCalled);
            Assert.AreEqual(flushMode == FlushMode.NoFlush ? ExpirationResult.NotUpdated : ExpirationResult.None, output.result);
            Assert.AreEqual(flushMode == FlushMode.NoFlush ? GetValue(key) : 0, output.retrievedValue);

            // Verify it's there with unchanged value; note that it has not been InitialIncrement()ed
            output = GetRecord(key, new(StatusCode.Found), flushMode);
            Assert.AreEqual(GetValue(key), output.retrievedValue);
        }

        [Test]
        [Category("FasterKV")]
        [Category("Smoke"), Category("RMW")]
        public void SetIfValueNotEqualsTest([Values] FlushMode flushMode, [Values(Phase.REST, Phase.INTERMEDIATE)] Phase phase)
        {
            InitialIncrement();
            MaybeEvict(flushMode);
            const TestOp testOp = TestOp.SetIfValueNotEquals;
            var key = ModifyKey;
            Status expectedFoundRmwStatus = flushMode == FlushMode.NoFlush ? new(StatusCode.InPlaceUpdatedRecord) : new(StatusCode.CopyUpdatedRecord);

            VerifyKeyNotCreated(testOp, flushMode);
            session.ctx.phase = phase;

            // Value equals
            ExpirationInput input = new() { testOp = testOp, value = -2, comparisonValue = GetValue(key) + 1 };
            ExpirationOutput output = ExecuteRMW(key, ref input, flushMode, GetMutableVsOnDiskStatus(flushMode));
            var expectedFuncs = GetExpectedFuncs(flushMode,
                                            noFlush: Funcs.InPlaceUpdater,
                                            readOnly: Funcs.NeedCopyUpdate,
                                            onDisk: Funcs.SkippedCopyUpdate);
            Assert.AreEqual(expectedFuncs, output.functionsCalled);
            Assert.AreEqual(flushMode == FlushMode.NoFlush ? ExpirationResult.NotUpdated : ExpirationResult.None, output.result);
            Assert.AreEqual(flushMode == FlushMode.NoFlush ? GetValue(key) + 1 : 0, output.retrievedValue);

            output = GetRecord(key, new(StatusCode.Found), flushMode);
            Assert.AreEqual(GetValue(key) + 1, output.retrievedValue);

            // Value doesn't equal
            input = new() { testOp = testOp, value = GetValue(key) + SetIncrement, comparisonValue = -1 };
            output = ExecuteRMW(key, ref input, flushMode, expectedFoundRmwStatus);
            expectedFuncs = GetExpectedFuncs(flushMode,
                                            noFlush: Funcs.InPlaceUpdater,
                                            readOnly: Funcs.DidCopyUpdate,
                                            onDisk: Funcs.DidCopyUpdateCC);
            Assert.AreEqual(expectedFuncs, output.functionsCalled);
            Assert.AreEqual(ExpirationResult.Updated, output.result);
            Assert.AreEqual(GetValue(key) + SetIncrement, output.retrievedValue);

            output = GetRecord(key, new(StatusCode.Found), flushMode);
            Assert.AreEqual(GetValue(key) + SetIncrement, output.retrievedValue);
        }

        [Test]
        [Category("FasterKV")]
        [Category("Smoke"), Category("RMW")]
        public void DeleteThenUpdateTest([Values] FlushMode flushMode, [Values] KeyEquality keyEquality, [Values(Phase.REST, Phase.INTERMEDIATE)] Phase phase)
        {
            InitialIncrement();
            MaybeEvict(flushMode);
            bool isEqual = keyEquality == KeyEquality.Equal;
            TestOp testOp = isEqual ? TestOp.DeleteIfValueEqualsThenUpdate : TestOp.DeleteIfValueNotEqualsThenUpdate;
            var key = ModifyKey;
            Status expectedFoundRmwStatus = flushMode == FlushMode.NoFlush ? new(StatusCode.Expired | StatusCode.InPlaceUpdatedRecord) : new(StatusCode.NotFound | StatusCode.Expired | StatusCode.CreatedRecord);

            VerifyKeyNotCreated(testOp, flushMode);
            session.ctx.phase = phase;

            // Target value - if isEqual, the actual value of the key, else a bogus value. Delete the record, then re-initialize it from input
            var reinitValue = GetValue(key) + 1000;
            ExpirationInput input = new() { testOp = testOp, value = reinitValue, comparisonValue = isEqual ? GetValue(key) + 1 : -1 };
            ExpirationOutput output = ExecuteRMW(key, ref input, flushMode, expectedFoundRmwStatus);
            var expectedFuncs = GetExpectedFuncs(flushMode,
                                            noFlush: Funcs.InPlaceUpdater | Funcs.DidInitialUpdate,
                                            readOnly: Funcs.DidCopyUpdate | Funcs.DidInitialUpdate,
                                            onDisk: Funcs.DidCopyUpdateCC | Funcs.DidInitialUpdate);
            Assert.AreEqual(expectedFuncs, output.functionsCalled);
            Assert.AreEqual(ExpirationResult.DeletedThenUpdated, output.result);

            // Verify we did the reInitialization (this test should always have restored it with its initial values)
            output = GetRecord(key, new(StatusCode.Found), flushMode);
            Assert.AreEqual(reinitValue, output.retrievedValue);

            // No-op value - if isEqual, a bogus value, else the actual value of the key. Does nothing to the record.
            key += 1;   // We deleted ModifyKey so get the next-higher key
            input = new() { testOp = testOp, comparisonValue = isEqual ? -1 : GetValue(key) };
            output = ExecuteRMW(key, ref input, flushMode, GetMutableVsOnDiskStatus(flushMode));
            expectedFuncs = GetExpectedFuncs(flushMode,
                                            noFlush: Funcs.InPlaceUpdater,
                                            readOnly: Funcs.NeedCopyUpdate,
                                            onDisk: Funcs.SkippedCopyUpdate);
            Assert.AreEqual(expectedFuncs, output.functionsCalled);
            Assert.AreEqual(flushMode == FlushMode.NoFlush ? ExpirationResult.NotDeleted : ExpirationResult.None, output.result);
            Assert.AreEqual(flushMode == FlushMode.NoFlush ? GetValue(key) : 0, output.retrievedValue);

            // Verify it's there with unchanged value; note that it has not been InitialIncrement()ed
            output = GetRecord(key, new(StatusCode.Found), flushMode);
            Assert.AreEqual(GetValue(key), output.retrievedValue);
        }

        [Test]
        [Category("FasterKV")]
        [Category("Smoke"), Category("RMW")]
        public void DeleteThenInsertTest([Values] FlushMode flushMode, [Values] KeyEquality keyEquality, [Values(Phase.REST, Phase.INTERMEDIATE)] Phase phase)
        {
            InitialIncrement();
            MaybeEvict(flushMode);
            bool isEqual = keyEquality == KeyEquality.Equal;
            TestOp testOp = isEqual ? TestOp.DeleteIfValueEqualsThenInsert : TestOp.DeleteIfValueNotEqualsThenInsert;
            var key = ModifyKey;
            Status expectedFoundRmwStatus = flushMode == FlushMode.NoFlush ?
                new(StatusCode.InPlaceUpdatedRecord | StatusCode.Expired ) :
                new(StatusCode.NotFound | StatusCode.Expired | StatusCode.CreatedRecord);

            VerifyKeyNotCreated(testOp, flushMode);
            session.ctx.phase = phase;

            // Target value - if isEqual, the actual value of the key, else a bogus value. Delete the record, then re-initialize it from input
            var reinitValue = GetValue(key) + 1000;
            ExpirationInput input = new() { testOp = testOp, value = reinitValue, comparisonValue = isEqual ? GetValue(key) + 1 : -1 };
            ExpirationOutput output = ExecuteRMW(key, ref input, flushMode, expectedFoundRmwStatus);
            var expectedFuncs = GetExpectedFuncs(flushMode,
                                            noFlush: Funcs.DidInitialUpdate | Funcs.InPlaceUpdater,
                                            readOnly: Funcs.DidInitialUpdate | Funcs.DidCopyUpdate,
                                            onDisk: Funcs.DidInitialUpdate | Funcs.DidCopyUpdateCC);
            Assert.AreEqual(expectedFuncs, output.functionsCalled);
            Assert.AreEqual(ExpirationResult.DeletedThenInserted, output.result);

            // Verify we did the reInitialization (this test should always have restored it with its initial values)
            output = GetRecord(key, new(StatusCode.Found), flushMode);
            Assert.AreEqual(reinitValue, output.retrievedValue);
            // No-op value - if isEqual, a bogus value, else the actual value of the key. Does nothing to the record.
            key += 1;   // We deleted ModifyKey so get the next-higher key
            input = new() { testOp = testOp, comparisonValue = isEqual ? -1 : GetValue(key) };
            output = ExecuteRMW(key, ref input, flushMode, GetMutableVsOnDiskStatus(flushMode));
            expectedFuncs = GetExpectedFuncs(flushMode,
                                            noFlush: Funcs.InPlaceUpdater,
                                            readOnly: Funcs.NeedCopyUpdate,
                                            onDisk: Funcs.SkippedCopyUpdate);
            Assert.AreEqual(expectedFuncs, output.functionsCalled);
            Assert.AreEqual(flushMode == FlushMode.NoFlush ? ExpirationResult.NotDeleted : ExpirationResult.None, output.result);
            Assert.AreEqual(flushMode == FlushMode.NoFlush ? GetValue(key) : 0, output.retrievedValue);

            // Verify it's there with unchanged value; note that it has not been InitialIncrement()ed
            output = GetRecord(key, new(StatusCode.Found), flushMode);
            Assert.AreEqual(GetValue(key), output.retrievedValue);
        }

        [Test]
        [Category("FasterKV")]
        [Category("Smoke"), Category("RMW")]
        public void DeleteAndCancelIfValueEqualsTest([Values(Phase.REST, Phase.INTERMEDIATE)] Phase phase)
        {
            InitialIncrement();
            const TestOp testOp = TestOp.DeleteIfValueEqualsAndStop;
            var key = ModifyKey;
            FlushMode flushMode = FlushMode.NoFlush;
            Status expectedFoundRmwStatus = new(StatusCode.InPlaceUpdatedRecord | StatusCode.Expired);

            VerifyKeyNotCreated(testOp, flushMode);
            session.ctx.phase = phase;

            // Value equals - delete it
            ExpirationInput input = new() { testOp = testOp, comparisonValue = GetValue(key) + 1 };
            ExpirationOutput output = ExecuteRMW(key, ref input, flushMode, expectedFoundRmwStatus);
            Assert.AreEqual(Funcs.InPlaceUpdater, output.functionsCalled);
            Assert.AreEqual(ExpirationResult.Deleted, output.result);

            // Verify it's not there
            GetRecord(key, new(StatusCode.NotFound), flushMode);

            // Value doesn't equal - no-op
            key += 1;   // We deleted ModifyKey so get the next-higher key
            input = new() { testOp = testOp, comparisonValue = -1 };
            output = ExecuteRMW(key, ref input, flushMode, GetMutableVsOnDiskStatus(flushMode));
            Assert.AreEqual(flushMode == FlushMode.NoFlush ? Funcs.InPlaceUpdater : Funcs.SkippedCopyUpdate, output.functionsCalled);
            Assert.AreEqual(flushMode == FlushMode.NoFlush ? ExpirationResult.NotDeleted : ExpirationResult.None, output.result);
            Assert.AreEqual(flushMode == FlushMode.NoFlush ? GetValue(key) : 0, output.retrievedValue);

            // Verify it's there with unchanged value; note that it has not been InitialIncrement()ed
            output = GetRecord(key, new(StatusCode.Found), flushMode);
            Assert.AreEqual(GetValue(key), output.retrievedValue);
        }

        [Test]
        [Category("FasterKV")]
        [Category("Smoke"), Category("RMW")]
        public void DeleteIfValueNotEqualsTest([Values] FlushMode flushMode, [Values(Phase.REST, Phase.INTERMEDIATE)] Phase phase)
        {
            InitialIncrement();
            MaybeEvict(flushMode);
            const TestOp testOp = TestOp.DeleteIfValueNotEqualsAndStop;
            var key = ModifyKey;

            // For this test, IPU will Cancel rather than go to the IPU path
            Status expectedFoundRmwStatus = flushMode == FlushMode.NoFlush ? new(StatusCode.InPlaceUpdatedRecord | StatusCode.Expired) : new(StatusCode.CreatedRecord | StatusCode.Expired);

            VerifyKeyNotCreated(testOp, flushMode);
            session.ctx.phase = phase;

            // Value equals - no-op
            ExpirationInput input = new() { testOp = testOp, comparisonValue = GetValue(key) + 1 };
            ExpirationOutput output = ExecuteRMW(key, ref input, flushMode, GetMutableVsOnDiskStatus(flushMode));
            var expectedFuncs = GetExpectedFuncs(flushMode,
                                            noFlush: Funcs.InPlaceUpdater,
                                            readOnly: Funcs.NeedCopyUpdate,
                                            onDisk: Funcs.SkippedCopyUpdate);
            Assert.AreEqual(expectedFuncs, output.functionsCalled);
            Assert.AreEqual(flushMode == FlushMode.NoFlush ? ExpirationResult.NotDeleted : ExpirationResult.None, output.result);
            Assert.AreEqual(flushMode == FlushMode.NoFlush ? GetValue(key) + 1 : 0, output.retrievedValue);

            // Verify it's there with unchanged value
            output = GetRecord(key, new(StatusCode.Found), flushMode);
            Assert.AreEqual(GetValue(key) + 1, output.retrievedValue);

            // Value doesn't equal - delete it
            input = new() { testOp = testOp, comparisonValue = -1 };
            output = ExecuteRMW(key, ref input, flushMode, expectedFoundRmwStatus);
            expectedFuncs = GetExpectedFuncs(flushMode,
                                            noFlush: Funcs.InPlaceUpdater,
                                            readOnly: Funcs.DidCopyUpdate,
                                            onDisk: Funcs.DidCopyUpdateCC);
            Assert.AreEqual(expectedFuncs, output.functionsCalled);
            Assert.AreEqual(ExpirationResult.Deleted, output.result);

            // Verify it's not there
            GetRecord(key, new(StatusCode.NotFound), flushMode);
        }
    }
}
