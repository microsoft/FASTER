// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using NUnit.Framework;
using System.Collections.Generic;
using System.Threading.Tasks;
using static FASTER.test.TestUtils;

namespace FASTER.test
{
    public struct LocalKeyStructComparer : IFasterEqualityComparer<KeyStruct>
    {
        internal long? forceCollisionHash;

        public long GetHashCode64(ref KeyStruct key)
        {
            return forceCollisionHash.HasValue ? forceCollisionHash.Value : Utility.GetHashCode(key.kfield1);
        }
        public bool Equals(ref KeyStruct k1, ref KeyStruct k2)
        {
            return k1.kfield1 == k2.kfield1 && k1.kfield2 == k2.kfield2;
        }

        public override string ToString() => $"forceHashCollision: {forceCollisionHash}";
    }

    [TestFixture]
    class CompletePendingTests
    {
        private FasterKV<KeyStruct, ValueStruct> fht;
        private IDevice log;
        LocalKeyStructComparer comparer = new();

        [SetUp]
        public void Setup()
        {
            // Clean up log files from previous test runs in case they weren't cleaned up
            DeleteDirectory(MethodTestDir, wait: true);

            log = Devices.CreateLogDevice($"{MethodTestDir}/CompletePendingTests.log", preallocateFile: true, deleteOnClose: true);
            fht = new FasterKV<KeyStruct, ValueStruct>(128, new LogSettings { LogDevice = log, MemorySizeBits = 29 }, comparer: comparer);
        }

        [TearDown]
        public void TearDown()
        {
            fht?.Dispose();
            fht = null;
            log?.Dispose();
            log = null;
            DeleteDirectory(MethodTestDir, wait: true);
        }

        const int numRecords = 1000;

        static KeyStruct NewKeyStruct(int key) => new() { kfield1 = key, kfield2 = key + numRecords * 10 };
        static ValueStruct NewValueStruct(int key) => new() { vfield1 = key, vfield2 = key + numRecords * 10 };

        static InputStruct NewInputStruct(int key) => new() { ifield1 = key + numRecords * 30, ifield2 = key + numRecords * 40 };
        static ContextStruct NewContextStruct(int key) => new() { cfield1 = key + numRecords * 50, cfield2 = key + numRecords * 60 };

        static void VerifyStructs(int key, ref KeyStruct keyStruct, ref InputStruct inputStruct, ref OutputStruct outputStruct, ref ContextStruct contextStruct, bool useRMW)
        {
            Assert.AreEqual(key, keyStruct.kfield1);
            Assert.AreEqual(key + numRecords * 10, keyStruct.kfield2);
            Assert.AreEqual(key + numRecords * 30, inputStruct.ifield1);
            Assert.AreEqual(key + numRecords * 40, inputStruct.ifield2);

            // RMW causes the InPlaceUpdater to be called, which adds input fields to the value.
            Assert.AreEqual(key + (useRMW ? inputStruct.ifield1 : 0), outputStruct.value.vfield1);
            Assert.AreEqual(key + numRecords * 10 + (useRMW ? inputStruct.ifield2 : 0), outputStruct.value.vfield2);
            Assert.AreEqual(key + numRecords * 50, contextStruct.cfield1);
            Assert.AreEqual(key + numRecords * 60, contextStruct.cfield2);
        }

        class ProcessPending
        {
            // Get the first chunk of outputs as a group, testing realloc.
            private int deferredPendingMax = CompletedOutputIterator<KeyStruct, ValueStruct, InputStruct, OutputStruct, ContextStruct>.kInitialAlloc + 1;
            private int deferredPending = 0;
            internal Dictionary<int, long> keyAddressDict = new();
            private bool isFirst = true;

            internal bool IsFirst()
            {
                var temp = this.isFirst;
                this.isFirst = false;
                return temp;
            }

            internal bool DeferPending()
            {
                if (deferredPending < deferredPendingMax)
                {
                    ++deferredPending;
                    return true;
                }
                return false;
            }

            internal void Process(CompletedOutputIterator<KeyStruct, ValueStruct, InputStruct, OutputStruct, ContextStruct> completedOutputs, List<(KeyStruct, long)> rmwCopyUpdatedAddresses)
            {
                var useRMW = rmwCopyUpdatedAddresses is not null;
                Assert.AreEqual(CompletedOutputIterator<KeyStruct, ValueStruct, InputStruct, OutputStruct, ContextStruct>.kInitialAlloc *
                                CompletedOutputIterator<KeyStruct, ValueStruct, InputStruct, OutputStruct, ContextStruct>.kReallocMultuple, completedOutputs.vector.Length);
                Assert.AreEqual(deferredPending, completedOutputs.maxIndex);
                Assert.AreEqual(-1, completedOutputs.currentIndex);

                var count = 0;
                for (; completedOutputs.Next(); ++count)
                {
                    ref var result = ref completedOutputs.Current;
                    VerifyStructs((int)result.Key.kfield1, ref result.Key, ref result.Input, ref result.Output, ref result.Context, useRMW);
                    if (!useRMW)
                        Assert.AreEqual(keyAddressDict[(int)result.Key.kfield1], result.RecordMetadata.Address);
                    else if (keyAddressDict[(int)result.Key.kfield1] != result.RecordMetadata.Address)
                        rmwCopyUpdatedAddresses.Add((result.Key, result.RecordMetadata.Address));
                }
                completedOutputs.Dispose();
                Assert.AreEqual(deferredPending + 1, count);
                Assert.AreEqual(-1, completedOutputs.maxIndex);
                Assert.AreEqual(-1, completedOutputs.currentIndex);

                deferredPending = 0;
                deferredPendingMax /= 2;
            }

            internal void VerifyNoDeferredPending()
            {
                Assert.AreEqual(0, this.deferredPendingMax);  // This implicitly does a null check as well as ensures processing actually happened
                Assert.AreEqual(0, this.deferredPending);
            }

            internal static void VerifyOneNotFound(CompletedOutputIterator<KeyStruct, ValueStruct, InputStruct, OutputStruct, ContextStruct> completedOutputs, ref KeyStruct keyStruct)
            {
                Assert.IsTrue(completedOutputs.Next());
                Assert.IsFalse(completedOutputs.Current.Status.Found);
                Assert.AreEqual(keyStruct, completedOutputs.Current.Key);
                Assert.IsFalse(completedOutputs.Next());
                completedOutputs.Dispose();
            }
        }

        [Test]
        [Category("FasterKV")]
        public async ValueTask ReadAndCompleteWithPendingOutput([Values] bool useRMW, [Values] bool isAsync)
        {
            using var session = fht.For(new FunctionsWithContext<ContextStruct>()).NewSession<FunctionsWithContext<ContextStruct>>();
            Assert.IsNull(session.completedOutputs);    // Do not instantiate until we need it

            ProcessPending processPending = new();

            for (var key = 0; key < numRecords; ++key)
            {
                var keyStruct = NewKeyStruct(key);
                var valueStruct = NewValueStruct(key);
                processPending.keyAddressDict[key] = fht.Log.TailAddress;
                session.Upsert(ref keyStruct, ref valueStruct);
            }

            // Flush to make reads or RMWs go pending.
            fht.Log.FlushAndEvict(wait: true);

            List<(KeyStruct key, long address)> rmwCopyUpdatedAddresses = new();

            for (var key = 0; key < numRecords; ++key)
            {
                var keyStruct = NewKeyStruct(key);
                var inputStruct = NewInputStruct(key);
                var contextStruct = NewContextStruct(key);
                OutputStruct outputStruct = default;

                if ((key % (numRecords / 10)) == 0)
                {
                    var ksUnfound = keyStruct;
                    ksUnfound.kfield1 += numRecords * 10;
                    if (session.Read(ref ksUnfound, ref inputStruct, ref outputStruct, contextStruct).IsPending)
                    {
                        CompletedOutputIterator<KeyStruct, ValueStruct, InputStruct, OutputStruct, ContextStruct> completedOutputs;
                        if (isAsync)
                            completedOutputs = await session.CompletePendingWithOutputsAsync();
                        else
                            session.CompletePendingWithOutputs(out completedOutputs, wait: true);
                        ProcessPending.VerifyOneNotFound(completedOutputs, ref ksUnfound);
                    }
                }

                // We don't use context (though we verify it), and Read does not use input.
                var status = useRMW
                    ? session.RMW(ref keyStruct, ref inputStruct, ref outputStruct, contextStruct)
                    : session.Read(ref keyStruct, ref inputStruct, ref outputStruct, contextStruct);
                if (status.IsPending)
                {
                    if (processPending.IsFirst())
                    {
                        session.CompletePending(wait: true);        // Test that this does not instantiate CompletedOutputIterator
                        Assert.IsNull(session.completedOutputs);    // Do not instantiate until we need it
                        continue;
                    }

                    if (!processPending.DeferPending())
                    {
                        CompletedOutputIterator<KeyStruct, ValueStruct, InputStruct, OutputStruct, ContextStruct> completedOutputs;
                        if (isAsync)
                            completedOutputs = await session.CompletePendingWithOutputsAsync();
                        else
                            session.CompletePendingWithOutputs(out completedOutputs, wait: true);
                        processPending.Process(completedOutputs, useRMW ? rmwCopyUpdatedAddresses : null);
                    }
                    continue;
                }
                Assert.IsTrue(status.Found);
            }
            processPending.VerifyNoDeferredPending();

            // If we are using RMW, then all records were pending and updated their addresses, and we skipped the first one in the loop above.
            if (useRMW)
                Assert.AreEqual(numRecords - 1, rmwCopyUpdatedAddresses.Count);

            foreach (var (key, address) in rmwCopyUpdatedAddresses)
            {
                // ConcurrentReader does not verify the input struct.
                InputStruct inputStruct = default;
                OutputStruct outputStruct = default;
                ReadOptions readOptions = default;

                // This should not be pending since we've not flushed.
                var localKey = key;
                var status = session.Read(ref localKey, ref inputStruct, ref outputStruct, ref readOptions, out RecordMetadata recordMetadata);
                Assert.IsFalse(status.IsPending);
                Assert.AreEqual(address, recordMetadata.Address);
            }
        }

        public enum StartAddressMode
        {
            UseStartAddress,
            NoStartAddress
        }

        public class PendingReadFunctions<TContext> : FunctionsBase<KeyStruct, ValueStruct, InputStruct, OutputStruct, Empty>
        {
            public override void ReadCompletionCallback(ref KeyStruct key, ref InputStruct input, ref OutputStruct output, Empty ctx, Status status, RecordMetadata recordMetadata)
            {
                Assert.IsTrue(status.Found);
                Assert.AreEqual(key.kfield1, output.value.vfield1);
                // Do not compare field2; that's our updated value, and the key won't be found if we change kfield2
            }

            // Read functions
            public override bool SingleReader(ref KeyStruct key, ref InputStruct input, ref ValueStruct value, ref OutputStruct dst, ref ReadInfo readInfo)
            {
                Assert.IsFalse(readInfo.RecordInfo.IsNull());
                dst.value = value;
                return true;
            }

            public override bool ConcurrentReader(ref KeyStruct key, ref InputStruct input, ref ValueStruct value, ref OutputStruct dst, ref ReadInfo readInfo)
                => SingleReader(ref key, ref input, ref value, ref dst, ref readInfo);
        }

        [Test]
        [Category("FasterKV")]
        public void ReadPendingWithNewSameKey([Values] StartAddressMode startAddressMode, [Values(FlushMode.NoFlush, FlushMode.OnDisk)] FlushMode secondRecordFlushMode)
        {
            const int valueMult = 1000;

            using var session = fht.For(new PendingReadFunctions<ContextStruct>()).NewSession<PendingReadFunctions<ContextStruct>>();

            // Store off startAddress before initial upsert
            var startAddress = startAddressMode == StartAddressMode.UseStartAddress ? fht.Log.TailAddress : Constants.kInvalidAddress;

            // Insert first record
            var firstValue = 0; // same as key
            var keyStruct = new KeyStruct { kfield1 = firstValue, kfield2 = firstValue * valueMult };
            var valueStruct = new ValueStruct { vfield1 = firstValue, vfield2 = firstValue * valueMult };
            session.Upsert(ref keyStruct, ref valueStruct);

            // Flush to make the Read() go pending.
            fht.Log.FlushAndEvict(wait: true);

            ReadOptions readOptions = new() { StartAddress = startAddress };
            var (status, outputStruct) = session.Read(keyStruct, ref readOptions);
            Assert.IsTrue(status.IsPending, $"Expected status.IsPending: {status}");

            // Insert next record with the same key and flush this too if requested.
            var secondValue = firstValue + 1;
            valueStruct.vfield2 = secondValue * valueMult;
            session.Upsert(ref keyStruct, ref valueStruct);
            if (secondRecordFlushMode == FlushMode.OnDisk)
                fht.Log.FlushAndEvict(wait: true);

            session.CompletePendingWithOutputs(out var completedOutputs, wait: true);
            (status, outputStruct) = GetSinglePendingResult(completedOutputs);

            if (startAddressMode == StartAddressMode.UseStartAddress)
                Assert.AreEqual(firstValue * valueMult, outputStruct.value.vfield2, "UseStartAddress should have returned first value");
            else
                Assert.AreEqual(secondValue * valueMult, outputStruct.value.vfield2, "NoStartAddress should have returned second value");
        }

        [Test]
        [Category("FasterKV")]
        public void ReadPendingWithNewDifferentKeyInChain([Values] StartAddressMode startAddressMode, [Values(FlushMode.NoFlush, FlushMode.OnDisk)] FlushMode secondRecordFlushMode)
        {
            const int valueMult = 1000;

            using var session = fht.For(new PendingReadFunctions<ContextStruct>()).NewSession<PendingReadFunctions<ContextStruct>>();

            // Store off startAddress before initial upsert
            var startAddress = startAddressMode == StartAddressMode.UseStartAddress ? fht.Log.TailAddress : Constants.kInvalidAddress;

            // Insert first record
            var firstValue = 0; // same as key
            var keyStruct = new KeyStruct { kfield1 = firstValue, kfield2 = firstValue * valueMult };
            var valueStruct = new ValueStruct { vfield1 = firstValue, vfield2 = firstValue * valueMult };
            session.Upsert(ref keyStruct, ref valueStruct);

            // Force collisions to test having another key in the chain
            comparer.forceCollisionHash = keyStruct.GetHashCode64(ref keyStruct);

            // Flush to make the Read() go pending.
            fht.Log.FlushAndEvict(wait: true);

            ReadOptions readOptions = new() { StartAddress = startAddress };
            var (status, outputStruct) = session.Read(keyStruct, ref readOptions);
            Assert.IsTrue(status.IsPending, $"Expected status.IsPending: {status}");

            // Insert next record with a different key and flush this too if requested.
            var secondValue = firstValue + 1;
            keyStruct = new() { kfield1 = secondValue, kfield2 = secondValue * valueMult };
            valueStruct = new() { vfield1 = secondValue, vfield2 = secondValue * valueMult };
            session.Upsert(ref keyStruct, ref valueStruct);
            if (secondRecordFlushMode == FlushMode.OnDisk)
                fht.Log.FlushAndEvict(wait: true);

            session.CompletePendingWithOutputs(out var completedOutputs, wait: true);
            (status, outputStruct) = GetSinglePendingResult(completedOutputs);

            Assert.AreEqual(firstValue * valueMult, outputStruct.value.vfield2, "Should have returned first value");
        }

        [Test]
        [Category("FasterKV")]
        public void ReadPendingWithNoNewKey([Values] StartAddressMode startAddressMode)
        {
            // Basic test of pending read
            const int valueMult = 1000;

            using var session = fht.For(new PendingReadFunctions<ContextStruct>()).NewSession<PendingReadFunctions<ContextStruct>>();

            // Store off startAddress before initial upsert
            var startAddress = startAddressMode == StartAddressMode.UseStartAddress ? fht.Log.TailAddress : Constants.kInvalidAddress;

            // Insert first record
            var firstValue = 0; // same as key
            var keyStruct = new KeyStruct { kfield1 = firstValue, kfield2 = firstValue * valueMult };
            var valueStruct = new ValueStruct { vfield1 = firstValue, vfield2 = firstValue * valueMult };
            session.Upsert(ref keyStruct, ref valueStruct);

            // Flush to make the Read() go pending.
            fht.Log.FlushAndEvict(wait: true);

            ReadOptions readOptions = new() { StartAddress = startAddress };
            var (status, outputStruct) = session.Read(keyStruct, ref readOptions);
            Assert.IsTrue(status.IsPending, $"Expected status.IsPending: {status}");

            session.CompletePendingWithOutputs(out var completedOutputs, wait: true);
            (status, outputStruct) = GetSinglePendingResult(completedOutputs);

            Assert.AreEqual(firstValue * valueMult, outputStruct.value.vfield2, "Should have returned first value");
        }
    }
}
