// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using FASTER.core;
using NUnit.Framework;

namespace FASTER.test
{
    [TestFixture]
    class CompletePendingTests
    {
        private FasterKV<KeyStruct, ValueStruct> fht;
        private IDevice log;
        private string path;


        [SetUp]
        public void Setup()
        {
            path = TestUtils.MethodTestDir + "/";

            // Clean up log files from previous test runs in case they weren't cleaned up
            TestUtils.DeleteDirectory(path, wait:true);

            log = Devices.CreateLogDevice(path + "/CompletePendingTests.log", preallocateFile: true, deleteOnClose: true);
            fht = new FasterKV<KeyStruct, ValueStruct>(128, new LogSettings { LogDevice = log, MemorySizeBits = 29 });
        }

        [TearDown]
        public void TearDown()
        {
            fht?.Dispose();
            fht = null;
            log?.Dispose();
            log = null;
            TestUtils.DeleteDirectory(path, wait: true);
        }

        const int numRecords = 1000;

        static KeyStruct NewKeyStruct(int key) => new KeyStruct { kfield1 = key, kfield2 = key + numRecords * 10 };
        static ValueStruct NewValueStruct(int key) => new ValueStruct { vfield1 = key, vfield2 = key + numRecords * 10 };

        static InputStruct NewInputStruct(int key) => new InputStruct { ifield1 = key + numRecords * 30, ifield2 = key + numRecords * 40 };
        static ContextStruct NewContextStruct(int key) => new ContextStruct { cfield1 = key + numRecords * 50, cfield2 = key + numRecords * 60 };

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
            internal Dictionary<int, long> keyAddressDict = new Dictionary<int, long>();
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

            internal void VerifyOneNotFound(CompletedOutputIterator<KeyStruct, ValueStruct, InputStruct, OutputStruct, ContextStruct> completedOutputs, ref KeyStruct keyStruct)
            {
                Assert.IsTrue(completedOutputs.Next());
                Assert.AreEqual(Status.NOTFOUND, completedOutputs.Current.Status);
                Assert.AreEqual(keyStruct, completedOutputs.Current.Key);
                Assert.IsFalse(completedOutputs.Next());
                completedOutputs.Dispose();
            }
        }

        [Test]
        [Category("FasterKV")]
        public async ValueTask ReadAndCompleteWithPendingOutput([Values]bool useRMW, [Values]bool isAsync)
        {
            using var session = fht.For(new FunctionsWithContext<ContextStruct>()).NewSession<FunctionsWithContext<ContextStruct>>();
            Assert.IsNull(session.completedOutputs);    // Do not instantiate until we need it

            ProcessPending processPending = new ProcessPending();

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

                CompletedOutputIterator<KeyStruct, ValueStruct, InputStruct, OutputStruct, ContextStruct> completedOutputs;
                if ((key % (numRecords / 10)) == 0)
                {
                    var ksUnfound = keyStruct;
                    ksUnfound.kfield1 += numRecords * 10;
                    if (session.Read(ref ksUnfound, ref inputStruct, ref outputStruct, contextStruct) == Status.PENDING)
                    {
                        if (isAsync)
                            completedOutputs = await session.CompletePendingWithOutputsAsync();
                        else
                            session.CompletePendingWithOutputs(out completedOutputs, wait: true);
                        processPending.VerifyOneNotFound(completedOutputs, ref ksUnfound);
                    }
                }

                // We don't use context (though we verify it), and Read does not use input.
                var status = useRMW
                    ? session.RMW(ref keyStruct, ref inputStruct, ref outputStruct, contextStruct)
                    : session.Read(ref keyStruct, ref inputStruct, ref outputStruct, contextStruct);
                if (status == Status.PENDING)
                {
                    if (processPending.IsFirst())
                    {
                        session.CompletePending(wait: true);        // Test that this does not instantiate CompletedOutputIterator
                        Assert.IsNull(session.completedOutputs);    // Do not instantiate until we need it
                        continue;
                    }

                    if (!processPending.DeferPending())
                    {
                        if (isAsync)
                            completedOutputs = await session.CompletePendingWithOutputsAsync();
                        else
                            session.CompletePendingWithOutputs(out completedOutputs, wait: true);
                        processPending.Process(completedOutputs, useRMW ? rmwCopyUpdatedAddresses : null);
                    }
                    continue;
                }
                Assert.AreEqual(Status.OK, status);
            }
            processPending.VerifyNoDeferredPending();

            // If we are using RMW, then all records were pending and updated their addresses, and we skipped the first one in the loop above.
            if (useRMW)
                Assert.AreEqual(numRecords - 1, rmwCopyUpdatedAddresses.Count);

            foreach (var keyAndAddress in rmwCopyUpdatedAddresses)
            {
                // ConcurrentReader does not verify the input struct.
                InputStruct inputStruct = default;
                OutputStruct outputStruct = default;
                RecordMetadata recordMetadata = default;

                // This should not be pending since we've not flushed.
                var localKey = keyAndAddress.key;
                var status = session.Read(ref localKey, ref inputStruct, ref outputStruct, ref recordMetadata);
                Assert.AreNotEqual(Status.PENDING, status);
                Assert.AreEqual(keyAndAddress.address, recordMetadata.Address);
            }
        }
    }
}
