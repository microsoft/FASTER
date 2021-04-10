// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using FASTER.core;
using NUnit.Framework;

namespace FASTER.test
{
    [TestFixture]
    class CompletePendingTests
    {
        private FasterKV<KeyStruct, ValueStruct> fht;
        private IDevice log;

        [SetUp]
        public void Setup()
        {
            log = Devices.CreateLogDevice(TestContext.CurrentContext.TestDirectory + "/CompletePendingTests.log", preallocateFile: true, deleteOnClose: true);
            fht = new FasterKV<KeyStruct, ValueStruct>(128, new LogSettings { LogDevice = log, MemorySizeBits = 29 });
        }

        [TearDown]
        public void TearDown()
        {
            fht.Dispose();
            fht = null;
            log.Dispose();
        }

        const int numRecords = 1000;

        static KeyStruct NewKeyStruct(int key) => new KeyStruct { kfield1 = key, kfield2 = key + numRecords * 10 };
        static ValueStruct NewValueStruct(int key) => new ValueStruct { vfield1 = key, vfield2 = key + numRecords * 10 };

        static InputStruct NewInputStruct(int key) => new InputStruct { ifield1 = key + numRecords * 30, ifield2 = key + numRecords * 40 };
        static ContextStruct NewContextStruct(int key) => new ContextStruct { cfield1 = key + numRecords * 50, cfield2 = key + numRecords * 60 };

        static void VerifyStructs(int key, ref KeyStruct keyStruct, ref InputStruct inputStruct, ref OutputStruct outputStruct, ref ContextStruct contextStruct)
        {
            Assert.AreEqual(key, keyStruct.kfield1);
            Assert.AreEqual(key + numRecords * 10, keyStruct.kfield2);
            Assert.AreEqual(key + numRecords * 30, inputStruct.ifield1);
            Assert.AreEqual(key + numRecords * 40, inputStruct.ifield2);

            Assert.AreEqual(key, outputStruct.value.vfield1);
            Assert.AreEqual(key + numRecords * 10, outputStruct.value.vfield2);
            Assert.AreEqual(key + numRecords * 50, contextStruct.cfield1);
            Assert.AreEqual(key + numRecords * 60, contextStruct.cfield2);
        }

        class ProcessPending
        {
            // Get the first chunk of outputs as a group, testing realloc.
            internal int deferredPendingMax = CompletedOutputIterator<KeyStruct, ValueStruct, InputStruct, OutputStruct, ContextStruct>.kInitialAlloc + 1;
            internal int deferredPending = 0;

            internal void Process(Func<CompletedOutputIterator<KeyStruct, ValueStruct, InputStruct, OutputStruct, ContextStruct>> completePendingFunc)
            {
                if (deferredPending < deferredPendingMax)
                {
                    ++deferredPending;
                    return;
                }

                // Get all pending outputs
                var completedOutputs = completePendingFunc();

                Assert.AreEqual(CompletedOutputIterator<KeyStruct, ValueStruct, InputStruct, OutputStruct, ContextStruct>.kInitialAlloc *
                                CompletedOutputIterator<KeyStruct, ValueStruct, InputStruct, OutputStruct, ContextStruct>.kReallocMultuple, completedOutputs.vector.Length);
                Assert.AreEqual(deferredPending, completedOutputs.maxIndex);
                Assert.AreEqual(-1, completedOutputs.currentIndex);

                var count = 0;
                for (; completedOutputs.Next(); ++count)
                {
                    ref var result = ref completedOutputs.Current;
                    VerifyStructs((int)result.Key.kfield1, ref result.Key, ref result.Input, ref result.Output, ref result.Context);
                }
                completedOutputs.Dispose();
                Assert.AreEqual(deferredPending + 1, count);
                Assert.AreEqual(-1, completedOutputs.maxIndex);
                Assert.AreEqual(-1, completedOutputs.currentIndex);

                deferredPending = 0;
                deferredPendingMax /= 2;
            }
        }

        [Test]
        [Category("FasterKV")]
        public void ReadAndCompleteWithPendingOutput()
        {
            using var session = fht.For(new FunctionsWithContext<ContextStruct>()).NewSession<FunctionsWithContext<ContextStruct>>();
            Assert.IsNull(session.completedOutputs);    // Do not instantiate until we need it

            for (var ii = 0; ii < numRecords; ++ii)
            {
                var keyStruct = NewKeyStruct(ii);
                var valueStruct = NewValueStruct(ii);
                session.Upsert(ref keyStruct, ref valueStruct);
            }

            // Flush to make reads go pending.
            fht.Log.FlushAndEvict(wait: true);
            ProcessPending processPending = null;

            for (var ii = 0; ii < numRecords; ++ii)
            {
                var keyStruct = NewKeyStruct(ii);
                var inputStruct = NewInputStruct(ii);
                var contextStruct = NewContextStruct(ii);
                OutputStruct outputStruct = default;

                // We don't use input or context, but we test that they were carried through correctly.
                var status = session.Read(ref keyStruct, ref inputStruct, ref outputStruct, contextStruct);
                if (status == Status.PENDING)
                {
                    if (processPending is null)
                    {
                        session.CompletePending(spinWait: true);
                        Assert.IsNull(session.completedOutputs);    // Do not instantiate until we need it
                        processPending = new ProcessPending();
                        continue;
                    }
                    processPending.Process(() => { session.CompletePending(out var completedOutputs, spinWait: true); return completedOutputs; });
                    continue;
                }
                Assert.IsTrue(status == Status.OK);
            }

            Assert.AreEqual(0, processPending.deferredPendingMax);  // This implicitly does a null check as well as ensures processing actually happened
            Assert.AreEqual(0, processPending.deferredPending);
        }

        [Test]
        [Category("FasterKV")]
        public void AdvReadAndCompleteWithPendingOutput()
        {
            using var session = fht.For(new AdvancedFunctionsWithContext<ContextStruct>()).NewSession<AdvancedFunctionsWithContext<ContextStruct>>();
            Assert.IsNull(session.completedOutputs);    // Do not instantiate until we need it

            for (var ii = 0; ii < numRecords; ++ii)
            {
                var keyStruct = NewKeyStruct(ii);
                var valueStruct = NewValueStruct(ii);
                session.Upsert(ref keyStruct, ref valueStruct);
            }

            // Flush to make reads go pending.
            fht.Log.FlushAndEvict(wait: true);
            ProcessPending processPending = null;

            for (var ii = 0; ii < numRecords; ++ii)
            {
                var keyStruct = NewKeyStruct(ii);
                var inputStruct = NewInputStruct(ii);
                var contextStruct = NewContextStruct(ii);
                OutputStruct outputStruct = default;

                // We don't use input or context, but we test that they were carried through correctly.
                var status = session.Read(ref keyStruct, ref inputStruct, ref outputStruct, contextStruct);
                if (status == Status.PENDING)
                {
                    if (processPending is null)
                    {
                        session.CompletePending(spinWait: true);
                        Assert.IsNull(session.completedOutputs);    // Do not instantiate until we need it
                        processPending = new ProcessPending();
                        continue;
                    }
                    processPending.Process(() => { session.CompletePending(out var completedOutputs, spinWait: true); return completedOutputs; });
                    continue;
                }
                Assert.IsTrue(status == Status.OK);
            }

            Assert.AreEqual(0, processPending.deferredPendingMax);  // This implicitly does a null check as well as ensures processing actually happened
            Assert.AreEqual(0, processPending.deferredPending);
        }
    }
}
