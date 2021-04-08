// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Linq;
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
            log = Devices.CreateLogDevice(TestContext.CurrentContext.TestDirectory + "/CompletePendingTests.log", deleteOnClose: true);
            fht = new FasterKV<KeyStruct, ValueStruct>(128, new LogSettings { LogDevice = log, MemorySizeBits = 29 });
        }

        [TearDown]
        public void TearDown()
        {
            fht.Dispose();
            fht = null;
            log.Dispose();
        }

        const int numRecords = 500;

        static KeyStruct NewKeyStruct(int key) => new KeyStruct { kfield1 = key, kfield2 = key + numRecords * 10 };
        static ValueStruct NewValueStruct(int key) => new ValueStruct { vfield1 = key, vfield2 = key + numRecords * 10 };

        static InputStruct NewInputStruct(int key) => new InputStruct { ifield1 = key + numRecords * 30, ifield2 = key + numRecords * 40 };
        static ContextStruct NewContextStruct(int key) => new ContextStruct { cfield1 = key + numRecords * 50, cfield2 = key + numRecords * 60 };

        static void VerifyStructs(int key, ref KeyStruct keyStruct, ref InputStruct inputStruct, OutputStruct outputStruct, ContextStruct contextStruct)
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
            // Get the first few outputs in a group.
            int deferredPendingMax = 0;
            int deferredPending = 0;

            internal void Process(Func<FasterKV<KeyStruct, ValueStruct>.CompletedOutputs<InputStruct, OutputStruct, ContextStruct>> completePendingFunc)
            {
                if (deferredPending < deferredPendingMax)
                {
                    ++deferredPending;
                    return;
                }

                // Get all pending outputs
                var completedOutputs = completePendingFunc();
                Assert.AreEqual(deferredPending + 1, completedOutputs.Outputs.Count);

                foreach (var output in completedOutputs.Outputs)
                {
                    var result = output;
                    VerifyStructs((int)result.Key.kfield1, ref result.Key, ref result.Input, result.Output, result.Context);
                }
                completedOutputs.Dispose();
                deferredPending = 0;
                if (deferredPendingMax > 0)
                    --deferredPendingMax;
            }
        }

        [Test]
        [Category("FasterKV")]
        public void ReadAndCompleteWithPendingOutput()
        {
            OutputStruct outputStruct = default;

            using var session = fht.For(new FunctionsWithContext<ContextStruct>()).NewSession<FunctionsWithContext<ContextStruct>>();

            for (var ii = 0; ii < numRecords; ++ii)
            {
                var keyStruct = NewKeyStruct(ii);
                var valueStruct = NewValueStruct(ii);
                session.Upsert(ref keyStruct, ref valueStruct);
            }

            // Flush to make reads go pending.
            fht.Log.FlushAndEvict(wait: true);

            var processPending = new ProcessPending();

            for (var ii = 0; ii < numRecords; ++ii)
            {
                var keyStruct = NewKeyStruct(ii);
                var inputStruct = NewInputStruct(ii);
                var contextStruct = NewContextStruct(ii);

                // We don't use input or context, but we test that they were carried through correctly.
                var status = session.Read(ref keyStruct, ref inputStruct, ref outputStruct, contextStruct);
                if (status == Status.PENDING)
                    processPending.Process(() => { session.CompletePending(out var completedOutputs, spinWait: true); return completedOutputs; });
                else
                    Assert.IsTrue(status == Status.OK);
            }
        }

        [Test]
        [Category("FasterKV")]
        public void AdvReadAndCompleteWithPendingOutput()
        {
            OutputStruct outputStruct = default;

            using var session = fht.For(new AdvancedFunctionsWithContext<ContextStruct>()).NewSession<AdvancedFunctionsWithContext<ContextStruct>>();

            for (var ii = 0; ii < numRecords; ++ii)
            {
                var keyStruct = NewKeyStruct(ii);
                var valueStruct = NewValueStruct(ii);
                session.Upsert(ref keyStruct, ref valueStruct);
            }

            // Flush to make reads go pending.
            fht.Log.FlushAndEvict(wait: true);

            var processPending = new ProcessPending();

            for (var ii = 0; ii < numRecords; ++ii)
            {
                var keyStruct = NewKeyStruct(ii);
                var inputStruct = NewInputStruct(ii);
                var contextStruct = NewContextStruct(ii);

                // We don't use input or context, but we test that they were carried through correctly.
                var status = session.Read(ref keyStruct, ref inputStruct, ref outputStruct, contextStruct);
                if (status == Status.PENDING)
                    processPending.Process(() => { session.CompletePending(out var completedOutputs, spinWait: true); return completedOutputs; });
                else
                    Assert.IsTrue(status == Status.OK);
            }
        }
    }
}
