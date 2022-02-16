
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Threading;
using FASTER.core;
using System.IO;
using NUnit.Framework;
using System.Threading.Tasks;

namespace FASTER.test.recovery.sumstore.recover_continue
{
    [TestFixture]
    internal class RecoverContinueTests
    {
        private FasterKV<AdId, NumClicks> fht1;
        private FasterKV<AdId, NumClicks> fht2;
        private FasterKV<AdId, NumClicks> fht3;
        private IDevice log;
        private int numOps;
        private string checkpointDir;

        [SetUp]
        public void Setup()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);
            log = Devices.CreateLogDevice(TestUtils.MethodTestDir + "/RecoverContinueTests.log", deleteOnClose: true);
            checkpointDir = TestUtils.MethodTestDir + "/checkpoints3";
            Directory.CreateDirectory(checkpointDir);

            fht1 = new FasterKV
                <AdId, NumClicks>
                (16,
                logSettings: new LogSettings { LogDevice = log, MutableFraction = 0.1, MemorySizeBits = 29 },
                checkpointSettings: new CheckpointSettings { CheckpointDir = checkpointDir }
                );

            fht2 = new FasterKV
                <AdId, NumClicks>
                (16,
                logSettings: new LogSettings { LogDevice = log, MutableFraction = 0.1, MemorySizeBits = 29 },
                checkpointSettings: new CheckpointSettings { CheckpointDir = checkpointDir }
                );

            fht3 = new FasterKV
                <AdId, NumClicks>
                (16,
                logSettings: new LogSettings { LogDevice = log, MutableFraction = 0.1, MemorySizeBits = 29 },
                checkpointSettings: new CheckpointSettings { CheckpointDir = checkpointDir }
                );

            numOps = 5000;
        }

        [TearDown]
        public void TearDown()
        {
            fht1?.Dispose();
            fht2?.Dispose();
            fht3?.Dispose();
            fht1 = null;
            fht2 = null;
            fht3 = null;
            log?.Dispose();
            log = null;
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);
            TestUtils.DeleteDirectory(checkpointDir);
        }

        [Test]
        [Category("FasterKV")]
        [Category("CheckpointRestore")]
        [Category("Smoke")]
        public async ValueTask RecoverContinueTest([Values]bool isAsync)
        {
            long sno = 0;

            var firstsession = fht1.For(new AdSimpleFunctions()).NewSession<AdSimpleFunctions>("first");
            IncrementAllValues(ref firstsession, ref sno);
            fht1.TryInitiateFullCheckpoint(out _, CheckpointType.Snapshot);
            fht1.CompleteCheckpointAsync().AsTask().GetAwaiter().GetResult();
            firstsession.Dispose();

            // Check if values after checkpoint are correct
            var session1 = fht1.For(new AdSimpleFunctions()).NewSession<AdSimpleFunctions>();
            CheckAllValues(ref session1, 1);
            session1.Dispose();

            // Recover and check if recovered values are correct
            if (isAsync)
                await fht2.RecoverAsync();
            else
                fht2.Recover();
            var session2 = fht2.For(new AdSimpleFunctions()).NewSession<AdSimpleFunctions>();
            CheckAllValues(ref session2, 1);
            session2.Dispose();

            // Continue and increment values
            var continuesession = fht2.For(new AdSimpleFunctions()).ResumeSession<AdSimpleFunctions>("first", out CommitPoint cp);
            long newSno = cp.UntilSerialNo;
            Assert.AreEqual(sno - 1, newSno);
            IncrementAllValues(ref continuesession, ref sno);
            fht2.TryInitiateFullCheckpoint(out _, CheckpointType.Snapshot);
            fht2.CompleteCheckpointAsync().AsTask().GetAwaiter().GetResult();
            continuesession.Dispose();

            // Check if values after continue checkpoint are correct
            var session3 = fht2.For(new AdSimpleFunctions()).NewSession<AdSimpleFunctions>();
            CheckAllValues(ref session3, 2);
            session3.Dispose();

            // Recover and check if recovered values are correct
            if (isAsync)
                await fht3.RecoverAsync();
            else
                fht3.Recover();

            var nextsession = fht3.For(new AdSimpleFunctions()).ResumeSession<AdSimpleFunctions>("first", out cp);
            long newSno2 = cp.UntilSerialNo;
            Assert.AreEqual(sno - 1, newSno2);
            CheckAllValues(ref nextsession, 2);
            nextsession.Dispose();
        }

        private void CheckAllValues(
            ref ClientSession<AdId, NumClicks, AdInput, Output, Empty, AdSimpleFunctions> fht,
            int value)
        {
            AdInput inputArg = default;
            Output outputArg = default;
            for (var key = 0; key < numOps; key++)
            {
                inputArg.adId.adId = key;
                var status = fht.Read(ref inputArg.adId, ref inputArg, ref outputArg, Empty.Default, fht.SerialNo);

                if (status.IsPending)
                    fht.CompletePending(true);
                else
                {
                    Assert.AreEqual(value, outputArg.value.numClicks);
                }
            }

            fht.CompletePending(true);
        }

        private void IncrementAllValues(
            ref ClientSession<AdId, NumClicks, AdInput, Output, Empty, AdSimpleFunctions> fht, 
            ref long sno)
        {
            AdInput inputArg = default;
            for (int key = 0; key < numOps; key++, sno++)
            {
                inputArg.adId.adId = key;
                inputArg.numClicks.numClicks = 1;
                fht.RMW(ref inputArg.adId, ref inputArg, Empty.Default, sno);
            }
            fht.CompletePending(true);
        }


    }

    public class AdSimpleFunctions : FunctionsBase<AdId, NumClicks, AdInput, Output, Empty>
    {
        public override void ReadCompletionCallback(ref AdId key, ref AdInput input, ref Output output, Empty ctx, Status status, RecordMetadata recordMetadata)
        {
            Assert.IsTrue(status.IsFound);
            Assert.AreEqual(key.adId, output.value.numClicks);
        }

        // Read functions
        public override bool SingleReader(ref AdId key, ref AdInput input, ref NumClicks value, ref Output dst, ref RecordInfo recordInfo, long address)
        {
            dst.value = value;
            return true;
        }

        public override bool ConcurrentReader(ref AdId key, ref AdInput input, ref NumClicks value, ref Output dst, ref RecordInfo recordInfo, long address)
        {
            dst.value = value;
            return true;
        }

        // RMW functions
        public override void InitialUpdater(ref AdId key, ref AdInput input, ref NumClicks value, ref Output output, ref RecordInfo recordInfo, long address)
        {
            value = input.numClicks;
        }

        public override bool InPlaceUpdater(ref AdId key, ref AdInput input, ref NumClicks value, ref Output output, ref RecordInfo recordInfo, long address)
        {
            Interlocked.Add(ref value.numClicks, input.numClicks.numClicks);
            return true;
        }

        public override bool NeedCopyUpdate(ref AdId key, ref AdInput input, ref NumClicks oldValue, ref Output output) => true;

        public override void CopyUpdater(ref AdId key, ref AdInput input, ref NumClicks oldValue, ref NumClicks newValue, ref Output output, ref RecordInfo recordInfo, long address)
        {
            newValue.numClicks += oldValue.numClicks + input.numClicks.numClicks;
        }
    }
}
