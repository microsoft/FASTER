// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using Performance.Common;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace FASTER.PerfTest
{
    internal class TestInstance<TKey, TKeyManager, TValue, TOutput, TValueWrapperFactory, TValueWrapper>
        where TKey : IKey, new()
        where TKeyManager : KeyManagerBase<TKey>, IKeyManager<TKey>
        where TValue : new()
        where TOutput : new()
        where TValueWrapperFactory : IValueWrapperFactory<TValue, TOutput, TValueWrapper>
        where TValueWrapper: IValueWrapper<TValue>
    {
        private readonly TestRun testRun;

        private readonly TKeyManager keyManager;
        private readonly IFasterEqualityComparer<TKey> keyComparer;
        private readonly TValueWrapperFactory valueWrapperFactory;

        long NextChunkStart = 0;
        const int numVerboseIntervals = 10;
        const int pendingInterval = 65536;

        // Readonly for inlining benefit
        private readonly bool useAsync;
        private readonly bool doReadBatch;

        // The TKey information is specified on the constructor; the TValue and other information on the Run() call.
        internal TestInstance(TestRun testRun, TKeyManager keyManager, IFasterEqualityComparer<TKey> keyComparer, TValueWrapperFactory wrapperFactory)
        {
            this.testRun = testRun;
            this.keyManager = keyManager;
            this.keyComparer = keyComparer;
            this.valueWrapperFactory = wrapperFactory;
            this.useAsync = testRun.TestResult.Inputs.ThreadMode == ThreadMode.Async;
            this.doReadBatch = this.useAsync && testRun.TestResult.Inputs.AsyncReadBatchSize > 1;
        }

        internal bool Run<TFunctions>(SerializerSettings<TKey, TValue> serializerSettings, VariableLengthStructSettings<TKey, TValue> varLenSettings)
            where TFunctions: IFunctions<TKey, TValue, Input, TOutput, Empty>, new()
        {
            this.keyManager.CreateKeys(testRun.TestResult);

            for (testRun.currentIter = 0; testRun.currentIter < testRun.TestResult.Inputs.IterationCount; ++testRun.currentIter)
            {
                Console.WriteLine($"Iteration {testRun.currentIter + 1} of {testRun.TestResult.Inputs.IterationCount}");

                const int pauseMs = 1000;
                if (Globals.Verbose)
                {
                    var workingSetMB = (ulong)Process.GetCurrentProcess().WorkingSet64 / 1048576;
                    Console.Write($"GC.Collect and pausing for {pauseMs}ms before starting iteration {testRun.currentIter}." +
                                  $" Working set: before {workingSetMB:N0}MB, ");
                }
                GC.Collect();
                if (Globals.Verbose)
                {
                    var workingSetMB = (ulong)Process.GetCurrentProcess().WorkingSet64 / 1048576;
                    Console.WriteLine($"after {workingSetMB:N0}MB");
                }
                Thread.Sleep(pauseMs);

                var fht = new FHT<TKey, TValue, TOutput, TFunctions>(
                    false, testRun.TestResult.Inputs.HashSizeShift, testRun.TestResult.Inputs.UseObjectKey || testRun.TestResult.Inputs.UseObjectValue,
                    testRun.TestResult.Inputs, serializerSettings, varLenSettings, this.keyComparer);
                RunIteration(fht, testRun);
            }
            this.keyManager.Dispose();
            return true;
        }
        
        private bool RunIteration<TFunctions>(FHT<TKey, TValue, TOutput, TFunctions> fht, TestRun testRun)
            where TFunctions : IFunctions<TKey, TValue, Input, TOutput, Empty>, new()
        {
            Initialize(fht, testRun);
            FlushLog(fht, testRun);
            RunOperations(fht, testRun);
            fht.Close();
            return true;
        }

        private void Initialize<TFunctions>(FHT<TKey, TValue, TOutput, TFunctions> fht, TestRun testRun)
            where TFunctions : IFunctions<TKey, TValue, Input, TOutput, Empty>, new()
        {
            if (Globals.Verbose)
                Console.WriteLine($"Writing initial key values from 0 to {testRun.TestResult.Inputs.InitKeyCount} to FASTER");

            var sw = new Stopwatch();
            sw.Start();

            // Reset the global chunk tracker.
            NextChunkStart = 0;

            Globals.IsInitialInsertPhase = true;
            var tasks = Enumerable.Range(0, testRun.TestResult.Inputs.ThreadCount)
                                  .Select(threadIdx => Task.Run(() => Initialize(fht, threadIdx, testRun)));
            Task.WaitAll(tasks.ToArray());
            Globals.IsInitialInsertPhase = false;

            sw.Stop();

            testRun.InitializeMs = (ulong)sw.ElapsedMilliseconds;
            var numSec = sw.ElapsedMilliseconds / 1000.0;
            var workingSetMB = (ulong)Process.GetCurrentProcess().WorkingSet64 / 1048576;
            Console.WriteLine($"Initialization: Time to insert {testRun.TestResult.Inputs.InitKeyCount:N0} initial key values:" +
                              $" {numSec:N3} sec ({testRun.TestResult.Inputs.InitKeyCount / numSec:N2} inserts/sec;" +
                              $" {testRun.TestResult.Inputs.InitKeyCount / (numSec * testRun.TestResult.Inputs.ThreadCount):N2} thread/sec);" +
                              $" working set {workingSetMB:N0}MB");
        }

        private async Task Initialize<TFunctions>(FHT<TKey, TValue, TOutput, TFunctions> fht, int threadIndex, TestRun testRun)
            where TFunctions : IFunctions<TKey, TValue, Input, TOutput, Empty>, new()
        {
            // Each thread needs to set NUMA and create a FASTER session
            Numa.AffinitizeThread(testRun.TestResult.Inputs.NumaMode, threadIndex);
            using var session = fht.Faster.NewSession<Input, TOutput, Empty, TFunctions>(fht.Functions, $"Initialize_{threadIndex}",
                                                                                         testRun.TestResult.Inputs.ThreadMode == ThreadMode.Affinitized);
            long serialNo = 0;
            var verboseInterval = testRun.TestResult.Inputs.InitKeyCount / (testRun.TestResult.Inputs.ThreadCount * numVerboseIntervals);
            var valueWrapper = this.valueWrapperFactory.GetValueWrapper(threadIndex);

            // We just do one iteration through the KeyCount to insert the initial keys. If there are
            // multiple threads, each thread does (KeyCount / #threads) Inserts (on average).
            for (long chunkStart = Interlocked.Add(ref NextChunkStart, Globals.ChunkSize) - Globals.ChunkSize;
                chunkStart < testRun.TestResult.Inputs.InitKeyCount;
                chunkStart = Interlocked.Add(ref NextChunkStart, Globals.ChunkSize) - Globals.ChunkSize)
            {
                var chunkEnd = chunkStart + Globals.ChunkSize;
                for (var ii = chunkStart; ii < chunkEnd; ii++)
                {
                    if (serialNo % 256 == 0)
                    {
                        if (!this.useAsync)
                            session.Refresh();
                        if (++serialNo % 65536 == 0)
                        {
                            if (!this.useAsync)
                                session.CompletePending(false);
                            if (Globals.Verbose && serialNo % verboseInterval < pendingInterval)
                            {
                                long workingSetMB = Process.GetCurrentProcess().WorkingSet64 / 1048576;
                                Console.WriteLine($"tid {threadIndex}, Insert: {serialNo}, {workingSetMB:N0}MB");
                            }
                        }
                    }

                    valueWrapper.SetInitialValue(this.keyManager.GetInitKey((int)ii).Value);
                    var status = Status.OK; // TODO: UpsertAsync does not return status
                    if (this.useAsync)
                        await session.UpsertAsync(ref this.keyManager.GetInitKey((int)ii), ref valueWrapper.GetRef(), waitForCommit:false);
                    else
                        status = session.Upsert(ref this.keyManager.GetInitKey((int)ii), ref valueWrapper.GetRef(), Empty.Default, serialNo);
                    if (status != Status.OK && status != Status.PENDING)
                        throw new ApplicationException($"Error: Unexpected status in {nameof(Initialize)}; key[{ii}] = {this.keyManager.GetInitKey((int)ii).Value}: {status}");
                }
            }
            session.CompletePending(true);
        }

        private void FlushLog<TFunctions>(FHT<TKey, TValue, TOutput, TFunctions> fht, TestRun testRun)
            where TFunctions : IFunctions<TKey, TValue, Input, TOutput, Empty>, new()
        {
            if (Globals.Verbose)
                Console.WriteLine("Flushing log");
            switch (testRun.TestResult.Inputs.LogMode)
            {
                case LogMode.None:
                    break;
                case LogMode.Flush:
                    fht.Faster.Log.Flush(true);
                    break;
                case LogMode.FlushAndEvict:
                    fht.Faster.Log.FlushAndEvict(true);
                    break;
                case LogMode.DisposeFromMemory:
                    fht.Faster.Log.DisposeFromMemory();
                    break;
                default:
                    Console.WriteLine($"Missing LogMode case: {testRun.TestResult.Inputs.LogMode}");
                    return;
            }
        }

        private void RunOperations<TFunctions>(FHT<TKey, TValue, TOutput, TFunctions> fht, TestRun testRun)
            where TFunctions : IFunctions<TKey, TValue, Input, TOutput, Empty>, new()
        {
            IEnumerable<(Operations, string, int)> prepareOps()
            {
                if (testRun.TestResult.Inputs.MixOperations)
                {
                    IEnumerable<string> getMixedOpNames()
                    {
                        if (testRun.TestResult.Inputs.UpsertCount > 0)
                            yield return "Upsert";
                        if (testRun.TestResult.Inputs.ReadCount > 0)
                            yield return "Read";
                        if (testRun.TestResult.Inputs.RMWCount > 0)
                            yield return "RMW";
                    }
                    yield return (Operations.Mixed, $"mixed {string.Join(", ", getMixedOpNames())} ops", testRun.TestResult.Inputs.TotalOperationCount);
                    yield break;
                }
                if (testRun.TestResult.Inputs.UpsertCount > 0)
                    yield return (Operations.Upsert, "Upsert", testRun.TestResult.Inputs.UpsertCount);
                if (testRun.TestResult.Inputs.ReadCount > 0)
                    yield return (Operations.Read, "Read", testRun.TestResult.Inputs.ReadCount);
                if (testRun.TestResult.Inputs.RMWCount > 0)
                    yield return (Operations.RMW, "RMW", testRun.TestResult.Inputs.RMWCount);
            }

            var ops = prepareOps();
            var sw = new Stopwatch();

            // Reset the global chunk tracker here (outside the loop).
            NextChunkStart = 0;

            foreach (var (op, opName, opCount) in ops)
            {
                long startTailAddress = fht.LogTailAddress;
                sw.Restart();

                // Each thread does the full count
                var tasks = Enumerable.Range(0, testRun.TestResult.Inputs.ThreadCount)
                                      .Select(threadIdx => Task.Run(() => RunOperations(fht, op, opCount, threadIdx, testRun)));
                Task.WaitAll(tasks.ToArray());

                sw.Stop();
                var numSec = sw.ElapsedMilliseconds / 1000.0;

                // Total Ops/Second is always reported 
                testRun.TotalOpsMs += (ulong)sw.ElapsedMilliseconds;

                switch (op)
                {
                    case Operations.Mixed: break;
                    case Operations.Upsert: testRun.UpsertMs = (ulong)sw.ElapsedMilliseconds; break;
                    case Operations.Read: testRun.ReadMs = (ulong)sw.ElapsedMilliseconds; break;
                    case Operations.RMW: testRun.RMWMs = (ulong)sw.ElapsedMilliseconds; break;
                    default:
                        throw new InvalidOperationException($"Unexpected Operations value: {op}");
                }

                var suffix = op == Operations.Mixed ? "" : "s";
                ulong totalOpsAllThreads = (ulong)opCount * (ulong)testRun.TestResult.Inputs.ThreadCount;
                Console.WriteLine($"Operations: Time for {opCount:N0} {opName}{suffix} per thread ({totalOpsAllThreads:N0} total):" +
                                  $" {numSec:N3} sec ({totalOpsAllThreads / numSec:N2} {op}{suffix}/sec; {opCount / numSec:N2} thread/sec)");
                var endTailAddress = fht.LogTailAddress;
                if (endTailAddress != startTailAddress)
                {
                    var isExpected = testRun.TestResult.Inputs.LogMode != LogMode.None || testRun.TestResult.Inputs.CheckpointMode != Checkpoint.Mode.None
                        ? $"expected due to"
                        : $"*** UNEXPECTED *** with";
                    Console.WriteLine($"Log growth: {endTailAddress - startTailAddress}; {isExpected} {nameof(LogMode)}.{testRun.TestResult.Inputs.LogMode}" +
                                      $" and {nameof(Checkpoint)}.{nameof(Checkpoint.Mode)}.{testRun.TestResult.Inputs.CheckpointMode}");
                }
            }
        }

        private async Task RunOperations<TFunctions>(FHT<TKey, TValue, TOutput, TFunctions> fht, Operations op, long opCount, int threadIndex, TestRun testRun)
            where TFunctions : IFunctions<TKey, TValue, Input, TOutput, Empty>, new()
        {
            // Each thread needs to set NUMA and create a FASTER session
            Numa.AffinitizeThread(testRun.TestResult.Inputs.NumaMode, threadIndex);
            using var session = fht.Faster.NewSession<Input, TOutput, Empty, TFunctions>(fht.Functions, $"RunOperations_{threadIndex}",
                                                                                         testRun.TestResult.Inputs.ThreadMode == ThreadMode.Affinitized);
            if (Globals.Verbose)
                Console.WriteLine($"Running Operation {op} count {opCount:N0} for threadId {threadIndex}");

            var rng = new RandomGenerator((uint)threadIndex);
            var totalOpCount = testRun.TestResult.Inputs.TotalOperationCount;
            var upsertThreshold = testRun.TestResult.Inputs.UpsertCount;
            var readThreshold = upsertThreshold + testRun.TestResult.Inputs.ReadCount;
            var rmwThreshold = readThreshold + testRun.TestResult.Inputs.RMWCount;

            var input = default(Input);
            var valueWrapper = this.valueWrapperFactory.GetValueWrapper(threadIndex);
            var output = this.valueWrapperFactory.GetOutput(threadIndex);

            long serialNo = 0;

            var verboseInterval = opCount / numVerboseIntervals;

            var readTasks = doReadBatch
                                ? new ValueTask<FasterKV<TKey, TValue>.ReadAsyncResult<Input, TOutput, Empty, TFunctions>>[testRun.TestResult.Inputs.AsyncReadBatchSize]
                                : null;
            int numReadTasks = 0;

            // We multiply the number of operations by the number of threads, so we will wrap around the end of the operations keys.
            for (long currentCount = 0;  currentCount < opCount; currentCount += Globals.ChunkSize) {
                long chunkStart = Interlocked.Add(ref NextChunkStart, Globals.ChunkSize) - Globals.ChunkSize;
                int opKeyCount = testRun.TestResult.Inputs.OperationKeyCount;
                while (chunkStart >= opKeyCount) {
                    // Make sure only one thread resets to zero. Note: TestInputs.Verify ensures that OperationKeyCount % Globals.ChunkSize == 0.
                    if (chunkStart == opKeyCount)
                        this.NextChunkStart = 0;
                    chunkStart = Interlocked.Add(ref this.NextChunkStart, Globals.ChunkSize) - Globals.ChunkSize;
                }

                var chunkEnd = chunkStart + Globals.ChunkSize;
                for (var ii = chunkStart; ii < chunkEnd; ii++)
                {
                    var thisOp = op != Operations.Mixed
                        ? op
                        : rng.Generate((uint)totalOpCount) switch
                            {
                                var rand when rand < upsertThreshold => Operations.Upsert,
                                var rand when rand <= readThreshold => Operations.Read,
                                var rand when rand <= rmwThreshold => Operations.RMW,
                                var rand => throw new InvalidOperationException($"rand {rand} out of threshold ranges: u {upsertThreshold} r {readThreshold} m {rmwThreshold}")
                            };

                    if (serialNo % 256 == 0)
                    {
                        if (!this.useAsync)
                            session.Refresh();
                        if (++serialNo % 65536 == 0)
                        {
                            if (!this.useAsync)
                                session.CompletePending(false);
                            if (Globals.Verbose && serialNo % verboseInterval < pendingInterval)
                                Console.WriteLine($"tid {threadIndex}, {thisOp}: {serialNo}");
                        }
                    }

                    Status status = Status.OK;
                    switch (thisOp)
                    {
                        case Operations.Upsert:
                            valueWrapper.SetUpsertValue(keyManager.GetOpKey((int)ii).Value, ii & 7);
                            if (this.useAsync)   // TODO: UpsertAsync does not return status
                                await session.UpsertAsync(ref keyManager.GetOpKey((int)ii), ref valueWrapper.GetRef(), waitForCommit: false);
                            else
                                status = session.Upsert(ref keyManager.GetOpKey((int)ii), ref valueWrapper.GetRef(), Empty.Default, serialNo);
                            break;

                        case Operations.Read:
                            if (this.useAsync)
                            {
                                var readTask = session.ReadAsync(ref keyManager.GetOpKey((int)ii), ref input);
                                if (doReadBatch)
                                {
                                    readTasks[numReadTasks] = readTask;
                                    if (++numReadTasks == testRun.TestResult.Inputs.AsyncReadBatchSize)
                                    {
                                        for (var jj = 0; jj < numReadTasks; ++jj)
                                        {
                                            var readStatus = (await readTasks[jj]).CompleteRead().Item1;
                                            if (readStatus != Status.OK)
                                                throw new ApplicationException($"Error: Unexpected readStatus in {nameof(RunOperations)} async {thisOp};" +
                                                                                $" key[{ii}] = {keyManager.GetOpKey((int)ii).Value}: {readStatus}");
                                            readTasks[jj] = default;
                                        }
                                        numReadTasks = 0;
                                    }
                                }
                                else
                                    status = (await readTask).CompleteRead().Item1;
                            }
                            else
                                status = session.Read(ref keyManager.GetOpKey((int)ii), ref input, ref output, Empty.Default, serialNo);
                            break;

                        case Operations.RMW:
                            input.value = (int)(ii & 7);
                            if (this.useAsync)   // TODO: RMWAsync does not return status
                                await session.RMWAsync(ref keyManager.GetOpKey((int)ii), ref input, waitForCommit: false);
                            else
                                status = session.RMW(ref keyManager.GetOpKey((int)ii), ref input, Empty.Default, serialNo);
                            break;
                    }

                    if (status != Status.OK && status != Status.PENDING)
                        throw new ApplicationException($"Error: Unexpected status in {nameof(RunOperations)} {thisOp}; key[{ii}] = {keyManager.GetOpKey((int)ii).Value}: {status}");
                }
            }

            // Finish off any pending reads
            for (var jj = 0; jj < numReadTasks; ++jj)
            {
                var readStatus = (await readTasks[jj]).CompleteRead().Item1;
                if (readStatus != Status.OK)
                    throw new ApplicationException($"Error: Unexpected readStatus in {nameof(RunOperations)} async Read: {readStatus}");
            }

            session.CompletePending(true);
        }
    }
}