// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using CommandLine;
using FASTER.core;
using NUnit.Framework;
using System.Diagnostics;

namespace FASTER.stress
{
    class TestLoader
    {
        internal const int MinDataLen = 8;
        internal readonly Options Options;

        // RUMD percentages. Delete is not a percent; it will fire automatically if the others do not sum to 100, saving an 'if'.
        internal readonly int ReadPercent, UpsertPercent, RmwPercent;

        internal readonly bool error;

        internal TestLoader(string[] args)
        {
            error = true;
            Parser parser = new (settings =>
            {
                settings.CaseSensitive = false;
                settings.CaseInsensitiveEnumValues = true;
                settings.HelpWriter ??= Parser.Default.Settings.HelpWriter;
            });
            ParserResult<Options> result = parser.ParseArguments<Options>(args);

            if (result.Tag == ParserResultType.NotParsed)
            {
                return;
            }
            Options = result.MapResult(o => o, xs => new Options());

            static bool verifyOption(bool isValid, string message)
            {
                if (!isValid)
                    Console.WriteLine(message);
                return isValid;
            }

            if (!verifyOption(Options.ThreadCount > 0, "ThreadCount must be > 0"))
                return;

            if (!verifyOption(Options.IterationCount > 0, "IterationCount must be > 0"))
                return;

            var intervalAndDelay = Options.CheckpointSecAndDelay.ToArray();
            if (!verifyOption(intervalAndDelay.Length <= 2, "A maximum of two CheckpointSec values (interval and delay) may be specified"))
                return;
            if (intervalAndDelay.Length > 0)
            {
                Options.CheckpointIntervalSec = intervalAndDelay[0];
                if (intervalAndDelay.Length > 1)
                    Options.CheckpointDelaySec = intervalAndDelay[1];
            }

            intervalAndDelay = Options.CompactSecAndDelay.ToArray();
            if (!verifyOption(intervalAndDelay.Length <= 2, "A maximum of two CompactSec values (interval and delay) may be specified"))
                return;
            if (intervalAndDelay.Length > 0)
            {
                Options.CompactIntervalSec = intervalAndDelay[0];
                if (intervalAndDelay.Length > 1)
                    Options.CompactDelaySec = intervalAndDelay[1];
            }

            if (!verifyOption(Options.CompactPercent >= 0 && Options.CompactPercent <= 100, "CompactPercent must be between 0 and 100"))
                return;

            if (!verifyOption(Options.KeyCount >= 0, "KeyCount must be > 0"))
                return;
            if (!verifyOption(Options.KeyLength >= MinDataLen, $"KeyLength must be > {MinDataLen}"))
                return;
            if (!verifyOption(Options.ValueLength >= MinDataLen, $"ValueLength must be > {MinDataLen}"))
                return;

            if (!verifyOption(Options.LogInMemPercent >= 0 && Options.LogInMemPercent <= 100, "LogInMemPercent must be between 0 and 100"))
                return;
            if (!verifyOption(Options.LogPageSizeShift >= 0, "LogPageSizeShift must be > 0"))
                return;
            if (!verifyOption(Options.LogSegmentSizeShift >= 0 && Options.LogSegmentSizeShift <= Options.LogPageSizeShift, "LogSegmentSizeShift must be > 0 and <= LogPageSizeShift"))
                return;

            if (!verifyOption(Options.ReadCacheInMemPercent >= 0 && Options.ReadCacheInMemPercent <= 100, "ReadCacheInMemPercent must be between 0 and 100"))
                return;
            if (!verifyOption(Options.ReadCachePageSizeShift >= 0, "ReadCachePageSizeShift must be > 0"))
                return;

            if (!verifyOption(Options.AsyncPercent >= 0 && Options.AsyncPercent <= 100, "AsyncPercent must be between 0 and 100"))
                return;

            if (!verifyOption(Options.LUCPercent >= 0 && Options.LUCPercent <= 100, "LUCPercent must be between 0 and 100"))
                return;
            if (!verifyOption(Options.LUCLockCount >= 0, "LUCPercent must be > 0"))
                return;

            var rumdPercents = Options.RumdPercents.ToArray();  // Will be non-null because we specified a default
            if (!verifyOption(rumdPercents.Length == 4 && Options.RumdPercents.Sum() == 100 && !Options.RumdPercents.Any(x => x < 0), 
                    "Percentages of [(r)eads,(u)pserts,r(m)ws,(d)eletes] must be empty or must sum to 100 with no negative elements"))
                return;
            this.ReadPercent = rumdPercents[0];
            this.UpsertPercent = this.ReadPercent + rumdPercents[1];
            this.RmwPercent = this.UpsertPercent + rumdPercents[2];

            var revivBinRecordSizes = Options.RevivBinRecordSizes?.ToArray();
            var revivBinRecordCounts = Options.RevivBinRecordCounts?.ToArray();
            bool hasRecordSizes = revivBinRecordSizes?.Length > 0, hasRecordCounts = revivBinRecordCounts?.Length > 0;

            if (hasRecordSizes)
            {
                if (hasRecordCounts && revivBinRecordCounts.Length > 1 && revivBinRecordCounts.Length != revivBinRecordSizes.Length)
                    throw new Exception("Incompatible revivification record size and count cardinality.");
                if (Options.UseRevivBinsPowerOf2)
                    throw new Exception("Revivification cannot specify both record sizes and powerof2 bins.");
                if (Options.RevivInChainOnly)
                    throw new Exception("Revivification cannot specify both record sizes and in-chain-only.");
            }
            if (hasRecordCounts)
            {
                if (Options.UseRevivBinsPowerOf2)
                    throw new Exception("Revivification cannot specify both record counts and powerof2 bins.");
                if (!hasRecordSizes)
                    throw new Exception("Revivification bin counts require bin sizes.");
            }
            if (Options.RevivBinBestFitScanLimit != 0)
            {
                if (!hasRecordSizes && !Options.UseRevivBinsPowerOf2)
                    throw new Exception("Revivification cannot specify best fit scan limit without specifying bins.");
                if (Options.RevivBinBestFitScanLimit < 0)
                    throw new Exception("RevivBinBestFitScanLimi must be >= 0.");
            }
            if (Options.RevivMutablePercent != 0)
            {
                if (!hasRecordSizes && !Options.UseRevivBinsPowerOf2)
                    throw new Exception("Revivification cannot specify mutable percent without specifying bins.");
                if (Options.RevivMutablePercent < 0 || Options.RevivMutablePercent > 100)
                    throw new Exception("RevivMutablePercent must be >= 0 and <= 100.");
            }

            error = false;

            var now = DateTime.Now;
            this.OutputDirectory = Path.Combine(Options.OutputDirectory, $"{now.Year}-{now.Month}-{now.Day}_{now.Hour}.{now.Minute}.{now.Second}");
            this.KeyModulo = Options.CollisionCount > 0 ? Options.KeyCount / Options.CollisionCount : -1;
        }

        internal string OutputDirectory { get; private set; }

        internal long KeyModulo { get; private set; }

        internal bool UseRandom => this.Options.RandomSeed != 0;
        internal bool UseCheckpoints => this.Options.CheckpointIntervalSec > 0;
        internal bool UseCompact => this.Options.CompactIntervalSec > 0;
        internal bool UseLocks => this.Options.LUCLockCount > 0;
        internal bool UseReadCache => this.Options.ReadCache;
        internal bool UseDelete => this.ReadPercent + this.UpsertPercent + this.RmwPercent < 100;

        internal bool HasObjects => this.Options.KeyType == DataType.String || this.Options.ValueType == DataType.String;

        internal string MissingKeyTypeHandler => $"Missing DataType handler for key type {this.Options.KeyType}";
        internal string MissingValueTypeHandler => $"Missing DataType handler for value type {this.Options.ValueType}";

        // Averages for initial record size estimation
        internal int AverageStringLength => Utility.GetSize(default(string));
        internal int AverageSpanByteLength => sizeof(int) + (this.UseRandom ? this.Options.ValueLength / 2 : this.Options.ValueLength);

        // Actual value lengths on a per-operation basis
        internal int GetKeyLength(Random rng) => this.UseRandom ? (int)(this.Options.KeyLength * rng.NextDouble()) : this.Options.KeyLength;
        internal int GetValueLength(Random rng) => this.UseRandom ? (int)(this.Options.ValueLength * rng.NextDouble()) : this.Options.ValueLength;

        internal int ValueIncrement => 1_000_000;
        internal string GetStringKeyFormat(Random rng) => $"D{GetKeyLength(rng)}";
        internal string GetStringValueFormat(Random rng) => $"D{GetValueLength(rng)}";

        internal bool WantLUC(Random rng) 
            => this.Options.LUCPercent switch
                {
                    0 => false,
                    100 => true,
                    var pct => rng.Next(100) <= pct
                };

        internal int LockKeyArraySize => Math.Max(Options.LUCLockCount, 1);

        internal int NumAsyncThreads() => Options.AsyncPercent == 0 ? 0 : (int)Math.Floor(Options.ThreadCount * (Options.AsyncPercent / 100.0) + 0.5);

        internal OperationType GetOperationType(Random rng)
        {
            // We've already guaranteed that the percentages are correctly distributed within 0-100.
            int rand = rng.Next(100);
            int sel = this.ReadPercent;
            if (rand < sel)
                return OperationType.READ;
            sel += this.RmwPercent;
            if (rand < sel)
                return OperationType.RMW;
            sel += this.UpsertPercent;
            if (rand < sel)
                return OperationType.UPSERT;
            return OperationType.DELETE;
        }

        internal long GetCompactUntilAddress(long beginAddress, long tailAddress) => beginAddress + (tailAddress - beginAddress) * (Options.CompactPercent / 100);

        internal int GetKeysToLock(Random rng, int startOrdinal, int[] ordinals)
        {
            if (!UseLocks)
            {
                // Not locking records, so just return the one key ordinal we'll operate on
                ordinals[0] = UseRandom ? rng.Next(Options.LUCLockCount) : startOrdinal;
                return 1;
            }

            int ordinal;
            if (UseRandom)
            {
                var count = rng.Next(Options.LUCLockCount);
                for (var ii = 0; ii < count; ++ii)
                {
                    while (true)
                    {
                        ordinal = rng.Next(Options.LUCLockCount);
                        if (!ordinals.Contains(ordinal))
                        {
                            ordinals[ii] = ordinal;
                            break;
                        }
                    }
                }
                return count;
            }

            // Sequential, with wrap
            ordinal = startOrdinal;
            for (var ii = 0; ii < ordinals.Length; ++ii)
            {
                ordinals[ii] = ordinal;
                if (++ordinal == Options.KeyCount)
                    ordinal = 0;
            }
            return ordinals.Length;
        }

        internal void Status(Verbose level, string message)
        {
            if (this.Options.Verbose >= level)
                Console.WriteLine(message);
        }

        internal void MaybeLock<TKey>(ILockableContext<TKey> luContext, int keyCount, FixedLengthLockableKeyStruct<TKey>[] keys, bool isRmw, bool isAsyncTest)
        {
            if (!UseLocks)
                return;

            var uContext = luContext as IUnsafeContext;
            if (isAsyncTest)
                uContext.BeginUnsafe();
            try
            {
                luContext.BeginLockable();

                // For RMW, simulate "putting the result" into keys[0]
                if (isRmw)
                    keys[0].LockType = LockType.Exclusive;
                luContext.Lock(keys);
            } 
            finally
            {
                luContext.EndLockable();
                if (isAsyncTest)
                    uContext.EndUnsafe();
            }
        }

        internal void MaybeUnlock<TKey>(ILockableContext<TKey> luContext, int keyCount, FixedLengthLockableKeyStruct<TKey>[] lockKeys, bool isRmw, bool isAsyncTest)
        {
            if (!UseLocks)
                return;

            var uContext = luContext as IUnsafeContext;
            if (isAsyncTest)
                uContext.BeginUnsafe();
            try
            {
                luContext.BeginLockable();
                luContext.Unlock(lockKeys);
            }
            finally
            {
                // Undo the setting from RMW
                lockKeys[0].LockType = LockType.Shared;
                luContext.EndLockable();
                if (isAsyncTest)
                    uContext.EndUnsafe();
            }
        }

        internal static (Status status, TOutput output) GetSinglePendingResult<TKey, TValue, TInput, TOutput, TContext>(CompletedOutputIterator<TKey, TValue, TInput, TOutput, TContext> completedOutputs)
            => GetSinglePendingResult(completedOutputs, out _);

        internal static (Status status, TOutput output) GetSinglePendingResult<TKey, TValue, TInput, TOutput, TContext>(CompletedOutputIterator<TKey, TValue, TInput, TOutput, TContext> completedOutputs, out RecordMetadata recordMetadata)
        {
            Assert.IsTrue(completedOutputs.Next());
            var result = (completedOutputs.Current.Status, completedOutputs.Current.Output);
            recordMetadata = completedOutputs.Current.RecordMetadata;
            Assert.IsFalse(completedOutputs.Next());
            completedOutputs.Dispose();
            return result;
        }

        internal void Test<TKey>(int tid, Random rng, int[] lockOrdinals, FixedLengthLockableKeyStruct<TKey>[] lockKeys, Func<int, TKey> getOrdinalKey, IValueTester<TKey> valueTester)
        {
            this.Status(Verbose.Low, $"Thread {tid}/{Environment.CurrentManagedThreadId} starting Sync Test");
            for (var iter = 0; iter < this.Options.IterationCount; ++iter)
            {
                for (var ii = 0; ii < this.Options.KeyCount; ++ii)
                {
                    var lockKeyCount = this.GetKeysToLock(rng, ii, lockOrdinals);
                    for (var jj = 0; jj < lockOrdinals.Length; ++jj)
                        lockKeys[jj] = new(getOrdinalKey(lockOrdinals[jj]), LockType.Shared, valueTester.FasterContext);
                    valueTester.LockableContext.SortKeyHashes(lockKeys);   // Sort to avoid deadlocks
                    valueTester.TestRecord(lockOrdinals[0], lockKeyCount, lockKeys);
                }
                this.Status(iter > 0 && iter % 100 == 0 ? Verbose.Low : Verbose.High, $"Thread {tid}/{Environment.CurrentManagedThreadId} completed Sync iteration {iter}");
            }
            this.Status(Verbose.Low, $"Thread {tid}/{Environment.CurrentManagedThreadId} completed Sync Test");
        }

        internal async Task TestAsync<TKey>(int tid, Random rng, int[] lockOrdinals, FixedLengthLockableKeyStruct<TKey>[] lockKeys, Func<int, TKey> getOrdinalKey, IValueTester<TKey> valueTester)
        {
            this.Status(Verbose.Low, $"Thread {tid}/{Environment.CurrentManagedThreadId} starting Async Test");
            await Task.Delay(50);  // Make sure the test doesn't start by executing synchronously for a while
            for (var iter = 0; iter < this.Options.IterationCount; ++iter)
            {
                for (var ii = 0; ii < this.Options.KeyCount; ++ii)
                {
                    var lockKeyCount = this.GetKeysToLock(rng, ii, lockOrdinals);
                    for (var jj = 0; jj < lockOrdinals.Length; ++jj)
                        lockKeys[jj] = new(getOrdinalKey(lockOrdinals[jj]), LockType.Shared, valueTester.FasterContext);
                    valueTester.LockableContext.SortKeyHashes(lockKeys);   // Sort to avoid deadlocks
                    await valueTester.TestRecordAsync(lockOrdinals[0], lockKeyCount, lockKeys);
                }
                this.Status(iter > 0 && iter % 100 == 0 ? Verbose.Low : Verbose.High, $"Thread {tid}/{Environment.CurrentManagedThreadId} completed Async iteration {iter}");
            }
            this.Status(Verbose.Low, $"Thread {tid}/{Environment.CurrentManagedThreadId} completed Async Test");
        }

        internal async Task DoPeriodicCheckpoints(IValueTester tester, CancellationToken cancellationToken)
        {
            if (Options.CheckpointDelaySec > 0)
            {
                try
                {
                    await Task.Delay(Options.CheckpointDelaySec * 1000, cancellationToken);
                }
                catch (TaskCanceledException)
                {
                    return;
                }
            }

            int checkpointsTaken = 0;
            int successfulCheckpoints = 0;
            Stopwatch sw = new();
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    await Task.Delay(Options.CheckpointIntervalSec * 1000, cancellationToken);
                }
                catch (TaskCanceledException)
                {
                    return;
                }
                sw.Restart();
                bool success = await tester.CheckpointStore();
                sw.Stop();
                ++checkpointsTaken;
                if (success)
                    ++successfulCheckpoints;
                Status(Verbose.Low, $"Checkpoint #{checkpointsTaken}: {(success ? "succeeded" : "failed")}, elapsed time: {sw.ElapsedMilliseconds}ms, successful checkpoints: {successfulCheckpoints}");
            }
        }

        internal async Task DoPeriodicCompact(IValueTester tester, CancellationToken cancellationToken)
        {
            if (Options.CompactDelaySec > 0)
            {
                try
                {
                    await Task.Delay(Options.CompactDelaySec * 1000, cancellationToken);
                }
                catch (TaskCanceledException)
                {
                    return;
                }
            }

            int compactsTaken = 0;
            int successfulCompacts = 0;
            Stopwatch sw = new();
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    await Task.Delay(Options.CompactIntervalSec * 1000, cancellationToken);
                }
                catch (TaskCanceledException)
                {
                    return;
                }
                sw.Restart();
                bool success = tester.CompactStore();
                sw.Stop();
                ++compactsTaken;
                if (success)
                    ++successfulCompacts;
                Status(Verbose.Low, $"Compact #{compactsTaken}: {(success ? "succeeded" : "failed")}, elapsed time: {sw.ElapsedMilliseconds}ms, successful compacts: {successfulCompacts}");
            }
        }
    }
}
