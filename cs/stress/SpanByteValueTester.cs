// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using NUnit.Framework;

namespace FASTER.stress
{
    internal class SpanByteValueTester<TKey, TFunctions> : IValueTester<TKey>
        where TFunctions : IFunctions<TKey, SpanByte, SpanByte, SpanByteAndMemory, Empty>
    {
        readonly TestLoader testLoader;
        readonly TFunctions functions;
        readonly int tid;
        readonly SessionWrapper<TKey, SpanByte, SpanByte, SpanByteAndMemory> session;

        static FasterKV<TKey, SpanByte> fkv;
        static PinnedByteArray[] values;
        readonly Random rng;

        internal SpanByteValueTester(int tid, TestLoader testLoader, TFunctions functions)
        {
            this.testLoader = testLoader;
            this.functions = functions;
            this.tid = tid;
            rng = new Random((tid + testLoader.Options.ThreadCount) * testLoader.Options.RandomSeed);
            values = new PinnedByteArray[testLoader.Options.KeyCount];
            this.session = new(testLoader, output => BitConverter.ToInt32(output.Memory.Memory.Span) % testLoader.ValueIncrement, rng, o => o.Memory.Dispose());
        }

        public long GetAverageSize() => sizeof(int) + (testLoader.Options.ValueLength / (testLoader.UseRandom ? 2 : 1));

        public void Create(int hashTableCacheLines, LogSettings logSettings, CheckpointSettings checkpointSettings, IFasterEqualityComparer<TKey> comparer)
        {
            Assert.IsNull(fkv);
            fkv = new FasterKV<TKey, SpanByte>(hashTableCacheLines, logSettings, checkpointSettings: checkpointSettings, comparer: comparer);
            PrepareTest();   // to create the sessions; this session will not be used in the actual test
            if (testLoader.Options.InitialEvict)
                fkv.Log.FlushAndEvict(wait: true);
        }

        public void PrepareTest() => this.session.PrepareTest(fkv.NewSession(functions));

        public void AddRecord(int keyOrdinal, ref TKey key)
        {
            values[keyOrdinal] = new PinnedByteArray(testLoader.GetValueLength(rng));
            Assert.IsTrue(BitConverter.TryWriteBytes(values[keyOrdinal].Vector, keyOrdinal + testLoader.ValueIncrement));
            var value = values[keyOrdinal].GetSpanByte();
            session.Upsert(ref key, ref value);
        }

        public async Task<bool> CheckpointStore()
        {
            if (!fkv.TryInitiateHybridLogCheckpoint(out _, testLoader.Options.CheckpointType, testLoader.Options.CheckpointIncremental))
                return false;
            await fkv.CompleteCheckpointAsync();
            return true;
        }

        public bool CompactStore()
        {
            long compactUntil = testLoader.GetCompactUntilAddress(fkv.Log.BeginAddress, fkv.Log.TailAddress);
            if (session.FkvSession.Compact(compactUntil, testLoader.Options.CompactType) != fkv.Log.BeginAddress)
                return false;
            if (testLoader.Options.CompactTruncate)
                fkv.Log.Truncate();
            return true;
        }

        public void TestRecord(int keyOrdinal, int keyCount, TKey[] keys)
        {
            var opType = testLoader.GetOperationType(rng);

            // Only ReadLUC and RMWLUC do locking; Upsert and Delete operate on a single key.

            // Read and Delete do not take a value
            switch (opType)
            {
                case OperationType.READ:
                    this.session.Read(keyOrdinal, keyCount, keys);
                    return;
                case OperationType.DELETE:
                    session.Delete(keys);
                    return;
                case OperationType.RMW:
                    session.RMW(keyOrdinal, keyCount, keys, values[keyOrdinal].GetSpanByte());
                    return;
                default:
                    session.Upsert(keys, values[keyOrdinal].GetSpanByte());
                    return;
            }
        }

        public Task TestRecordAsync(int keyOrdinal, int keyCount, TKey[] keys)
        {
            var opType = testLoader.GetOperationType(rng);

            // Only ReadLUC and RMWLUC do locking; Upsert and Delete operate on a single key.

            // Read and Delete do not take a value
            return opType switch
            {
                OperationType.READ => session.ReadAsync(keyOrdinal, keyCount, keys),
                OperationType.DELETE => session.DeleteAsync(keys),
                OperationType.RMW => session.RMWAsync(keyOrdinal, keyCount, keys, values[keyOrdinal].GetSpanByte()),
                _ => session.UpsertAsync(keys, values[keyOrdinal].GetSpanByte())
            };
        }

        public void Dispose() => session.Dispose();
    }
}
