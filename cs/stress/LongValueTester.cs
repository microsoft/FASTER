// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using NUnit.Framework;

namespace FASTER.stress
{
    internal class LongValueTester<TKey, TFunctions> : IValueTester<TKey>
        where TFunctions : IFunctions<TKey, long, long, long, Empty>
    {
        readonly TestLoader testLoader;
        readonly TFunctions functions;
        readonly int tid;
        readonly SessionWrapper<TKey, long, long, long> session;

        static FasterKV<TKey, long> fkv;
        readonly Random rng;

        internal LongValueTester(int tid, TestLoader testLoader, TFunctions functions)
        {
            this.testLoader = testLoader;
            this.functions = functions;
            this.tid = tid;
            rng = new Random((tid + testLoader.Options.ThreadCount) * testLoader.Options.RandomSeed);
            this.session = new(testLoader, output => (int)(output % testLoader.ValueIncrement), rng);
        }

        public ILockableContext<TKey> LockableContext => this.session.LockableContext;

        public long GetAverageSize() => sizeof(long);

        public void Create(int hashTableCacheLines, LogSettings logSettings, CheckpointSettings checkpointSettings, RevivificationSettings revivSettings, IFasterEqualityComparer<TKey> comparer)
        {
            Assert.IsNull(fkv);
            fkv = new FasterKV<TKey, long>(hashTableCacheLines, logSettings, checkpointSettings: checkpointSettings, revivificationSettings: revivSettings, comparer: comparer);
            PrepareTest();   // to create the sessions; this session will not be used in the actual test
            if (testLoader.Options.InitialEvict)
                fkv.Log.FlushAndEvict(wait: true);
        }

        public void PrepareTest() => this.session.PrepareTest(fkv.NewSession(functions));

        public void AddRecord(int keyOrdinal, ref TKey key)
        {
            long value = keyOrdinal + testLoader.ValueIncrement;
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

        public void TestRecord(int keyOrdinal, int keyCount, FixedLengthLockableKeyStruct<TKey>[] lockKeys)
        {
            var opType = testLoader.GetOperationType(rng);

            // Only ReadLUC and RMWLUC do locking; Upsert and Delete operate on a single key.
            long value = keyOrdinal + tid * testLoader.ValueIncrement;

            // Read and Delete do not take a value
            switch (opType)
            {
                case OperationType.READ:
                    this.session.Read(keyOrdinal, keyCount, lockKeys);
                    return;
                case OperationType.DELETE:
                    session.Delete(lockKeys);
                    return;
                case OperationType.RMW:
                    session.RMW(keyOrdinal, keyCount, lockKeys, value);
                    return;
                default:
                    session.Upsert(lockKeys, value);
                    return;
            }
        }

        public Task TestRecordAsync(int keyOrdinal, int keyCount, FixedLengthLockableKeyStruct<TKey>[] lockKeys)
        {
            var opType = testLoader.GetOperationType(rng);

            // Only ReadLUC and RMWLUC do locking; Upsert and Delete operate on a single key.
            long value = keyOrdinal + tid * testLoader.ValueIncrement;

            // Read and Delete do not take a value
            return opType switch
            {
                OperationType.READ => session.ReadAsync(keyOrdinal, keyCount, lockKeys),
                OperationType.DELETE => session.DeleteAsync(lockKeys),
                OperationType.RMW => session.RMWAsync(keyOrdinal, keyCount, lockKeys, value),
                _ => session.UpsertAsync(lockKeys, value)
            };
        }

        public void Dispose() => session.Dispose();
    }
}
