// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using NUnit.Framework;

namespace FASTER.stress
{
    internal class StringValueTester<TKey, TFunctions> : IValueTester<TKey>
        where TFunctions : IFunctions<TKey, string, string, string, Empty>
    {
        readonly TestLoader testLoader;
        readonly TFunctions functions;
        readonly int tid;
        readonly SessionWrapper<TKey, string, string, string> session;

        static FasterKV<TKey, string> fkv;
        static string[] values;   // Keep these so we don't invoke the GC
        readonly Random rng;

        internal StringValueTester(int tid, TestLoader testLoader, TFunctions functions)
        {
            this.testLoader = testLoader;
            this.functions = functions;
            this.tid = tid;
            rng = new Random((tid + testLoader.Options.ThreadCount) * testLoader.Options.RandomSeed);
            this.session = new(testLoader, output => (int)(long.Parse(output) % testLoader.ValueIncrement), rng);
            values = new string[testLoader.Options.KeyCount];
        }

        public ILockableContext<TKey> LockableContext => this.session.LockableContext;

        public long GetAverageSize() => testLoader.AverageStringLength;

        public void Create(int hashTableCacheLines, LogSettings logSettings, CheckpointSettings checkpointSettings, RevivificationSettings revivSettings, IFasterEqualityComparer<TKey> comparer)
        {
            Assert.IsNull(fkv);
            fkv = new FasterKV<TKey, string>(hashTableCacheLines, logSettings, checkpointSettings: checkpointSettings, revivificationSettings: revivSettings, comparer: comparer);
            PrepareTest();   // to create the sessions; this session will not be used in the actual test
            if (testLoader.Options.InitialEvict)
                fkv.Log.FlushAndEvict(wait: true);
        }

        public void PrepareTest() => this.session.PrepareTest(fkv.NewSession(functions));

        public void AddRecord(int keyOrdinal, ref TKey key)
        {
            values[keyOrdinal] = (keyOrdinal + testLoader.ValueIncrement).ToString(testLoader.GetStringValueFormat(rng));
            session.Upsert(ref key, ref values[keyOrdinal]);
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
                    session.RMW(keyOrdinal, keyCount, lockKeys, values[keyOrdinal]);
                    return;
                default:
                    session.Upsert(lockKeys, values[keyOrdinal]);
                    return;
            }
        }

        public Task TestRecordAsync(int keyOrdinal, int keyCount, FixedLengthLockableKeyStruct<TKey>[] lockKeys)
        {
            var opType = testLoader.GetOperationType(rng);

            // Only ReadLUC and RMWLUC do locking; Upsert and Delete operate on a single key.

            // Read and Delete do not take a value
            return opType switch
            {
                OperationType.READ => session.ReadAsync(keyOrdinal, keyCount, lockKeys),
                OperationType.DELETE => session.DeleteAsync(lockKeys),
                OperationType.RMW => session.RMWAsync(keyOrdinal, keyCount, lockKeys, values[keyOrdinal]),
                _ => session.UpsertAsync(lockKeys, values[keyOrdinal])
            };
        }

        public void Dispose() => session.Dispose();
    }
}
