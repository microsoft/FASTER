// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;

namespace FASTER.stress
{
    internal class LongKeyTester : IKeyTester
    {
        readonly TestLoader testLoader;
        readonly int tid;
        readonly IValueTester<long> valueTester;
        readonly Random rng;
        readonly int[] lockOrdinals;
        readonly long[] lockKeys;

        internal LongKeyTester(int tid, TestLoader testLoader)
        {
            this.testLoader = testLoader;
            this.tid = tid;
            this.valueTester = testLoader.Options.ValueType switch
            {
                DataType.Long => new LongValueTester<long, NonSpanByteValueFunctions<long, long>>(tid, testLoader, new ()),
                DataType.String => new StringValueTester<long, NonSpanByteValueFunctions<long, string>>(tid, testLoader, new ()),
                DataType.SpanByte => new SpanByteValueTester<long, SpanByteValueFunctions<long>>(tid, testLoader, new ()),
                _ => throw new ApplicationException(testLoader.MissingKeyTypeHandler)
            };
            rng = new Random(tid * testLoader.Options.RandomSeed);
            lockOrdinals = new int[testLoader.LockKeyArraySize];
            lockKeys = new long[testLoader.LockKeyArraySize];
        }

        public long GetAverageRecordSize() => sizeof(long) + valueTester.GetAverageSize();

        public IValueTester ValueTester => valueTester;

        internal class Comparer : IFasterEqualityComparer<long>
        {
            readonly LongFasterEqualityComparer comparer;
            readonly long mod;

            internal Comparer(TestLoader testLoader)
            {
                comparer = new();
                mod = testLoader.KeyModulo;
            }

            public bool Equals(ref long k1, ref long k2) => k1 == k2;

            public long GetHashCode64(ref long k)
            {
                long hash = comparer.GetHashCode64(ref k);
                return mod > 0 ? hash % mod : hash;
            }
        }

        public void Populate(int hashTableCacheLines, LogSettings logSettings, CheckpointSettings checkpointSettings)
        {
            valueTester.Create(hashTableCacheLines, logSettings, checkpointSettings, new Comparer(testLoader));

            for (int keyOrdinal = 0; keyOrdinal < testLoader.Options.KeyCount; ++keyOrdinal)
            {
                long key = keyOrdinal;
                valueTester.AddRecord(keyOrdinal, ref key);
            }
        }

        class SpanByteSortComparer : IComparer<long>
        {
            public int Compare(long x, long y) => x.CompareTo(y);
        }
        readonly SpanByteSortComparer sortComparer = new();

        public void Test() => testLoader.Test(tid, rng, lockOrdinals, lockKeys, ordinal => ordinal, sortComparer, valueTester);

        public Task TestAsync() => testLoader.TestAsync(tid, rng, lockOrdinals, lockKeys, ordinal => ordinal, sortComparer, valueTester);

        public void Dispose() { }
    }
}
