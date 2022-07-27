// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;

namespace FASTER.stress
{
    internal class StringKeyTester : IKeyTester
    {
        readonly TestLoader testLoader;
        readonly int tid;
        readonly IValueTester<string> valueTester;
        readonly Random rng;
        readonly int[] lockOrdinals;
        readonly string[] lockKeys;

        static string[] keys;       // Keep these so we don't invoke the GC

        internal StringKeyTester(int tid, TestLoader testLoader)
        {
            this.testLoader = testLoader;
            this.tid = tid;
            this.valueTester = testLoader.Options.ValueType switch
            {
                DataType.Long => new LongValueTester<string, NonSpanByteValueFunctions<string, long>>(tid, testLoader, new ()),
                DataType.String => new StringValueTester<string, NonSpanByteValueFunctions<string, string>>(tid, testLoader, new ()),
                DataType.SpanByte => new SpanByteValueTester<string, SpanByteValueFunctions<string>>(tid, testLoader, new ()),
                _ => throw new ApplicationException(testLoader.MissingKeyTypeHandler)
            };
            rng = new Random(tid * testLoader.Options.RandomSeed);
            lockOrdinals = new int[testLoader.LockKeyArraySize];
            lockKeys = new string[testLoader.LockKeyArraySize];
            keys = new string[testLoader.Options.KeyCount];
        }

        public long GetAverageRecordSize() => sizeof(long) + valueTester.GetAverageSize();

        public IValueTester ValueTester => valueTester;

        internal class Comparer : IFasterEqualityComparer<string>
        {
            readonly StringFasterEqualityComparer comparer;
            readonly long mod;

            internal Comparer(TestLoader testLoader)
            {
                comparer = new();
                mod = testLoader.KeyModulo;
            }

            public bool Equals(ref string k1, ref string k2) => k1.Equals(k2, StringComparison.Ordinal);

            public long GetHashCode64(ref string k)
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
                keys[keyOrdinal] = keyOrdinal.ToString(testLoader.GetStringKeyFormat(rng));
                valueTester.AddRecord(keyOrdinal, ref keys[keyOrdinal]);
            }
        }

        class StringSortComparer : IComparer<string>
        {
            public int Compare(string x, string y) => String.CompareOrdinal(x, y);
        }
        readonly StringSortComparer sortComparer = new();

        public void Test() => testLoader.Test(tid, rng, lockOrdinals, lockKeys, ordinal => keys[ordinal], sortComparer, valueTester);

        public Task TestAsync() => testLoader.TestAsync(tid, rng, lockOrdinals, lockKeys, ordinal => keys[ordinal], sortComparer, valueTester);

        public void Dispose() { }
    }
}
