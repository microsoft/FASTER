// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using NUnit.Framework;
using System.Runtime.InteropServices;

namespace FASTER.stress
{
    internal struct PinnedByteArray
    {
        internal byte[] Vector;
        internal GCHandle Handle;
        internal IntPtr Pointer;

        internal int Length => Vector.Length;

        internal PinnedByteArray(int length)
        {
            this.Vector = new byte[length];
            this.Handle = GCHandle.Alloc(Vector, GCHandleType.Pinned);
            this.Pointer = this.Handle.AddrOfPinnedObject();
        }

        internal SpanByte GetSpanByte() => new SpanByte(this.Length, this.Pointer);

        internal void Dispose()
        {
            this.Handle.Free();
            this.Handle = default;
            this.Pointer = default;
        }
    }

    internal class SpanByteKeyTester : IKeyTester
    {
        readonly TestLoader testLoader;
        readonly int tid;
        readonly IValueTester<SpanByte> valueTester;
        readonly Random rng;
        readonly int[] lockOrdinals;
        readonly FixedLengthLockableKeyStruct<SpanByte>[] lockKeys;

        static PinnedByteArray[] keys;

        internal SpanByteKeyTester(int tid, TestLoader testLoader)
        {
            this.testLoader = testLoader;
            this.tid = tid;
            this.valueTester = testLoader.Options.ValueType switch
            {
                DataType.Long => new LongValueTester<SpanByte, NonSpanByteValueFunctions<SpanByte, long>>(tid, testLoader, new ()),
                DataType.String => new StringValueTester<SpanByte, NonSpanByteValueFunctions<SpanByte, string>>(tid, testLoader, new ()),
                DataType.SpanByte => new SpanByteValueTester<SpanByte, SpanByteValueFunctions<SpanByte>>(tid, testLoader, new ()),
                _ => throw new ApplicationException(testLoader.MissingKeyTypeHandler)
            };
            rng = new Random(tid * testLoader.Options.RandomSeed);
            lockOrdinals = new int[testLoader.LockKeyArraySize];
            lockKeys = new FixedLengthLockableKeyStruct<SpanByte>[testLoader.LockKeyArraySize];
            keys = new PinnedByteArray[testLoader.Options.KeyCount];
        }

        public long GetAverageRecordSize() => testLoader.AverageSpanByteLength + valueTester.GetAverageSize();

        public IValueTester ValueTester => valueTester;

        struct Comparer : IFasterEqualityComparer<SpanByte>
        {
            readonly long mod;

            internal Comparer(TestLoader testLoader) => mod = testLoader.KeyModulo;

            public bool Equals(ref SpanByte k1, ref SpanByte k2) => SpanByteComparer.StaticEquals(ref k1, ref k2);

            // Force collisions to create a chain
            public long GetHashCode64(ref SpanByte k)
            {
                long hash = SpanByteComparer.StaticGetHashCode64(ref k);
                return mod > 0 ? hash % mod : hash;
            }
        }

        public void Populate(int hashTableCacheLines, LogSettings logSettings, CheckpointSettings checkpointSettings)
        {
            valueTester.Create(hashTableCacheLines, logSettings, checkpointSettings, new Comparer(testLoader));

            for (int keyOrd = 0; keyOrd < testLoader.Options.KeyCount; ++keyOrd)
            {
                keys[keyOrd] = new PinnedByteArray(testLoader.GetKeyLength(rng));
                var key = new SpanByte(keys[keyOrd].Length, keys[keyOrd].Pointer);
                Assert.IsTrue(BitConverter.TryWriteBytes(keys[keyOrd].Vector, keyOrd));
                valueTester.AddRecord(keyOrd, ref key);
            }
        }

        public void Test() => testLoader.Test(tid, rng, lockOrdinals, lockKeys, ordinal => keys[ordinal].GetSpanByte(), valueTester);

        public Task TestAsync() => testLoader.TestAsync(tid, rng, lockOrdinals, lockKeys, ordinal => keys[ordinal].GetSpanByte(), valueTester);

        public void Dispose()
        {
            for (int keyOrd = 0; keyOrd < testLoader.Options.KeyCount; ++keyOrd)
                keys[keyOrd].Dispose();
        }
    }
}
