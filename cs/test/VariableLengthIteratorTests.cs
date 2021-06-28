// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using FASTER.core;
using NUnit.Framework;

namespace FASTER.test
{
    [TestFixture]
    public class IteratorTests
    {
        [Test]
        [Category("FasterKV")]
        public void ShouldSkipEmptySpaceAtEndOfPage()
        {
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir, wait: true);

            var vlLength = new VLValue();
            var log = Devices.CreateLogDevice(TestUtils.MethodTestDir + "/hlog-vl-iter.log", deleteOnClose: true);
            var fht = new FasterKV<Key, VLValue>
                (128,
                new LogSettings { LogDevice = log, MemorySizeBits = 17, PageSizeBits = 10 }, // 1KB page
                null, null, null, new VariableLengthStructSettings<Key, VLValue> { valueLength = vlLength }
                );

            var session = fht.NewSession(new VLFunctions());

            try
            {
                Set(1L, 200, 1);// page#0
                Set(2L, 200, 2);// page#1 because there is not enough space in page#0

                var len = 1024; // fill page#1 exactly
                len = len - 2 * RecordInfo.GetLength() - 2 * 8 - vlLength.GetLength(ref GetValue(200, 2));

                Set(3, len / 4, 3); // should be in page#1

                Set(4, 64, 4);

                session.CompletePending(true);

                var data = new List<Tuple<long, int, int>>();
                using (var iterator = fht.Log.Scan(fht.Log.BeginAddress, fht.Log.TailAddress))
                {
                    while (iterator.GetNext(out var info))
                    {
                        ref var scanKey = ref iterator.GetKey();
                        ref var scanValue = ref iterator.GetValue();

                        data.Add(Tuple.Create(scanKey.key, scanValue.length, scanValue.field1));
                    }
                }

                Assert.AreEqual(4, data.Count);

                Assert.AreEqual(200, data[1].Item2);
                Assert.AreEqual(2, data[1].Item3);

                Assert.AreEqual(3, data[2].Item1);
                Assert.AreEqual(3, data[2].Item3);

                Assert.AreEqual(4, data[3].Item1);
                Assert.AreEqual(64, data[3].Item2);
                Assert.AreEqual(4, data[3].Item3);
            }
            finally
            {
                session.Dispose();
                fht.Dispose();
                log.Dispose();
            }
            TestUtils.DeleteDirectory(TestUtils.MethodTestDir);

            void Set(long keyValue, int length, int tag)
            {
                var key = new Key() { key = keyValue };
                ref var value = ref GetValue(length, tag);
                session.Upsert(ref key, ref value, Empty.Default, 0);
            }
        }

        private static ref VLValue GetValue(int length, int tag)
        {
            var data = new byte[length * 4];
            ref var value = ref Unsafe.As<byte, VLValue>(ref data[0]);
            value.length = length;
            value.field1 = tag;
            return ref value;
        }
    }
}
