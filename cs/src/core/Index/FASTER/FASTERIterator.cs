// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.
#pragma warning disable 0162

using System;

namespace FASTER.core
{
    public partial class FasterKV<Key, Value> : FasterBase, IFasterKV<Key, Value>
    {

        /// <summary>
        /// Iterator for all (distinct) live key-values stored in FASTER
        /// </summary>
        /// <param name="untilAddress">Report records until this address (tail by default)</param>
        /// <returns>FASTER iterator</returns>
        public IFasterScanIterator<Key, Value> Iterate(long untilAddress = -1)
        {
            if (untilAddress == -1)
                untilAddress = Log.TailAddress;

            if (hlog is VariableLengthBlittableAllocator<Key, Value> varLen)
            {
                var functions = new LogVariableCompactFunctions<Key, Value, DefaultVariableCompactionFunctions<Key, Value>>(varLen, default);
                var variableLengthStructSettings = new VariableLengthStructSettings<Key, Value>
                {
                    keyLength = varLen.KeyLength,
                    valueLength = varLen.ValueLength,
                };

                return new FasterKVIterator<Key, Value, LogVariableCompactFunctions<Key, Value, DefaultVariableCompactionFunctions<Key, Value>>, DefaultVariableCompactionFunctions<Key, Value>>
                    (this, functions, default, untilAddress, variableLengthStructSettings);
            }
            else
            {
                return new FasterKVIterator<Key, Value, LogCompactFunctions<Key, Value, DefaultCompactionFunctions<Key, Value>>, DefaultCompactionFunctions<Key, Value>>
                    (this, new LogCompactFunctions<Key, Value, DefaultCompactionFunctions<Key, Value>>(default), default, untilAddress, null);
            }
        }

        /// <summary>
        /// Iterator for all (distinct) live key-values stored in FASTER
        /// </summary>
        /// <param name="compactionFunctions">User provided compaction functions (see <see cref="ICompactionFunctions{Key, Value}"/>).</param>
        /// <param name="untilAddress">Report records until this address (tail by default)</param>
        /// <returns>FASTER iterator</returns>
        public IFasterScanIterator<Key, Value> Iterate<CompactionFunctions>(CompactionFunctions compactionFunctions, long untilAddress = -1)
            where CompactionFunctions : ICompactionFunctions<Key, Value>
        {
            if (untilAddress == -1)
                untilAddress = Log.TailAddress;

            if (hlog is VariableLengthBlittableAllocator<Key, Value> varLen)
            {
                var functions = new LogVariableCompactFunctions<Key, Value, CompactionFunctions>(varLen, compactionFunctions);
                var variableLengthStructSettings = new VariableLengthStructSettings<Key, Value>
                {
                    keyLength = varLen.KeyLength,
                    valueLength = varLen.ValueLength,
                };

                return new FasterKVIterator<Key, Value, LogVariableCompactFunctions<Key, Value, CompactionFunctions>, CompactionFunctions>
                    (this, functions, compactionFunctions, untilAddress, variableLengthStructSettings);
            }
            else
            {
                return new FasterKVIterator<Key, Value, LogCompactFunctions<Key, Value, CompactionFunctions>, CompactionFunctions>
                    (this, new LogCompactFunctions<Key, Value, CompactionFunctions>(compactionFunctions), compactionFunctions, untilAddress, null);

            }
        }
    }


    internal sealed class FasterKVIterator<Key, Value, Functions, CompactionFunctions> : IFasterScanIterator<Key, Value>
        where Functions : IFunctions<Key, Value, Empty, Empty, Empty>
        where CompactionFunctions : ICompactionFunctions<Key, Value>
    {
        private readonly CompactionFunctions cf;
        private readonly FasterKV<Key, Value> fht;
        private readonly FasterKV<Key, Value> tempKv;
        private readonly ClientSession<Key, Value, Empty, Empty, Empty, Functions> fhtSession;
        private readonly ClientSession<Key, Value, Empty, Empty, Empty, Functions> tempKvSession;
        private readonly IFasterScanIterator<Key, Value> iter1;
        private IFasterScanIterator<Key, Value> iter2;

        private int enumerationPhase;

        public FasterKVIterator(FasterKV<Key, Value> fht, Functions functions, CompactionFunctions cf, long untilAddress, VariableLengthStructSettings<Key, Value> variableLengthStructSettings)
        {
            this.fht = fht;
            this.cf = cf;
            enumerationPhase = 0;
            fhtSession = fht.NewSession<Empty, Empty, Empty, Functions>(functions);
            tempKv = new FasterKV<Key, Value>(fht.IndexSize, new LogSettings { MutableFraction = 1 }, comparer: fht.Comparer, variableLengthStructSettings: variableLengthStructSettings);
            tempKvSession = tempKv.NewSession<Empty, Empty, Empty, Functions>(functions);
            iter1 = fht.Log.Scan(fht.Log.BeginAddress, untilAddress);
        }

        public long CurrentAddress => enumerationPhase == 0 ? iter1.CurrentAddress : iter2.CurrentAddress;

        public void Dispose()
        {
            iter1.Dispose();
            iter2.Dispose();
            fhtSession.Dispose();
            tempKvSession.Dispose();
            tempKv.Dispose();
        }

        public ref Key GetKey()
        {
            if (enumerationPhase == 0)
                return ref iter1.GetKey();
            return ref iter2.GetKey();
        }

        public unsafe bool GetNext(out RecordInfo recordInfo)
        {
            while (true)
            {
                if (enumerationPhase == 0)
                {
                    if (iter1.GetNext(out recordInfo))
                    {
                        ref var key = ref iter1.GetKey();
                        ref var value = ref iter1.GetValue();

                        var bucket = default(HashBucket*);
                        var slot = default(int);
                        var entry = default(HashBucketEntry);
                        var hash = fht.Comparer.GetHashCode64(ref key);
                        var tag = (ushort)((ulong)hash >> Constants.kHashTagShift);
                        if (fht.FindTag(hash, tag, ref bucket, ref slot, ref entry) && entry.Address == iter1.CurrentAddress)
                        {
                            if (recordInfo.PreviousAddress >= fht.Log.BeginAddress)
                            {
                                if (tempKvSession.ContainsKeyInMemory(ref key) == Status.OK)
                                {
                                    tempKvSession.Delete(ref key, Empty.Default, 0);
                                }
                            }

                            if (!recordInfo.Tombstone && !cf.IsDeleted(in key, in value))
                                return true;

                            continue;
                        }
                        else
                        {
                            if (recordInfo.Tombstone || cf.IsDeleted(in key, in value))
                                tempKvSession.Delete(ref key, Empty.Default, 0);
                            else
                                tempKvSession.Upsert(ref key, ref value, default, 0);
                            continue;
                        }
                    }
                    else
                    {
                        iter1.Dispose();
                        enumerationPhase = 1;
                        iter2 = tempKv.Log.Scan(tempKv.Log.BeginAddress, tempKv.Log.TailAddress);
                    }
                }

                if (enumerationPhase == 1)
                {
                    if (iter2.GetNext(out recordInfo))
                    {
                        if (!recordInfo.Tombstone)
                            return true;
                        continue;
                    }
                    else
                    {
                        iter2.Dispose();
                        enumerationPhase = 2;
                    }
                }

                recordInfo = default;
                return false;
            }
        }

        public bool GetNext(out RecordInfo recordInfo, out Key key, out Value value)
        {
            if (GetNext(out recordInfo))
            {
                if (enumerationPhase == 0)
                {
                    key = iter1.GetKey();
                    value = iter1.GetValue();
                }
                else
                {
                    key = iter2.GetKey();
                    value = iter2.GetValue();
                }
                return true;
            }

            key = default;
            value = default;

            return false;
        }

        public ref Value GetValue()
        {
            if (enumerationPhase == 0)
                return ref iter1.GetValue();
            return ref iter2.GetValue();
        }
    }
}