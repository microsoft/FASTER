// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using Microsoft.Extensions.Logging;
using System;

namespace FASTER.core
{
    public partial class FasterKV<Key, Value> : FasterBase, IFasterKV<Key, Value>
    {
        /// <summary>
        /// Iterator for all (distinct) live key-values stored in FASTER
        /// </summary>
        /// <param name="functions">Functions used to manage key-values during iteration</param>
        /// <param name="untilAddress">Report records until this address (tail by default)</param>
        /// <returns>FASTER iterator</returns>
        public IFasterScanIterator<Key, Value> Iterate<Input, Output, Context, Functions>(Functions functions, long untilAddress = -1)
            where Functions : IFunctions<Key, Value, Input, Output, Context>
        {
            if (untilAddress == -1)
                untilAddress = Log.TailAddress;

            return new FasterKVIterator<Key, Value, Input, Output, Context, Functions>
                (this, functions, untilAddress, loggerFactory: loggerFactory);
        }


        /// <summary>
        /// Iterator for all (distinct) live key-values stored in FASTER
        /// </summary>
        /// <param name="untilAddress">Report records until this address (tail by default)</param>
        /// <returns>FASTER iterator</returns>
        [Obsolete("Invoke Iterate() on a client session (ClientSession), or use store.Iterate overload with Functions provided as parameter")]
        public IFasterScanIterator<Key, Value> Iterate(long untilAddress = -1)
        {
            throw new FasterException("Invoke Iterate() on a client session (ClientSession), or use store.Iterate overload with Functions provided as parameter");
        }

        /// <summary>
        /// Iterator for all (distinct) live key-values stored in FASTER
        /// </summary>
        /// <param name="compactionFunctions">User provided compaction functions (see <see cref="ICompactionFunctions{Key, Value}"/>).</param>
        /// <param name="untilAddress">Report records until this address (tail by default)</param>
        /// <returns>FASTER iterator</returns>
        [Obsolete("Invoke Iterate() on a client session (ClientSession), or use store.Iterate overload with Functions provided as parameter")]
        public IFasterScanIterator<Key, Value> Iterate<CompactionFunctions>(CompactionFunctions compactionFunctions, long untilAddress = -1)
            where CompactionFunctions : ICompactionFunctions<Key, Value>
        {
            throw new FasterException("Invoke Iterate() on a client session (ClientSession), or use store.Iterate overload with Functions provided as parameter");
        }
    }


    internal sealed class FasterKVIterator<Key, Value, Input, Output, Context, Functions> : IFasterScanIterator<Key, Value>
        where Functions : IFunctions<Key, Value, Input, Output, Context>
    {
        private readonly FasterKV<Key, Value> fht;
        private readonly FasterKV<Key, Value> tempKv;
        private readonly ClientSession<Key, Value, Input, Output, Context, Functions> tempKvSession;
        private readonly IFasterScanIterator<Key, Value> iter1;
        private IFasterScanIterator<Key, Value> iter2;

        private int enumerationPhase;

        public FasterKVIterator(FasterKV<Key, Value> fht, Functions functions, long untilAddress, ILoggerFactory loggerFactory = null)
        {
            this.fht = fht;
            enumerationPhase = 0;

            VariableLengthStructSettings<Key, Value> variableLengthStructSettings = null;
            if (fht.hlog is VariableLengthBlittableAllocator<Key, Value> varLen)
            {
                variableLengthStructSettings = new VariableLengthStructSettings<Key, Value>
                {
                    keyLength = varLen.KeyLength,
                    valueLength = varLen.ValueLength,
                };
            }

            tempKv = new FasterKV<Key, Value>(fht.IndexSize, new LogSettings { LogDevice = new NullDevice(), ObjectLogDevice = new NullDevice(), MutableFraction = 1 }, comparer: fht.Comparer, variableLengthStructSettings: variableLengthStructSettings, loggerFactory: loggerFactory);
            tempKvSession = tempKv.NewSession<Input, Output, Context, Functions>(functions);
            iter1 = fht.Log.Scan(fht.Log.BeginAddress, untilAddress);
        }

        public long CurrentAddress => enumerationPhase == 0 ? iter1.CurrentAddress : iter2.CurrentAddress;

        public long NextAddress => enumerationPhase == 0 ? iter1.NextAddress : iter2.NextAddress;

        public long BeginAddress => enumerationPhase == 0 ? iter1.BeginAddress : iter2.BeginAddress;

        public long EndAddress => enumerationPhase == 0 ? iter1.EndAddress : iter2.EndAddress;

        public void Dispose()
        {
            iter1?.Dispose();
            iter2?.Dispose();
            tempKvSession?.Dispose();
            tempKv?.Dispose();
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

                        HashEntryInfo hei = new(fht.Comparer.GetHashCode64(ref key));
                        if (fht.FindTag(ref hei) && hei.entry.Address == iter1.CurrentAddress)
                        {
                            if (recordInfo.PreviousAddress >= fht.Log.BeginAddress)
                            {
                                if (tempKvSession.ContainsKeyInMemory(ref key, out _).Found)
                                {
                                    tempKvSession.Delete(ref key);
                                }
                            }

                            if (!recordInfo.Tombstone)
                                return true;

                            continue;
                        }
                        else
                        {
                            if (recordInfo.Tombstone)
                                tempKvSession.Delete(ref key);
                            else
                                tempKvSession.Upsert(ref key, ref value);
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