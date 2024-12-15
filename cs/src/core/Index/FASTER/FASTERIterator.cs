// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using Microsoft.Extensions.Logging;
using System;
using System.Runtime.CompilerServices;

namespace FASTER.core
{
    public partial class FasterKV<Key, Value> : FasterBase, IFasterKV<Key, Value>
    {
        /// <summary>
        /// Pull iterator for all (distinct) live key-values stored in FASTER
        /// </summary>
        /// <param name="functions">Functions used to manage key-values during iteration</param>
        /// <param name="untilAddress">Report records until this address (tail by default)</param>
        /// <returns>FASTER iterator</returns>
        public IFasterScanIterator<Key, Value> Iterate<Input, Output, Context, Functions>(Functions functions, long untilAddress = -1)
            where Functions : IFunctions<Key, Value, Input, Output, Context>
        {
            if (untilAddress == -1)
                untilAddress = Log.TailAddress;
            return new FasterKVIterator<Key, Value, Input, Output, Context, Functions>(this, functions, untilAddress, isPull: true, loggerFactory: loggerFactory);
        }

        /// <summary>
        /// Push iteration of all (distinct) live key-values stored in FASTER
        /// </summary>
        /// <param name="functions">Functions used to manage key-values during iteration</param>
        /// <param name="scanFunctions">Functions receiving pushed records</param>
        /// <param name="untilAddress">Report records until this address (tail by default)</param>
        /// <returns>FASTER iterator</returns>
        public bool Iterate<Input, Output, Context, Functions, TScanFunctions>(Functions functions, ref TScanFunctions scanFunctions, long untilAddress = -1)
            where Functions : IFunctions<Key, Value, Input, Output, Context>
            where TScanFunctions : IScanIteratorFunctions<Key, Value>
        {
            if (untilAddress == -1)
                untilAddress = Log.TailAddress;
            using FasterKVIterator<Key, Value, Input, Output, Context, Functions> iter = new(this, functions, untilAddress, isPull: false, loggerFactory: loggerFactory);

            if (!scanFunctions.OnStart(iter.BeginAddress, iter.EndAddress))
                return false;

            long numRecords = 1;
            bool stop = false;
            for ( ; !stop && iter.PushNext(ref scanFunctions, numRecords, out stop); ++numRecords)
                ;

            scanFunctions.OnStop(!stop, numRecords);
            return !stop;
        }

        /// <summary>
        /// Iterator for all (distinct) live key-values stored in FASTER
        /// </summary>
        /// <param name="untilAddress">Report records until this address (tail by default)</param>
        /// <returns>FASTER iterator</returns>
        [Obsolete("Invoke Iterate() on a client session (ClientSession), or use store.Iterate overload with Functions provided as parameter")]
        public IFasterScanIterator<Key, Value> Iterate(long untilAddress = -1) 
            => throw new FasterException("Invoke Iterate() on a client session (ClientSession), or use store.Iterate overload with Functions provided as parameter");

        /// <summary>
        /// Iterator for all (distinct) live key-values stored in FASTER
        /// </summary>
        /// <param name="compactionFunctions">User provided compaction functions (see <see cref="ICompactionFunctions{Key, Value}"/>).</param>
        /// <param name="untilAddress">Report records until this address (tail by default)</param>
        /// <returns>FASTER iterator</returns>
        [Obsolete("Invoke Iterate() on a client session (ClientSession), or use store.Iterate overload with Functions provided as parameter")]
        public IFasterScanIterator<Key, Value> Iterate<CompactionFunctions>(CompactionFunctions compactionFunctions, long untilAddress = -1)
            where CompactionFunctions : ICompactionFunctions<Key, Value> 
            => throw new FasterException("Invoke Iterate() on a client session (ClientSession), or use store.Iterate overload with Functions provided as parameter");
    }

    internal sealed class FasterKVIterator<Key, Value, Input, Output, Context, Functions> : IFasterScanIterator<Key, Value>
        where Functions : IFunctions<Key, Value, Input, Output, Context>
    {
        private readonly FasterKV<Key, Value> fht;
        private readonly FasterKV<Key, Value> tempKv;
        private readonly ClientSession<Key, Value, Input, Output, Context, Functions> tempKvSession;
        private readonly IFasterScanIterator<Key, Value> mainKvIter;
        private readonly IPushScanIterator<Key> pushScanIterator;
        private IFasterScanIterator<Key, Value> tempKvIter;

        enum IterationPhase {
            MainKv,     // Iterating fht; if the record is the tailmost for the fht tag chain, then return it, else add it to tempKv.
            TempKv,     // Return records from tempKv.
            Done        // Done iterating tempKv; Iterator is complete.
        };
        private IterationPhase iterationPhase;

        public FasterKVIterator(FasterKV<Key, Value> fht, Functions functions, long untilAddress, bool isPull, ILoggerFactory loggerFactory = null)
        {
            this.fht = fht;
            iterationPhase = IterationPhase.MainKv;

            VariableLengthStructSettings<Key, Value> variableLengthStructSettings = null;
            if (fht.hlog is VariableLengthBlittableAllocator<Key, Value> varLen)
            {
                variableLengthStructSettings = new VariableLengthStructSettings<Key, Value>
                {
                    keyLength = varLen.KeyLength,
                    valueLength = varLen.ValueLength,
                };
            }

            tempKv = new FasterKV<Key, Value>(fht.IndexSize, new LogSettings { LogDevice = new NullDevice(), ObjectLogDevice = new NullDevice(), MutableFraction = 1 }, comparer: fht.Comparer,
                                              variableLengthStructSettings: variableLengthStructSettings, loggerFactory: loggerFactory, concurrencyControlMode: ConcurrencyControlMode.None);
            tempKvSession = tempKv.NewSession<Input, Output, Context, Functions>(functions);
            mainKvIter = fht.Log.Scan(fht.Log.BeginAddress, untilAddress);
            pushScanIterator = mainKvIter as IPushScanIterator<Key>;
        }

        public long CurrentAddress => iterationPhase == IterationPhase.MainKv ? mainKvIter.CurrentAddress : tempKvIter.CurrentAddress;

        public long NextAddress => iterationPhase == IterationPhase.MainKv ? mainKvIter.NextAddress : tempKvIter.NextAddress;

        public long BeginAddress => iterationPhase == IterationPhase.MainKv ? mainKvIter.BeginAddress : tempKvIter.BeginAddress;

        public long EndAddress => iterationPhase == IterationPhase.MainKv ? mainKvIter.EndAddress : tempKvIter.EndAddress;

        public void Dispose()
        {
            mainKvIter?.Dispose();
            tempKvIter?.Dispose();
            tempKvSession?.Dispose();
            tempKv?.Dispose();
        }

        public ref Key GetKey() => ref iterationPhase == IterationPhase.MainKv ? ref mainKvIter.GetKey() : ref tempKvIter.GetKey();

        public ref Value GetValue() => ref iterationPhase == IterationPhase.MainKv ? ref mainKvIter.GetValue() : ref tempKvIter.GetValue();

        public bool GetNext(out RecordInfo recordInfo)
        {
            while (true)
            {
                if (iterationPhase == IterationPhase.MainKv)
                {
                    if (mainKvIter.GetNext(out recordInfo))
                    {
                        ref var key = ref mainKvIter.GetKey();
                        OperationStackContext<Key, Value> stackCtx = default;
                        if (IsTailmostMainKvRecord(ref key, recordInfo, ref stackCtx))
                        {
                            if (recordInfo.Tombstone)
                                continue;
                            return true;
                        }

                        ProcessNonTailmostMainKvRecord(recordInfo, key);
                        continue;
                    }

                    // Done with MainKv; dispose mainKvIter, initialize tempKvIter, and drop through to TempKv iteration.
                    mainKvIter.Dispose();
                    iterationPhase = IterationPhase.TempKv;
                    tempKvIter = tempKv.Log.Scan(tempKv.Log.BeginAddress, tempKv.Log.TailAddress);
                }

                if (iterationPhase == IterationPhase.TempKv)
                {
                    if (tempKvIter.GetNext(out recordInfo))
                    {
                        if (!recordInfo.Tombstone)
                            return true;
                        continue;
                    }

                    // Done with TempKv iteration, so we're done. Drop through to Done handling.
                    tempKvIter.Dispose();
                    iterationPhase = IterationPhase.Done;
                }

                // We're done. This handles both the call that exhausted tempKvIter, and any subsequent calls on this outer iterator.
                recordInfo = default;
                return false;
            }
        }

        internal bool PushNext<TScanFunctions>(ref TScanFunctions scanFunctions, long numRecords, out bool stop)
            where TScanFunctions : IScanIteratorFunctions<Key, Value>
        {
            while (true)
            {
                if (iterationPhase == IterationPhase.MainKv)
                {
                    OperationStackContext<Key, Value> stackCtx = default;
                    if (mainKvIter.GetNext(out var recordInfo))
                    {
                        try
                        {
                            ref var key = ref mainKvIter.GetKey();
                            if (IsTailmostMainKvRecord(ref key, recordInfo, ref stackCtx))
                            {
                                if (recordInfo.Tombstone)
                                    continue;

                                // Push Iter records are in temp storage so do not need locks, but we'll call ConcurrentReader because, for example, GenericAllocator
                                // may need to know the object is in that region.
                                if (mainKvIter.CurrentAddress >= fht.hlog.ReadOnlyAddress)
                                    stop = !scanFunctions.ConcurrentReader(ref key, ref mainKvIter.GetValue(), new RecordMetadata(recordInfo, mainKvIter.CurrentAddress), numRecords);
                                else
                                    stop = !scanFunctions.SingleReader(ref key, ref mainKvIter.GetValue(), new RecordMetadata(recordInfo, mainKvIter.CurrentAddress), numRecords);
                                return !stop;
                            }

                            ProcessNonTailmostMainKvRecord(recordInfo, key);
                            continue;
                        }
                        catch (Exception ex)
                        {
                            scanFunctions.OnException(ex, numRecords);
                            throw;
                        }
                        finally
                        {
                            if (stackCtx.recSrc.HasLock)
                                fht.UnlockForScan(ref stackCtx, ref mainKvIter.GetKey(), ref pushScanIterator.GetLockableInfo());
                        }
                    }

                    // Done with MainKv; dispose mainKvIter, initialize tempKvIter, and drop through to TempKv iteration.
                    mainKvIter.Dispose();
                    iterationPhase = IterationPhase.TempKv;
                    tempKvIter = tempKv.Log.Scan(tempKv.Log.BeginAddress, tempKv.Log.TailAddress);
                }

                if (iterationPhase == IterationPhase.TempKv)
                {
                    if (tempKvIter.GetNext(out var recordInfo))
                    {
                        if (!recordInfo.Tombstone)
                        { 
                            stop = !scanFunctions.SingleReader(ref tempKvIter.GetKey(), ref tempKvIter.GetValue(), new RecordMetadata(recordInfo, tempKvIter.CurrentAddress), numRecords);
                            return !stop;
                        }
                        continue;
                    }

                    // Done with TempKv iteration, so we're done. Drop through to Done handling.
                    tempKvIter.Dispose();
                    iterationPhase = IterationPhase.Done;
                }

                // We're done. This handles both the call that exhausted tempKvIter, and any subsequent calls on this outer iterator.
                stop = false;
                return false;
            }
        }

        private void ProcessNonTailmostMainKvRecord(RecordInfo recordInfo, Key key)
        {
            // Not the tailmost record in the tag chain so add it to or remove it from tempKV (we want to return only the latest version).
            if (recordInfo.Tombstone)
            {
                // Check if it's in-memory first so we don't spuriously create a tombstone record.
                if (tempKvSession.ContainsKeyInMemory(ref key, out _).Found)
                    tempKvSession.Delete(ref key);
            }
            else
                tempKvSession.Upsert(ref key, ref mainKvIter.GetValue());
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        bool IsTailmostMainKvRecord(ref Key key, RecordInfo recordInfo, ref OperationStackContext<Key, Value> stackCtx)
        {
            stackCtx = new(fht.comparer.GetHashCode64(ref key));
            if (fht.FindTag(ref stackCtx.hei))
            {
                stackCtx.SetRecordSourceToHashEntry(fht.hlog);
                if (fht.UseReadCache)
                    fht.SkipReadCache(ref stackCtx, out _);
                if (stackCtx.recSrc.LogicalAddress == mainKvIter.CurrentAddress)
                {
                    // The tag chain starts with this record, so we won't see this key again; remove it from tempKv if we've seen it before.
                    if (recordInfo.PreviousAddress >= fht.Log.BeginAddress)
                    {
                        // Check if it's in-memory first so we don't spuriously create a tombstone record.
                        if (tempKvSession.ContainsKeyInMemory(ref key, out _).Found)
                            tempKvSession.Delete(ref key);
                    }

                    // Let the caller process it directly within the mainKvIter loop, including detecting that the record is deleted (and thus tempKv has been handled here).
                    return true;
                }
            }
            return false;
        }

        public bool GetNext(out RecordInfo recordInfo, out Key key, out Value value)
        {
            if (GetNext(out recordInfo))
            {
                if (iterationPhase == IterationPhase.MainKv)
                {
                    key = mainKvIter.GetKey();
                    value = mainKvIter.GetValue();
                }
                else
                {
                    key = tempKvIter.GetKey();
                    value = tempKvIter.GetValue();
                }
                return true;
            }

            key = default;
            value = default;
            return false;
        }
    }
}