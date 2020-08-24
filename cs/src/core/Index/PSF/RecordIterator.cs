// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

namespace FASTER.core.Index.PSF
{
    /// <summary>
    /// Base class to implement common functionality between sync and async versions of RecordIterator.
    /// </summary>
    internal abstract class RecordIteratorBase<TRecordId, TEnumerator> where TRecordId : IComparable<TRecordId>
    {
        internal IPSF psf;
        internal int psfIndex;
        protected readonly TEnumerator enumerator;

        protected RecordIteratorBase(IPSF psf, int psfIndex, TEnumerator enumer)
        {
            this.psf = psf;
            this.psfIndex = psfIndex;
            this.enumerator = enumer;
        }

        internal bool IsDone { get; set; }

        internal abstract TRecordId Current { get; }

        internal void GetIfLower(ref TRecordId currentLowest)
        {
            if (!this.IsDone && this.Current.CompareTo(currentLowest) < 0)
                currentLowest = this.Current;
        }

        internal bool IsMatch(TRecordId recordId) => !this.IsDone && this.Current.CompareTo(recordId) == 0;

        public override string ToString() => $"psfIdx {this.psfIndex}, current {this.Current}, isDone {this.IsDone}";
    }

    /// <summary>
    /// A single PSF's stream of recordIds
    /// </summary>
    internal class RecordIterator<TRecordId> : RecordIteratorBase<TRecordId, IEnumerator<TRecordId>> where TRecordId : IComparable<TRecordId>
    {
        internal RecordIterator(IPSF psf, int psfIndex, IEnumerator<TRecordId> enumerator) : base(psf, psfIndex, enumerator) { }

        internal bool Next() 
        {
            if (!this.IsDone)
                this.IsDone = !this.enumerator.MoveNext();
            return !this.IsDone;
        }

        internal override TRecordId Current => this.enumerator.Current;
    }

#if DOTNETCORE
    /// <summary>
    /// A single PSF's async stream of recordIds
    /// </summary>
    internal class AsyncRecordIterator<TRecordId> : RecordIteratorBase<TRecordId, IAsyncEnumerator<TRecordId>> where TRecordId : IComparable<TRecordId>
    {
        internal AsyncRecordIterator(IPSF psf, int psfIndex, IAsyncEnumerator<TRecordId> enumerator) : base(psf, psfIndex, enumerator) { }

        internal async Task<bool> NextAsync()
        {
            if (!this.IsDone)
                this.IsDone = !await this.enumerator.MoveNextAsync();
            return !this.IsDone;
        }

        internal override TRecordId Current => this.enumerator.Current;
    }
#endif // DOTNETCORE

    /// <summary>
    /// Base class to implement common functionality between sync and async versions of KeyTypeRecordIterator.
    /// </summary>
    internal class KeyTypeRecordIteratorBase<TRecordId, TEnumerator> where TRecordId : IComparable<TRecordId> 
    {
        private readonly int keyTypeOrdinal;
        protected readonly RecordIteratorBase<TRecordId, TEnumerator>[] psfRecordIterators;
        protected readonly PSFQuerySettings querySettings;
        private int numDone;

        protected KeyTypeRecordIteratorBase(int keyTypeOrd, RecordIteratorBase<TRecordId, TEnumerator>[] psfRecEnums, PSFQuerySettings querySettings)
        {
            this.keyTypeOrdinal = keyTypeOrd;
            this.psfRecordIterators = psfRecEnums;
            this.querySettings = querySettings;
        }

        internal int Count => this.psfRecordIterators.Length;

        internal bool IsDone => this.numDone == this.Count;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        protected static (bool @continue, TRecordId lowest, bool first) GetIfLower(RecordIteratorBase<TRecordId, TEnumerator> recordIter, TRecordId currentLowest, bool isFirst)
        {
            if (recordIter.IsDone)
                return (true, currentLowest, isFirst);
            if (isFirst)
                return (true, recordIter.Current, false);
            recordIter.GetIfLower(ref currentLowest);
            return (true, currentLowest, false);
        }

        protected bool ContinueOnEOS(RecordIteratorBase<TRecordId, TEnumerator> recordIter)
        {
            ++this.numDone;
            return !this.querySettings.CancelOnEOS(recordIter.psf, (this.keyTypeOrdinal, recordIter.psfIndex));
        }

        internal void MarkMatchIndicators(TRecordId currentLowest, bool[] matchIndicators)
        {
            foreach (var (recordIter, psfIndex) in this.psfRecordIterators.Select((item, index) => (item, index)))
                matchIndicators[psfIndex] = recordIter.IsMatch(currentLowest);
        }

        internal IEnumerable<RecordIteratorBase<TRecordId, TEnumerator>> GetEnumeratorsMatchingPrevLowest(TRecordId previousLowest)
        {
            foreach (var (recordIter, psfIndex) in this.psfRecordIterators.Select((item, index) => (item, index)))
            {
                if (recordIter.IsDone)
                    continue;
                if (this.querySettings.IsCanceled)
                    yield break;
                if (recordIter.IsMatch(previousLowest))
                    yield return recordIter;
            }
        }

        public override string ToString() => $"keyTypeOrd {this.keyTypeOrdinal}, count {this.Count}, isDone {this.IsDone}";
    }

    /// <summary>
    /// A single TPSFKey type's vector of its PSFs' streams of recordIds (each TPSFKey type may have multiple PSFs being queried).
    /// </summary>
    internal class KeyTypeRecordIterator<TRecordId> : KeyTypeRecordIteratorBase<TRecordId, IEnumerator<TRecordId>> where TRecordId : IComparable<TRecordId>
    {
        internal KeyTypeRecordIterator(int keyTypeOrd, IPSF psf1, IEnumerator<TRecordId> psfRecordEnumerator1, PSFQuerySettings querySettings)
            : base(keyTypeOrd, new[] { new RecordIterator<TRecordId>(psf1, 0, psfRecordEnumerator1) }, querySettings)
        { }

        internal KeyTypeRecordIterator(int keyTypeOrd, IEnumerable<(IPSF psf, IEnumerator<TRecordId> psfRecEnum)> queryResults, PSFQuerySettings querySettings)
            : base(keyTypeOrd, queryResults.Select((tup, psfIdx) => new RecordIterator<TRecordId>(tup.psf, psfIdx, tup.psfRecEnum)).ToArray(), querySettings)
        { }

        #region Sync methods
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal (bool @continue, TRecordId lowest, bool first) Initialize(TRecordId currentLowest, bool isFirst)
            => IterateAndGetIfLower(true, currentLowest, currentLowest, isFirst);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal (bool @continue, TRecordId lowest, bool first) GetNextLowest(TRecordId previousLowest, TRecordId currentLowest, bool isFirst)
            => IterateAndGetIfLower(false, previousLowest, currentLowest, isFirst);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal (bool @continue, TRecordId lowest, bool first) IterateAndGetIfLower(bool isInit, TRecordId previousLowest, TRecordId currentLowest, bool isFirst)
        {
            var tuple = (true, currentLowest, isFirst);
            foreach (var recordIter in this.psfRecordIterators.Where(iter => !iter.IsDone).Cast<RecordIterator<TRecordId>>())
            {
                // If in initialization, always do the initial Next(); otherwise, advance the iterator if it matches the previous lowest record ID.
                if (((isInit || recordIter.IsMatch(previousLowest)) && !recordIter.Next() && !ContinueOnEOS(recordIter)) || querySettings.IsCanceled)
                    return (false, currentLowest, isFirst);
                tuple = GetIfLower(recordIter, tuple.currentLowest, tuple.isFirst);
            }
            return tuple;
        }
        #endregion Sync methods
    }

#if DOTNETCORE
    /// <summary>
    /// A single TPSFKey type's vector of its PSFs' async streams of recordIds (each TPSFKey type may have multiple PSFs being queried).
    /// </summary>
    internal class AsyncKeyTypeRecordIterator<TRecordId> : KeyTypeRecordIteratorBase<TRecordId, IAsyncEnumerator<TRecordId>> where TRecordId : IComparable<TRecordId>
    {
        internal AsyncKeyTypeRecordIterator(int keyTypeOrd, IPSF psf1, IAsyncEnumerator<TRecordId> psfRecordEnumerator1, PSFQuerySettings querySettings)
            : base(keyTypeOrd, new[] { new AsyncRecordIterator<TRecordId>(psf1, 0, psfRecordEnumerator1) }, querySettings)
        { }

        internal AsyncKeyTypeRecordIterator(int keyTypeOrd, IEnumerable<(IPSF psf, IAsyncEnumerator<TRecordId> psfRecEnum)> queryResults, PSFQuerySettings querySettings)
            : base(keyTypeOrd, queryResults.Select((tup, psfIdx) => new AsyncRecordIterator<TRecordId>(tup.psf, psfIdx, tup.psfRecEnum)).ToArray(), querySettings)
        { }

        #region Async methods
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal async Task<(bool @continue, TRecordId lowest, bool first)> InitializeAsync(TRecordId currentLowest, bool isFirst)
            => await IterateAndGetIfLowerAsync(true, currentLowest, currentLowest, isFirst);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal async Task<(bool @continue, TRecordId lowest, bool first)> GetNextLowestAsync(TRecordId previousLowest, TRecordId currentLowest, bool isFirst)
            => await IterateAndGetIfLowerAsync(false, previousLowest, currentLowest, isFirst);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal async Task<(bool @continue, TRecordId lowest, bool first)> IterateAndGetIfLowerAsync(bool isInit, TRecordId previousLowest, TRecordId currentLowest, bool isFirst)
        {
            var tuple = (true, currentLowest, isFirst);
            foreach (var recordIter in this.psfRecordIterators.Where(iter => !iter.IsDone).Cast<AsyncRecordIterator<TRecordId>>())
            {
                // If in initialization, always do the initial Next(); otherwise, advance the iterator if it matches the previous lowest record ID.
                if (((isInit || recordIter.IsMatch(previousLowest)) && !await recordIter.NextAsync() && !ContinueOnEOS(recordIter)) || querySettings.IsCanceled)
                    return (false, currentLowest, isFirst);
                tuple = GetIfLower(recordIter, tuple.currentLowest, tuple.isFirst);
            }
            return tuple;
        }
        #endregion Async methods
    }
#endif // DOTNETCORE

    /// <summary>
    /// Base class to implement common functionality between sync and async versions of KeyTypeRecordIterator.
    /// </summary>
    internal class QueryRecordIteratorBase<TRecordId, TEnumerator> where TRecordId : IComparable<TRecordId>
    {
        protected readonly KeyTypeRecordIteratorBase<TRecordId, TEnumerator>[] keyTypeRecordIterators;
        private readonly bool[][] matchIndicators;
        private readonly PSFQuerySettings querySettings;
        private readonly Func<bool[][], bool> callerLambda;

        protected QueryRecordIteratorBase(KeyTypeRecordIteratorBase<TRecordId, TEnumerator>[] ktris, Func<bool[][], bool> callerLambda, PSFQuerySettings querySettings)
        {
            this.keyTypeRecordIterators = ktris;
            this.matchIndicators = this.keyTypeRecordIterators.Select(ktri => new bool[ktri.Count]).ToArray();
            this.callerLambda = callerLambda;
            this.querySettings = querySettings;
        }

        protected bool CallLambda(ref TRecordId current, out bool emit)
        {
            var allDone = true;
            foreach (var (keyIter, keyIndex) in this.keyTypeRecordIterators.Select((iter, index) => (iter, index)))
            {
                keyIter.MarkMatchIndicators(current, this.matchIndicators[keyIndex]);
                allDone &= keyIter.IsDone;
            }

            allDone |= this.querySettings.IsCanceled;
            emit = !allDone && this.callerLambda(this.matchIndicators);
            return !allDone;
        }
    }

    /// <summary>
    /// The complete query's PSFs' streams of recordIds (each TPSFKey type may have multiple PSFs being queried).
    /// </summary>
    internal class QueryRecordIterator<TRecordId> : QueryRecordIteratorBase<TRecordId, IEnumerator<TRecordId>> where TRecordId : IComparable<TRecordId>
    {
        // Unfortunately we must sort to do the merge.
        private static IEnumerator<TRecordId> GetOrderedEnumerator(IEnumerable<TRecordId> enumerable) => enumerable.OrderBy(rec => rec).GetEnumerator();

        internal QueryRecordIterator(IPSF psf1, IEnumerable<TRecordId> keyRecords1, IPSF psf2, IEnumerable<TRecordId> keyRecords2,
                                     Func<bool[][], bool> callerLambda, PSFQuerySettings querySettings)
            : base(new[] {
                    new KeyTypeRecordIterator<TRecordId>(0, psf1, GetOrderedEnumerator(keyRecords1), querySettings),
                    new KeyTypeRecordIterator<TRecordId>(1, psf2, GetOrderedEnumerator(keyRecords2), querySettings)
                }, callerLambda, querySettings)
        { }

        internal QueryRecordIterator(IPSF psf1, IEnumerable<TRecordId> keyRecords1, IPSF psf2, IEnumerable<TRecordId> keyRecords2,
                                     IPSF psf3, IEnumerable<TRecordId> keyRecords3,
                                     Func<bool[][], bool> callerLambda, PSFQuerySettings querySettings)
            : base(new[] {
                    new KeyTypeRecordIterator<TRecordId>(0, psf1, GetOrderedEnumerator(keyRecords1), querySettings),
                    new KeyTypeRecordIterator<TRecordId>(1, psf2, GetOrderedEnumerator(keyRecords2), querySettings),
                    new KeyTypeRecordIterator<TRecordId>(2, psf3, GetOrderedEnumerator(keyRecords3), querySettings)
                }, callerLambda, querySettings)
        { }

        internal QueryRecordIterator(IEnumerable<IEnumerable<(IPSF psf, IEnumerable<TRecordId> keyRecEnums)>> keyTypeQueryResultsEnum,
                                     Func<bool[][], bool> callerLambda, PSFQuerySettings querySettings)
            : base(keyTypeQueryResultsEnum.Select((ktqr, index) => new KeyTypeRecordIterator<TRecordId>(index, ktqr.Select(tuple => (tuple.psf, GetOrderedEnumerator(tuple.keyRecEnums))), querySettings)).ToArray(),
                   callerLambda, querySettings)
        { }

        #region Sync methods
        internal IEnumerable<TRecordId> Run()
        {
            // The tuple is necessary due to async prohibition of byref parameters.
            (bool @continue, TRecordId current, bool isFirst) tuple = (true, default, true);
            foreach (var keyIter in this.keyTypeRecordIterators.Cast<KeyTypeRecordIterator<TRecordId>>())
            {
                tuple = keyIter.Initialize(tuple.current, tuple.isFirst);
                if (!tuple.@continue)
                    yield break;
            }

            while (true)
            {
                if (!CallLambda(ref tuple.current, out bool emit))
                    yield break;
                if (emit)
                    yield return tuple.current;

                var prevLowest = tuple.current;
                tuple.isFirst = true;
                foreach (var keyIter in this.keyTypeRecordIterators.Cast<KeyTypeRecordIterator<TRecordId>>())
                {
                    // TODOperf: consider a PQ here. Given that we have to go through all matchIndicators anyway, at what number of streams would the additional complexity improve speed?
                    tuple = keyIter.GetNextLowest(prevLowest, tuple.current, tuple.isFirst);
                    if (!tuple.@continue)
                        yield break;
                }
            }
        }
        #endregion Sync methods
    }

#if DOTNETCORE
    /// <summary>
    /// The complete query's PSFs' async streams of recordIds (each TPSFKey type may have multiple PSFs being queried).
    /// </summary>
    internal class AsyncQueryRecordIterator<TRecordId> : QueryRecordIteratorBase<TRecordId, IAsyncEnumerator<TRecordId>> where TRecordId : IComparable<TRecordId>
    {
        // Unfortunately we must sort to do the merge.
        private static IAsyncEnumerator<TRecordId> GetOrderedEnumerator(IAsyncEnumerable<TRecordId> enumerable) => enumerable.OrderBy(rec => rec).GetAsyncEnumerator();

        internal AsyncQueryRecordIterator(IPSF psf1, IAsyncEnumerable<TRecordId> keyRecords1, IPSF psf2, IAsyncEnumerable<TRecordId> keyRecords2,
                                     Func<bool[][], bool> callerLambda, PSFQuerySettings querySettings)
            : base(new[] {
                    new AsyncKeyTypeRecordIterator<TRecordId>(0, psf1, GetOrderedEnumerator(keyRecords1), querySettings),
                    new AsyncKeyTypeRecordIterator<TRecordId>(1, psf2, GetOrderedEnumerator(keyRecords2), querySettings)
                }, callerLambda, querySettings)
        { }

        internal AsyncQueryRecordIterator(IPSF psf1, IAsyncEnumerable<TRecordId> keyRecords1, IPSF psf2, IAsyncEnumerable<TRecordId> keyRecords2,
                                     IPSF psf3, IAsyncEnumerable<TRecordId> keyRecords3,
                                     Func<bool[][], bool> callerLambda, PSFQuerySettings querySettings)
            : base(new[] {
                    new AsyncKeyTypeRecordIterator<TRecordId>(0, psf1, GetOrderedEnumerator(keyRecords1), querySettings),
                    new AsyncKeyTypeRecordIterator<TRecordId>(1, psf2, GetOrderedEnumerator(keyRecords2), querySettings),
                    new AsyncKeyTypeRecordIterator<TRecordId>(2, psf3, GetOrderedEnumerator(keyRecords3), querySettings)
                }, callerLambda, querySettings)
        { }

        internal AsyncQueryRecordIterator(IEnumerable<IEnumerable<(IPSF psf, IAsyncEnumerable<TRecordId> keyRecEnums)>> keyTypeQueryResultsEnum,
                                     Func<bool[][], bool> callerLambda, PSFQuerySettings querySettings)
            : base(keyTypeQueryResultsEnum.Select((ktqr, index) => new AsyncKeyTypeRecordIterator<TRecordId>(index, ktqr.Select(tuple => (tuple.psf, GetOrderedEnumerator(tuple.keyRecEnums))), querySettings)).ToArray(),
                   callerLambda, querySettings)
        { }

        #region Sync methods
        internal async IAsyncEnumerable<TRecordId> Run()
        {
            // The tuple is necessary due to async prohibition of byref parameters.
            (bool @continue, TRecordId current, bool isFirst) tuple = (true, default, true);
            foreach (var keyIter in this.keyTypeRecordIterators.Cast<AsyncKeyTypeRecordIterator<TRecordId>>())
            {
                tuple = await keyIter.InitializeAsync(tuple.current, tuple.isFirst);
                if (!tuple.@continue)
                    yield break;
            }

            while (true)
            {
                if (!CallLambda(ref tuple.current, out bool emit))
                    yield break;
                if (emit)
                    yield return tuple.current;

                var prevLowest = tuple.current;
                tuple.isFirst = true;
                foreach (var keyIter in this.keyTypeRecordIterators.Cast<AsyncKeyTypeRecordIterator<TRecordId>>())
                {
                    // TODOperf: consider a PQ here. Given that we have to go through all matchIndicators anyway, at what number of streams would the additional complexity improve speed?
                    tuple = await keyIter.GetNextLowestAsync(prevLowest, tuple.current, tuple.isFirst);
                    if (!tuple.@continue)
                        yield break;
                }
            }
        }
        #endregion Sync methods
    }
#endif // DOTNETCORE
}
