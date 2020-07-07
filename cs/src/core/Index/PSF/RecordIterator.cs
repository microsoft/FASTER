// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;

namespace FASTER.core.Index.PSF
{
    /// <summary>
    /// A single PSF's stream of recordIds
    /// </summary>
    internal class RecordIterator<TRecordId> where TRecordId : IComparable<TRecordId>
    {
        private readonly IEnumerator<TRecordId> enumerator;

        // Unfortunately we must sort to do the merge.
        internal RecordIterator(IEnumerable<TRecordId> enumer) => this.enumerator = enumer.OrderBy(rec => rec).GetEnumerator();

        internal bool Next() 
        {
            if (!this.IsDone)
                this.IsDone = !this.enumerator.MoveNext();
            return !this.IsDone;
        }

        internal bool IsDone { get; set; }

        internal TRecordId Current => this.enumerator.Current;

        internal void GetIfLower(ref TRecordId currentLowest)
        {
            if (!this.IsDone && this.enumerator.Current.CompareTo(currentLowest) < 0)
                currentLowest = this.enumerator.Current;
        }

        internal bool IsMatch(TRecordId recordId) => !this.IsDone && this.enumerator.Current.CompareTo(recordId) == 0;
    }

    /// <summary>
    /// A single TPSFKey type's vector of its PSFs' streams of recordIds (each TPSFKey type may have multiple PSFs being queried).
    /// </summary>
    internal class KeyTypeRecordIterator<TRecordId> where TRecordId : IComparable<TRecordId>
    {
        private struct PsfRecords
        {
            internal IPSF psf;
            internal RecordIterator<TRecordId> iterator;
        }

        private readonly int keyTypeOrdinal;
        private readonly PsfRecords[] psfRecords;
        private readonly PSFQuerySettings querySettings;
        private int numDone;

        internal KeyTypeRecordIterator(int keyTypeOrd, IPSF psf1, IEnumerable<TRecordId> psfRecordEnumerator1, PSFQuerySettings querySettings)
            : this(keyTypeOrd, new[] { new PsfRecords { psf = psf1, iterator = new RecordIterator<TRecordId>(psfRecordEnumerator1) } }, querySettings)
        { }

        internal KeyTypeRecordIterator(int keyTypeOrd, IEnumerable<(IPSF psf, IEnumerable<TRecordId> psfRecEnum)> queryResults, PSFQuerySettings querySettings)
            : this(keyTypeOrd, queryResults.Select(tup => new PsfRecords { psf = tup.psf, iterator = new RecordIterator<TRecordId>(tup.psfRecEnum) }).ToArray(), querySettings)
        { }

        private KeyTypeRecordIterator(int keyTypeOrd, PsfRecords[] psfRecs, PSFQuerySettings querySettings)
        {
            this.keyTypeOrdinal = keyTypeOrd;
            this.psfRecords = psfRecs;
            this.querySettings = querySettings;
        }

        internal int Count => this.psfRecords.Length;

        internal bool IsDone => this.numDone == this.Count;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void GetIfLower(RecordIterator<TRecordId> recordIter, ref TRecordId currentLowest, ref bool isFirst)
        {
            if (recordIter.IsDone)
                return;
            if (isFirst)
            {
                currentLowest = recordIter.Current;
                isFirst = false;
                return;
            }
            recordIter.GetIfLower(ref currentLowest);
        }

        internal bool Initialize(ref TRecordId currentLowest, ref bool isFirst)
        {
            foreach (var (recordIter, psfIndex) in this.psfRecords.Select((item, index) => (item.iterator, index)))
            {
                if (!this.Next(recordIter, psfIndex) || querySettings.IsCanceled)
                    return false;
                this.GetIfLower(recordIter, ref currentLowest, ref isFirst);
            }
            return true;
        }

        private bool Next(RecordIterator<TRecordId> recordIter, int psfIndex)
        {
            if (!recordIter.Next())
            {
                ++this.numDone;
                if (this.querySettings.CancelOnEOS(this.psfRecords[psfIndex].psf, (this.keyTypeOrdinal, psfIndex)))
                    return false;
            }
            return true;
        }

        internal bool GetNextLowest(TRecordId previousLowest, ref TRecordId currentLowest, ref bool isFirst)
        {
            foreach (var (recordIter, psfIndex) in this.psfRecords.Select((item, index) => (item.iterator, index)))
            {
                if (recordIter.IsDone)
                    continue;
                if (querySettings.IsCanceled)
                    return false;
                if (recordIter.IsMatch(previousLowest) && !this.Next(recordIter, psfIndex))
                    return false;
                this.GetIfLower(recordIter, ref currentLowest, ref isFirst);
            }
            return true;
        }

        internal void MarkMatchIndicators(TRecordId currentLowest, bool[] matchIndicators)
        {
            foreach (var (recordIter, psfIndex) in this.psfRecords.Select((item, index) => (item.iterator, index)))
                matchIndicators[psfIndex] = recordIter.IsMatch(currentLowest);
        }
    }

    /// <summary>
    /// The complete query's PSFs' streams of recordIds (each TPSFKey type may have multiple PSFs being queried).
    /// </summary>
    internal class QueryRecordIterator<TRecordId> where TRecordId : IComparable<TRecordId>
    {
        private readonly KeyTypeRecordIterator<TRecordId>[] keyTypeRecordIterators;
        private readonly bool[][] matchIndicators;
        private readonly PSFQuerySettings querySettings;
        private readonly Func<bool[][], bool> callerLambda;

        internal QueryRecordIterator(IPSF psf1, IEnumerable<TRecordId> keyRecords1, IPSF psf2, IEnumerable<TRecordId> keyRecords2,
                                     Func<bool[][], bool> callerLambda, PSFQuerySettings querySettings)
            : this(new[] {
                    new KeyTypeRecordIterator<TRecordId>(0, psf1, keyRecords1, querySettings),
                    new KeyTypeRecordIterator<TRecordId>(1, psf2, keyRecords2, querySettings)
                }, callerLambda, querySettings)
        { }

        internal QueryRecordIterator(IPSF psf1, IEnumerable<TRecordId> keyRecords1, IPSF psf2, IEnumerable<TRecordId> keyRecords2,
                                     IPSF psf3, IEnumerable<TRecordId> keyRecords3,
                                     Func<bool[][], bool> callerLambda, PSFQuerySettings querySettings)
            : this(new[] {
                    new KeyTypeRecordIterator<TRecordId>(0, psf1, keyRecords1, querySettings),
                    new KeyTypeRecordIterator<TRecordId>(1, psf2, keyRecords2, querySettings),
                    new KeyTypeRecordIterator<TRecordId>(2, psf3, keyRecords3, querySettings)
                }, callerLambda, querySettings)
        { }

        internal QueryRecordIterator(IEnumerable<IEnumerable<(IPSF psf, IEnumerable<TRecordId> keyRecEnums)>> keyTypeQueryResultsEnum,
                                     Func<bool[][], bool> callerLambda, PSFQuerySettings querySettings)
            : this(keyTypeQueryResultsEnum.Select((ktqr, index) => new KeyTypeRecordIterator<TRecordId>(index, ktqr, querySettings)).ToArray(),
                   callerLambda, querySettings)
        { }

        private QueryRecordIterator(KeyTypeRecordIterator<TRecordId>[] ktris, Func<bool[][], bool> callerLambda, PSFQuerySettings querySettings)
        {
            this.keyTypeRecordIterators = ktris;
            this.matchIndicators = this.keyTypeRecordIterators.Select(ktri => new bool[ktri.Count]).ToArray();
            this.callerLambda = callerLambda;
            this.querySettings = querySettings;
        }

        internal IEnumerable<TRecordId> Run()
        {
            TRecordId current = default;
            bool isFirst = true;
            foreach (var keyIter in this.keyTypeRecordIterators)
            {
                if (!keyIter.Initialize(ref current, ref isFirst))
                    yield break;
            }

            while(true)
            {
                var allDone = true;
                foreach (var (keyIter, keyIndex) in this.keyTypeRecordIterators.Select((iter, index) => (iter, index)))
                {
                    keyIter.MarkMatchIndicators(current, this.matchIndicators[keyIndex]);
                    allDone &= keyIter.IsDone;
                }

                if (allDone || this.querySettings.IsCanceled)
                    yield break;

                if (this.callerLambda(this.matchIndicators))
                    yield return current;

                var prevLowest = current;
                isFirst = true;
                foreach (var keyIter in this.keyTypeRecordIterators)
                {
                    // TODOperf: consider a PQ here. Given that we have to go through all matchIndicators anyway, at what number of streams would the additional complexity improve speed?
                    if (!keyIter.GetNextLowest(prevLowest, ref current, ref isFirst))
                        yield break;
                }
            }
        }
    }
}
