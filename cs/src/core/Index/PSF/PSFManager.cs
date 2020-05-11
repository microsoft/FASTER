// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;

namespace FASTER.core
{
    internal class PSFManager<TProviderData, TRecordId> where TRecordId : struct, IComparable<TRecordId>
    {
        readonly List<IExecutePSF<TProviderData, TRecordId>> psfGroups = new List<IExecutePSF<TProviderData, TRecordId>>();

        internal bool HasPSFs => this.psfGroups.Count > 0;

        internal Status Upsert(TProviderData providerData, TRecordId recordId, bool isInserted)
        {
            // TODO: This is called from ContextUpsert or InternalCompleteRetryRequest, where it's still
            // in the Context threading control for the primaryKV. I think it needs to move out of there.
            foreach (var group in this.psfGroups)
            {
                var status = group.ExecuteAndStore(providerData, recordId, isInserted);
                if (status != Status.OK)
                {
                    // TODO handle errors
                }
            }
            return Status.OK;
        }

        internal string[][] GetRegisteredPSFs() => throw new NotImplementedException("TODO");

        internal PSF<TPSFKey, TRecordId> RegisterPSF<TPSFKey>(IPSFDefinition<TProviderData, TPSFKey> def,
                                                              PSFRegistrationSettings<TPSFKey> registrationSettings)
            where TPSFKey : struct
        {
            // TODO: Runtime check that TPSFKey is blittable
            var group = new PSFGroup<TProviderData, TPSFKey, TRecordId>(this.psfGroups.Count, new[] { def }, registrationSettings);
            this.psfGroups.Add(group);
            return group[def.Name];
        }        

        internal PSF<TPSFKey, TRecordId>[] RegisterPSF<TPSFKey>(IPSFDefinition<TProviderData, TPSFKey>[] defs,
                                                              PSFRegistrationSettings<TPSFKey> registrationSettings)
            where TPSFKey : struct
        {
            // TODO: Runtime check that TPSFKey is blittable
            var group = new PSFGroup<TProviderData, TPSFKey, TRecordId>(this.psfGroups.Count, defs, registrationSettings);
            this.psfGroups.Add(group);
            return group.PSFs;
        }

        internal IEnumerable<TProviderData> QueryPSF<TPSFKey>(IPSFCreateProviderData<TRecordId, TProviderData> providerDataCreator,
                                                              PSF<TPSFKey, TRecordId> psf,
                                                              TPSFKey psfKey, PSFQuerySettings querySettings)
            where TPSFKey : struct
        {
            // TODO make sure 'psf' is from one of our groups
            // TODO if we delete groups, the GroupOrdinals in the other groups' PSFs must be updated; change to IQueryPSF
            var group = this.psfGroups[psf.GroupOrdinal];
            foreach (var recordId in psf.Query(psfKey))
            {
                if (querySettings != null && querySettings.CancellationToken.IsCancellationRequested)
                {
                    if (querySettings.ThrowOnCancellation)
                        querySettings.CancellationToken.ThrowIfCancellationRequested();
                    yield break;
                }
                var providerData = providerDataCreator.Create(recordId);
                if (providerData is null)
                    continue;
                if (group.Verify(providerData, psf.PsfOrdinal))
                    yield return providerData;
            }
        }

        internal IEnumerable<TProviderData> QueryPSF<TPSFKey>(IPSFCreateProviderData<TRecordId, TProviderData> providerDataCreator,
                                                              PSF<TPSFKey, TRecordId> psf,
                                                              TPSFKey[] psfKeys, PSFQuerySettings querySettings)
            where TPSFKey : struct
        {
            // TODO make sure 'psf' is from one of our groups

            // TODO implement range queries. This will start retrieval of a stream of returned values for the
            // chains for all specified keys for the PSF, returning them via an IEnumerable<TPSFKey> over a PQ
            // (PriorityQueue) that is populated from each key's stream. This is how multiple range query bins
            // are handled; the semantics are that a Union (via stream merge) of all records for all keys in the
            // array is done. Obviously there will be a tradeoff between the granularity of the bins and the
            // overhead of the PQ for the streams returned.
            return QueryPSF(providerDataCreator, psf, psfKeys[0], querySettings);   // TODO just to make the compiler happy
        }

        internal IEnumerable<TProviderData> QueryPSF<TPSFKey1, TPSFKey2>(
                    IPSFCreateProviderData<TRecordId, TProviderData> providerDataCreator,
                    PSF<TPSFKey1, TRecordId> psf1, TPSFKey1 psfKey1,
                    PSF<TPSFKey2, TRecordId> psf2, TPSFKey2 psfKey2,
                    Func<bool, bool, bool> matchPredicate,
                    PSFQuerySettings querySettings)
        {
            // TODO: full implementation via PQ
            using var e1 = psf1.Query(psfKey1).GetEnumerator();
            using var e2 = psf2.Query(psfKey2).GetEnumerator();

            var e1done = !e1.MoveNext();
            var e2done = !e2.MoveNext();

            var group1 = this.psfGroups[psf1.GroupOrdinal];
            var group2 = this.psfGroups[psf2.GroupOrdinal];

            var cancelToken = querySettings is null ? default : querySettings.CancellationToken;
            bool cancellationRequested()
            {
                if (querySettings is null)
                    return false;
                if (querySettings.ThrowOnCancellation)
                    cancelToken.ThrowIfCancellationRequested();
                return cancelToken.IsCancellationRequested;
            }

            while (!e1done && !e2done)
            {
                if (cancellationRequested())
                    yield break;

                // Descending order by recordId. TODO doc: Require IComparable on TRecordId
                var cmp = e1.Current.CompareTo(e2.Current);
                var predResult = cmp == 0
                    ? matchPredicate(true, true)
                    : cmp > 0 ? matchPredicate(true, false) : matchPredicate(false, true);
                if (predResult)
                {
                    // Let the trailing one catch up
                    var providerData = providerDataCreator.Create(cmp < 0 ? e1.Current : e2.Current);
                    var verify = cmp <= 0 ? group1.Verify(providerData, psf1.PsfOrdinal) : true
                                 && cmp >= 0 ? group2.Verify(providerData, psf2.PsfOrdinal) : true;
                }
                if (cmp <= 0)
                    e1done = !e1.MoveNext();
                if (cmp >= 0)
                    e2done = !e2.MoveNext();
            }

            // If all streams are done, normal conclusion.
            if (e1done && e2done)
                yield break;

            // At least one stream is still alive, but not all. See if they registered a callback.
            if (!(querySettings is null) && !(querySettings.OnStreamEnded is null))
            {
                if (!querySettings.OnStreamEnded(e1done ? (IPSF)psf1 : psf2, e1done ? 0 : 1))
                    yield break;
            }

            while ((!e1done || !e2done) && !cancellationRequested())
            { 
                var predResult = matchPredicate(!e1done, !e2done);
                if (predResult)
                {
                    //yield return providerDataCreator(!e1done ? e1.Current : e2.Current);
                    var providerData = providerDataCreator.Create(!e1done ? e1.Current : e2.Current);
                    if (!(providerData is null))
                    {
                        var psfOrdinal = !e1done ? psf1.PsfOrdinal : psf2.PsfOrdinal;
                        if ((!e1done ? group1 : group2).Verify(providerData, psfOrdinal))
                            yield return providerData;
                    }
                }
                if (!(!e1done ? e1 : e2).MoveNext())
                    yield break;
            }
        }

        internal IEnumerable<TProviderData> QueryPSF<TPSFKey1, TPSFKey2>(
                    IPSFCreateProviderData<TRecordId, TProviderData> providerDataCreator,
                    PSF<TPSFKey1, TRecordId> psf1, TPSFKey1[] psfKeys1,
                    PSF<TPSFKey2, TRecordId> psf2, TPSFKey2[] psfKeys2,
                    Func<bool, bool, bool> matchPredicate,
                    PSFQuerySettings querySettings)
        {
            // TODO: Similar range-query/PQ implementation (and first-element only execution) as discussed above.
            return QueryPSF(providerDataCreator, psf1, psfKeys1[0], psf2, psfKeys2[0], matchPredicate, querySettings);
        }

        // Power user versions. We could add up to 3. Anything more complicated than
        // that, they can just post-process with LINQ.

        internal IEnumerable<TProviderData> QueryPSF<TPSFKey>(
                    IPSFCreateProviderData<TRecordId, TProviderData> providerDataCreator,
                    (PSF<TPSFKey, TRecordId> psf1, TPSFKey[])[] psfsAndKeys,
                    Func<bool[], bool> matchPredicate,
                    PSFQuerySettings querySettings)
        {
            // TODO: Not implemented. The input argument to the predicate is the matches to each
            // element of psfsAndKeys.
            return Array.Empty<TProviderData>();
        }

        internal IEnumerable<TProviderData> QueryPSF<TPSFKey1, TPSFKey2>(
                    IPSFCreateProviderData<TRecordId, TProviderData> providerDataCreator,
                    (PSF<TPSFKey1, TRecordId> psf1, TPSFKey1[])[] psfsAndKeys1,
                    (PSF<TPSFKey2, TRecordId> psf2, TPSFKey2[])[] psfsAndKeys2,
                    Func<bool[], bool[], bool> matchPredicate,
                    PSFQuerySettings querySettings)
        {
            // TODO: Not implemented. The first input argument to the predicate is the matches to each
            // element of psfsAndKeys1; the second input argument to the predicate is the matches to each
            // element of psfsAndKeys2.
            return Array.Empty<TProviderData>();
        }
    }
}
