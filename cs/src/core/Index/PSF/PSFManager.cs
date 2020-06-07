// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace FASTER.core
{
    internal class PSFManager<TProviderData, TRecordId> where TRecordId : struct, IComparable<TRecordId>
    {
        private readonly ConcurrentDictionary<long, IExecutePSF<TProviderData, TRecordId>> psfGroups 
            = new ConcurrentDictionary<long, IExecutePSF<TProviderData, TRecordId>>();

        private readonly ConcurrentDictionary<string, Guid> psfNames = new ConcurrentDictionary<string, Guid>();

        internal bool HasPSFs => this.psfGroups.Count > 0;

        internal Status Upsert(TProviderData data, TRecordId recordId,
                               PSFChangeTracker<TProviderData, TRecordId> changeTracker)
        {
            // TODO: RecordId locking, to ensure consistency of multiple PSFs if the same record is updated
            // multiple times; possibly a single Array<CacheLine>[N] which is locked on TRecordId.GetHashCode % N.

            // This Upsert was an Insert: For the FasterKV Insert fast path, changeTracker is null.
            if (changeTracker is null || changeTracker.UpdateOp == UpdateOperation.Insert)
            {
                foreach (var group in this.psfGroups.Values)
                {
                    // Fast Insert path: No IPUCache lookup is done for Inserts, so this is called directly here.
                    var status = group.ExecuteAndStore(data, recordId, PSFExecutePhase.Insert, changeTracker);
                    if (status != Status.OK)
                    {
                        // TODO handle errors
                    }
                }
                return Status.OK;
            }

            // This Upsert was an IPU or RCU
            return this.Update(changeTracker);
        }

        internal Status Update(PSFChangeTracker<TProviderData, TRecordId> changeTracker)
        {
            // TODO: same comment as Insert re: Context threading control for the primaryKV
            changeTracker.PrepareGroups(this.psfGroups.Count);
            foreach (var group in this.psfGroups.Values)
            {
                var status = group.Update(changeTracker);
                if (status != Status.OK)
                {
                    // TODO handle errors
                }
            }
            return Status.OK;
        }

        internal Status Delete(PSFChangeTracker<TProviderData, TRecordId> changeTracker)
        {
            // TODO: same comment as Insert re: Context threading control for the primaryKV
            changeTracker.PrepareGroups(this.psfGroups.Count);
            foreach (var group in this.psfGroups.Values)
            {
                var status = group.Delete(changeTracker);
                if (status != Status.OK)
                {
                    // TODO handle errors
                }
            }
            return Status.OK;
        }

        internal string[][] GetRegisteredPSFs() => throw new NotImplementedException("TODO");

        internal PSFChangeTracker<TProviderData, TRecordId> CreateChangeTracker() 
            => new PSFChangeTracker<TProviderData, TRecordId>();

        private static long NextGroupId = 0;

        private void AddGroup<TPSFKey>(PSFGroup<TProviderData, TPSFKey, TRecordId> group) where TPSFKey : struct
        {
            var gId = Interlocked.Increment(ref NextGroupId);
            this.psfGroups.TryAdd(gId - 1, group);
        }

        private void VerifyIsBlittable<TPSFKey>()
        {
            if (!Utility.IsBlittable<TPSFKey>())
                throw new PSFArgumentException("The PSF Key type must be blittable.");
        }

        private void VerifyIsOurPSF<TPSFKey>(PSF<TPSFKey, TRecordId> psf)
        {
            if (!this.psfNames.TryGetValue(psf.Name, out Guid id) || id != psf.Id)
                throw new PSFArgumentException($"The PSF {psf.Name} is not registered with this FasterKV.");
        }

        internal PSF<TPSFKey, TRecordId> RegisterPSF<TPSFKey>(IPSFDefinition<TProviderData, TPSFKey> def,
                                                              PSFRegistrationSettings<TPSFKey> registrationSettings)
            where TPSFKey : struct
        {
            this.VerifyIsBlittable<TPSFKey>();
            if (def is null)
                throw new PSFArgumentException("PSF definition cannot be null");

            // This is a very rare operation and unlikely to have any contention, and locking the dictionary
            // makes it much easier to recover from duplicates if needed.
            lock (this.psfNames)
            {
                if (psfNames.ContainsKey(def.Name))
                    throw new PSFArgumentException($"A PSF named {def.Name} is already registered in another group");
                var group = new PSFGroup<TProviderData, TPSFKey, TRecordId>(this.psfGroups.Count, new[] { def }, registrationSettings);
                AddGroup(group);
                var psf = group[def.Name];
                this.psfNames.TryAdd(psf.Name, psf.Id);
                return psf;
            }
        }

        internal PSF<TPSFKey, TRecordId>[] RegisterPSF<TPSFKey>(IPSFDefinition<TProviderData, TPSFKey>[] defs,
                                                              PSFRegistrationSettings<TPSFKey> registrationSettings)
            where TPSFKey : struct
        {
            this.VerifyIsBlittable<TPSFKey>();
            if (defs is null || defs.Length == 0 || defs.Any(def => def is null) || defs.Length == 0)
                throw new PSFArgumentException("PSF definitions cannot be null");

            // For PSFs defined on a FasterKV instance we create intelligent defaults in regSettings.
            if (registrationSettings is null)
                throw new PSFArgumentException("PSFRegistrationSettings is required");
            if (registrationSettings.LogSettings is null)
                throw new PSFArgumentException("PSFRegistrationSettings.LogSettings is required");

            // This is a very rare operation and unlikely to have any contention, and locking the dictionary
            // makes it much easier to recover from duplicates if needed.
            lock (this.psfNames)
            {
                for (var ii = 0; ii < defs.Length; ++ii)
                {
                    var def = defs[ii];
                    if (psfNames.ContainsKey(def.Name))
                        throw new PSFArgumentException($"A PSF named {def.Name} is already registered in another group");
                    for (var jj = ii + 1; jj < defs.Length; ++jj)
                    {
                        if (defs[jj].Name == def.Name)
                            throw new PSFArgumentException($"The PSF name {def.Name} cannot be specfied twice");
                    }
                }

                var group = new PSFGroup<TProviderData, TPSFKey, TRecordId>(this.psfGroups.Count, defs, registrationSettings);
                AddGroup(group);
                foreach (var psf in group.PSFs)
                    this.psfNames.TryAdd(psf.Name, psf.Id);
                return group.PSFs;
            }
        }

        internal IEnumerable<TRecordId> QueryPSF<TPSFKey>(PSF<TPSFKey, TRecordId> psf,
                                                          TPSFKey psfKey, PSFQuerySettings querySettings)
            where TPSFKey : struct
        {
            this.VerifyIsOurPSF(psf);
            foreach (var recordId in psf.Query(psfKey))
            {
                if (querySettings != null && querySettings.CancellationToken.IsCancellationRequested)
                {
                    if (querySettings.ThrowOnCancellation)
                        querySettings.CancellationToken.ThrowIfCancellationRequested();
                    yield break;
                }
                yield return recordId;
            }
        }

        internal IEnumerable<TRecordId> QueryPSF<TPSFKey>(PSF<TPSFKey, TRecordId> psf,
                                                          TPSFKey[] psfKeys, PSFQuerySettings querySettings)
            where TPSFKey : struct
        {
            this.VerifyIsOurPSF(psf);

            // TODO implement range queries. This will start retrieval of a stream of returned values for the
            // chains for all specified keys for the PSF, returning them via an IEnumerable<TPSFKey> over a PQ
            // (PriorityQueue) that is populated from each key's stream. This is how multiple range query bins
            // are handled; the semantics are that a Union (via stream merge) of all records for all keys in the
            // array is done. Obviously there will be a tradeoff between the granularity of the bins and the
            // overhead of the PQ for the streams returned.
            return QueryPSF(psf, psfKeys[0], querySettings);   // TODO just to make the compiler happy
        }

        internal IEnumerable<TRecordId> QueryPSF<TPSFKey1, TPSFKey2>(
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

            var group1 = this.psfGroups[psf1.GroupId];
            var group2 = this.psfGroups[psf2.GroupId];

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
                    yield return cmp < 0 ? e1.Current : e2.Current;

                // Let the trailing one catch up
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
                    yield return !e1done ? e1.Current : e2.Current;
                if (!(!e1done ? e1 : e2).MoveNext())
                    yield break;
            }
        }

        internal IEnumerable<TRecordId> QueryPSF<TPSFKey1, TPSFKey2>(
                    PSF<TPSFKey1, TRecordId> psf1, TPSFKey1[] psfKeys1,
                    PSF<TPSFKey2, TRecordId> psf2, TPSFKey2[] psfKeys2,
                    Func<bool, bool, bool> matchPredicate,
                    PSFQuerySettings querySettings)
        {
            // TODO: Similar range-query/PQ implementation (and first-element only execution) as discussed above.
            return QueryPSF(psf1, psfKeys1[0], psf2, psfKeys2[0], matchPredicate, querySettings);
        }

        // Power user versions. We could add up to 3. Anything more complicated than
        // that, they can just post-process with LINQ.

        internal IEnumerable<TRecordId> QueryPSF<TPSFKey>(
                    (PSF<TPSFKey, TRecordId> psf1, TPSFKey[])[] psfsAndKeys,
                    Func<bool[], bool> matchPredicate,
                    PSFQuerySettings querySettings)
        {
            // TODO: Not implemented. The input argument to the predicate is the matches to each
            // element of psfsAndKeys.
            return Array.Empty<TRecordId>();
        }

        internal IEnumerable<TRecordId> QueryPSF<TPSFKey1, TPSFKey2>(
                    (PSF<TPSFKey1, TRecordId> psf1, TPSFKey1[])[] psfsAndKeys1,
                    (PSF<TPSFKey2, TRecordId> psf2, TPSFKey2[])[] psfsAndKeys2,
                    Func<bool[], bool[], bool> matchPredicate,
                    PSFQuerySettings querySettings)
        {
            // TODO: Not implemented. The first input argument to the predicate is the matches to each
            // element of psfsAndKeys1; the second input argument to the predicate is the matches to each
            // element of psfsAndKeys2.
            return Array.Empty<TRecordId>();
        }
    }
}
