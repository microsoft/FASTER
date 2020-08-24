// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core.Index.PSF;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

// TODO: Remove PackageId and PackageOutputPath from csproj when this is folded into master
// TODO: Make a new FASTER.PSF.dll

namespace FASTER.core
{
    internal class PSFManager<TProviderData, TRecordId> where TRecordId : struct, IComparable<TRecordId>
    {
        private readonly ConcurrentDictionary<long, IExecutePSF<TProviderData, TRecordId>> psfGroups 
            = new ConcurrentDictionary<long, IExecutePSF<TProviderData, TRecordId>>();

        private readonly ConcurrentDictionary<string, Guid> psfNames = new ConcurrentDictionary<string, Guid>();

        internal bool HasPSFs => this.psfGroups.Count > 0;

        internal Status Upsert(TProviderData data, TRecordId recordId, PSFChangeTracker<TProviderData, TRecordId> changeTracker)
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
                        // TODOerr: handle errors
                    }
                }
                return Status.OK;
            }

            // This Upsert was an IPU or RCU
            return this.Update(changeTracker);
        }

        internal Status Update(PSFChangeTracker<TProviderData, TRecordId> changeTracker)
        {
            foreach (var group in this.psfGroups.Values)
            {
                var status = group.Update(changeTracker);
                if (status != Status.OK)
                {
                    // TODOerr: handle errors
                }
            }
            return Status.OK;
        }

        internal Status Delete(PSFChangeTracker<TProviderData, TRecordId> changeTracker)
        {
            foreach (var group in this.psfGroups.Values)
            {
                var status = group.Delete(changeTracker);
                if (status != Status.OK)
                {
                    // TODOerr: handle errors
                }
            }
            return Status.OK;
        }

        internal string[][] GetRegisteredPSFNames() => throw new NotImplementedException("TODO");

        internal PSFChangeTracker<TProviderData, TRecordId> CreateChangeTracker() 
            => new PSFChangeTracker<TProviderData, TRecordId>(this.psfGroups.Values.Select(group => group.Id));

        public Status SetBeforeData(PSFChangeTracker<TProviderData, TRecordId> changeTracker, TProviderData data, TRecordId recordId, bool executePSFsNow)
        {
            changeTracker.SetBeforeData(data, recordId);
            if (executePSFsNow)
            {
                foreach (var group in this.psfGroups.Values)
                {
                    var status = group.GetBeforeKeys(changeTracker);
                    if (status != Status.OK)
                    {
                        // TODOerr: handle errors
                    }
                }
                changeTracker.HasBeforeKeys = true;
            }
            return Status.OK;
        }

        public Status SetAfterData(PSFChangeTracker<TProviderData, TRecordId> changeTracker, TProviderData data, TRecordId recordId)
        {
            changeTracker.SetAfterData(data, recordId);
            return Status.OK;
        }

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

        private PSF<TPSFKey, TRecordId> GetImplementingPSF<TPSFKey>(IPSF ipsf)
        {
            if (ipsf is null)
                throw new PSFArgumentException($"The PSF cannot be null.");
            var psf = ipsf as PSF<TPSFKey, TRecordId>;
            Guid id = default;
            if (psf is null || !this.psfNames.TryGetValue(psf.Name, out id) || id != psf.Id)
                throw new PSFArgumentException($"The PSF {psf.Name} with Id {(psf is null ? "(unavailable)" : id.ToString())} is not registered with this FasterKV.");
            return psf;
        }

        private void VerifyIsOurPSF(params IPSF[] psfs)
        {
            foreach (var psf in psfs)
            {
                if (psf is null)
                    throw new PSFArgumentException($"The PSF cannot be null.");
                if (!this.psfNames.ContainsKey(psf.Name))
                    throw new PSFArgumentException($"The PSF {psf.Name} is not registered with this FasterKV.");
            }
        }

        private void VerifyIsOurPSF<TPSFKey>(IEnumerable<(IPSF, IEnumerable<TPSFKey>)> psfsAndKeys)
        {
            if (psfsAndKeys is null)
                throw new PSFArgumentException($"The PSF enumerable cannot be null.");
            foreach (var psfAndKeys in psfsAndKeys)
                this.VerifyIsOurPSF(psfAndKeys.Item1);
        }

        private void VerifyIsOurPSF<TPSFKey1, TPSFKey2>(IEnumerable<(IPSF, IEnumerable<TPSFKey1>)> psfsAndKeys1,
                                                        IEnumerable<(IPSF, IEnumerable<TPSFKey2>)> psfsAndKeys2)
        {
            VerifyIsOurPSF(psfsAndKeys1);
            VerifyIsOurPSF(psfsAndKeys2);
        }

        private void VerifyIsOurPSF<TPSFKey1, TPSFKey2, TPSFKey3>(IEnumerable<(IPSF, IEnumerable<TPSFKey1>)> psfsAndKeys1,
                                                        IEnumerable<(IPSF, IEnumerable<TPSFKey2>)> psfsAndKeys2,
                                                        IEnumerable<(IPSF, IEnumerable<TPSFKey3>)> psfsAndKeys3)
        {
            VerifyIsOurPSF(psfsAndKeys1);
            VerifyIsOurPSF(psfsAndKeys2);
            VerifyIsOurPSF(psfsAndKeys3);
        }

        private static void VerifyRegistrationSettings<TPSFKey>(PSFRegistrationSettings<TPSFKey> registrationSettings) where TPSFKey : struct
        {
            if (registrationSettings is null)
                throw new PSFArgumentException("PSFRegistrationSettings is required");
            if (registrationSettings.LogSettings is null)
                throw new PSFArgumentException("PSFRegistrationSettings.LogSettings is required");
            if (registrationSettings.CheckpointSettings is null)
                throw new PSFArgumentException("PSFRegistrationSettings.CheckpointSettings is required");

            // TODOdcr: Support ReadCache and CopyReadsToTail for PSFs
            if (!(registrationSettings.LogSettings.ReadCacheSettings is null) || registrationSettings.LogSettings.CopyReadsToTail)
                throw new PSFArgumentException("PSFs do not support ReadCache or CopyReadsToTail");
        }

        internal IPSF RegisterPSF<TPSFKey>(PSFRegistrationSettings<TPSFKey> registrationSettings, IPSFDefinition<TProviderData, TPSFKey> def)
            where TPSFKey : struct
        {
            this.VerifyIsBlittable<TPSFKey>();
            VerifyRegistrationSettings(registrationSettings);
            if (def is null)
                throw new PSFArgumentException("PSF definition cannot be null");

            // This is a very rare operation and unlikely to have any contention, and locking the dictionary
            // makes it much easier to recover from duplicates if needed.
            lock (this.psfNames)
            {
                if (psfNames.ContainsKey(def.Name))
                    throw new PSFArgumentException($"A PSF named {def.Name} is already registered in another group");
                var group = new PSFGroup<TProviderData, TPSFKey, TRecordId>(registrationSettings, new[] { def }, this.psfGroups.Count);
                AddGroup(group);
                var psf = group[def.Name];
                this.psfNames.TryAdd(psf.Name, psf.Id);
                return psf;
            }
        }

        internal IPSF[] RegisterPSF<TPSFKey>(PSFRegistrationSettings<TPSFKey> registrationSettings, IPSFDefinition<TProviderData, TPSFKey>[] defs)
            where TPSFKey : struct
        {
            this.VerifyIsBlittable<TPSFKey>();
            VerifyRegistrationSettings(registrationSettings);
            if (defs is null || defs.Length == 0 || defs.Any(def => def is null) || defs.Length == 0)
                throw new PSFArgumentException("PSF definitions cannot be null or empty");

            // We use stackalloc for speed and can recurse in pending operations, so make sure we don't blow the stack.
            if (defs.Length > Constants.kInvalidPsfOrdinal)
                throw new PSFArgumentException($"There can be no more than {Constants.kInvalidPsfOrdinal} PSFs in a single Group");
            const int maxKeySize = 256;
            if (Utility.GetSize(default(KeyPointer<TPSFKey>)) > maxKeySize)
                throw new PSFArgumentException($"The size of the PSF key can be no more than {maxKeySize} bytes");

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

                var group = new PSFGroup<TProviderData, TPSFKey, TRecordId>(registrationSettings, defs, this.psfGroups.Count);
                AddGroup(group);
                foreach (var psf in group.PSFs)
                    this.psfNames.TryAdd(psf.Name, psf.Id);
                return group.PSFs;
            }
        }

        internal IEnumerable<TRecordId> QueryPSF<TPSFKey>(IPSF psf, TPSFKey key, PSFQuerySettings querySettings)
            where TPSFKey : struct
        {
            var psfImpl = this.GetImplementingPSF<TPSFKey>(psf);
            querySettings ??= PSFQuerySettings.Default;
            foreach (var recordId in psfImpl.Query(key, querySettings))
            {
                if (querySettings.IsCanceled)
                    yield break;
                yield return recordId;
            }
        }

#if DOTNETCORE
        internal async IAsyncEnumerable<TRecordId> QueryPSFAsync<TPSFKey>(IPSF psf, TPSFKey key, PSFQuerySettings querySettings)
            where TPSFKey : struct
        {
            var psfImpl = this.GetImplementingPSF<TPSFKey>(psf);
            querySettings ??= PSFQuerySettings.Default;
            await foreach (var recordId in psfImpl.QueryAsync(key, querySettings))
            {
                if (querySettings.IsCanceled)
                    yield break;
                yield return recordId;
            }
        }

#endif // DOTNETCORE

        internal IEnumerable<TRecordId> QueryPSF<TPSFKey>(IPSF psf, IEnumerable<TPSFKey> keys, PSFQuerySettings querySettings)
            where TPSFKey : struct
        {
            this.VerifyIsOurPSF(psf);
            querySettings ??= PSFQuerySettings.Default;

            // The recordIds cannot overlap between keys (unless something's gone wrong), so return them all.
            // TODOperf: Consider a PQ ordered on secondary FKV LA so we can walk through in parallel (and in memory sequence) in one PsfRead(Key|Address) loop.
            foreach (var key in keys)
            {
                foreach (var recordId in QueryPSF(psf, key, querySettings))
                {
                    if (querySettings.IsCanceled)
                        yield break;
                    yield return recordId;
                }
            }
        }

#if DOTNETCORE
        internal async IAsyncEnumerable<TRecordId> QueryPSFAsync<TPSFKey>(IPSF psf, IEnumerable<TPSFKey> keys, PSFQuerySettings querySettings)
            where TPSFKey : struct
        {
            this.VerifyIsOurPSF(psf);
            querySettings ??= PSFQuerySettings.Default;

            // The recordIds cannot overlap between keys (unless something's gone wrong), so return them all.
            // TODOperf: Consider a PQ ordered on secondary FKV LA so we can walk through in parallel (and in memory sequence) in one PsfRead(Key|Address) loop.
            foreach (var key in keys)
            {
                await foreach (var recordId in QueryPSFAsync(psf, key, querySettings))
                {
                    if (querySettings.IsCanceled)
                        yield break;
                    yield return recordId;
                }
            }
        }

#endif // DOTNETCORE

        internal IEnumerable<TRecordId> QueryPSF<TPSFKey1, TPSFKey2>(
                     IPSF psf1, TPSFKey1 key1,
                     IPSF psf2, TPSFKey2 key2,
                    Func<bool, bool, bool> matchPredicate,
                    PSFQuerySettings querySettings)
            where TPSFKey1 : struct
            where TPSFKey2 : struct
        {
            this.VerifyIsOurPSF(psf1, psf2);
            querySettings ??= PSFQuerySettings.Default;

            return new QueryRecordIterator<TRecordId>(psf1, this.QueryPSF(psf1, key1, querySettings), psf2, this.QueryPSF(psf2, key2, querySettings),
                                                      matchIndicators => matchPredicate(matchIndicators[0][0], matchIndicators[1][0]), querySettings).Run();
        }

#if DOTNETCORE
        internal IAsyncEnumerable<TRecordId> QueryPSFAsync<TPSFKey1, TPSFKey2>(
                     IPSF psf1, TPSFKey1 key1,
                     IPSF psf2, TPSFKey2 key2,
                    Func<bool, bool, bool> matchPredicate,
                    PSFQuerySettings querySettings)
            where TPSFKey1 : struct
            where TPSFKey2 : struct
        {
            this.VerifyIsOurPSF(psf1, psf2);
            querySettings ??= PSFQuerySettings.Default;

            return new AsyncQueryRecordIterator<TRecordId>(psf1, this.QueryPSFAsync(psf1, key1, querySettings), psf2, this.QueryPSFAsync(psf2, key2, querySettings),
                                                           matchIndicators => matchPredicate(matchIndicators[0][0], matchIndicators[1][0]), querySettings).Run();
        }

#endif // DOTNETCORE

        internal IEnumerable<TRecordId> QueryPSF<TPSFKey1, TPSFKey2>(
                     IPSF psf1, IEnumerable<TPSFKey1> keys1,
                     IPSF psf2, IEnumerable<TPSFKey2> keys2,
                    Func<bool, bool, bool> matchPredicate,
                    PSFQuerySettings querySettings)
            where TPSFKey1 : struct
            where TPSFKey2 : struct
        {
            this.VerifyIsOurPSF(psf1, psf2);
            querySettings ??= PSFQuerySettings.Default;

            return new QueryRecordIterator<TRecordId>(psf1, this.QueryPSF(psf1, keys1, querySettings), psf2, this.QueryPSF(psf2, keys2, querySettings),
                                                      matchIndicators => matchPredicate(matchIndicators[0][0], matchIndicators[1][0]), querySettings).Run();
        }

#if DOTNETCORE
        internal IAsyncEnumerable<TRecordId> QueryPSFAsync<TPSFKey1, TPSFKey2>(
                     IPSF psf1, IEnumerable<TPSFKey1> keys1,
                     IPSF psf2, IEnumerable<TPSFKey2> keys2,
                    Func<bool, bool, bool> matchPredicate,
                    PSFQuerySettings querySettings)
            where TPSFKey1 : struct
            where TPSFKey2 : struct
        {
            this.VerifyIsOurPSF(psf1, psf2);
            querySettings ??= PSFQuerySettings.Default;

            return new AsyncQueryRecordIterator<TRecordId>(psf1, this.QueryPSFAsync(psf1, keys1, querySettings), psf2, this.QueryPSFAsync(psf2, keys2, querySettings),
                                                      matchIndicators => matchPredicate(matchIndicators[0][0], matchIndicators[1][0]), querySettings).Run();
        }
#endif // DOTNETCORE

        public IEnumerable<TRecordId> QueryPSF<TPSFKey1, TPSFKey2, TPSFKey3>(
                     IPSF psf1, TPSFKey1 key1,
                     IPSF psf2, TPSFKey2 key2,
                     IPSF psf3, TPSFKey3 key3,
                    Func<bool, bool, bool, bool> matchPredicate,
                    PSFQuerySettings querySettings = null)
            where TPSFKey1 : struct
            where TPSFKey2 : struct
            where TPSFKey3 : struct
        {
            this.VerifyIsOurPSF(psf1, psf2, psf3);
            querySettings ??= PSFQuerySettings.Default;

            return new QueryRecordIterator<TRecordId>(psf1, this.QueryPSF(psf1, key1, querySettings), psf2, this.QueryPSF(psf2, key2, querySettings),
                                                      psf3, this.QueryPSF(psf3, key3, querySettings),
                                                      matchIndicators => matchPredicate(matchIndicators[0][0], matchIndicators[1][0], matchIndicators[2][0]), querySettings).Run();
        }

#if DOTNETCORE
        public IAsyncEnumerable<TRecordId> QueryPSFAsync<TPSFKey1, TPSFKey2, TPSFKey3>(
                     IPSF psf1, TPSFKey1 key1,
                     IPSF psf2, TPSFKey2 key2,
                     IPSF psf3, TPSFKey3 key3,
                    Func<bool, bool, bool, bool> matchPredicate,
                    PSFQuerySettings querySettings = null)
            where TPSFKey1 : struct
            where TPSFKey2 : struct
            where TPSFKey3 : struct
        {
            this.VerifyIsOurPSF(psf1, psf2, psf3);
            querySettings ??= PSFQuerySettings.Default;

            return new AsyncQueryRecordIterator<TRecordId>(psf1, this.QueryPSFAsync(psf1, key1, querySettings), psf2, this.QueryPSFAsync(psf2, key2, querySettings),
                                                      psf3, this.QueryPSFAsync(psf3, key3, querySettings),
                                                      matchIndicators => matchPredicate(matchIndicators[0][0], matchIndicators[1][0], matchIndicators[2][0]), querySettings).Run();
        }
#endif // DOTNETCORE

        public IEnumerable<TRecordId> QueryPSF<TPSFKey1, TPSFKey2, TPSFKey3>(
                     IPSF psf1, IEnumerable<TPSFKey1> keys1,
                     IPSF psf2, IEnumerable<TPSFKey2> keys2,
                     IPSF psf3, IEnumerable<TPSFKey3> keys3,
                    Func<bool, bool, bool, bool> matchPredicate,
                    PSFQuerySettings querySettings = null)
            where TPSFKey1 : struct
            where TPSFKey2 : struct
            where TPSFKey3 : struct
        {
            this.VerifyIsOurPSF(psf1, psf2, psf3);
            querySettings ??= PSFQuerySettings.Default;

            return new QueryRecordIterator<TRecordId>(psf1, this.QueryPSF(psf1, keys1, querySettings), psf2, this.QueryPSF(psf2, keys2, querySettings),
                                                      psf3, this.QueryPSF(psf3, keys3, querySettings),
                                                      matchIndicators => matchPredicate(matchIndicators[0][0], matchIndicators[1][0], matchIndicators[2][0]), querySettings).Run();
        }

#if DOTNETCORE
        public IAsyncEnumerable<TRecordId> QueryPSFAsync<TPSFKey1, TPSFKey2, TPSFKey3>(
                     IPSF psf1, IEnumerable<TPSFKey1> keys1,
                     IPSF psf2, IEnumerable<TPSFKey2> keys2,
                     IPSF psf3, IEnumerable<TPSFKey3> keys3,
                    Func<bool, bool, bool, bool> matchPredicate,
                    PSFQuerySettings querySettings = null)
            where TPSFKey1 : struct
            where TPSFKey2 : struct
            where TPSFKey3 : struct
        {
            this.VerifyIsOurPSF(psf1, psf2, psf3);
            querySettings ??= PSFQuerySettings.Default;

            return new AsyncQueryRecordIterator<TRecordId>(psf1, this.QueryPSFAsync(psf1, keys1, querySettings), psf2, this.QueryPSFAsync(psf2, keys2, querySettings),
                                                      psf3, this.QueryPSFAsync(psf3, keys3, querySettings),
                                                      matchIndicators => matchPredicate(matchIndicators[0][0], matchIndicators[1][0], matchIndicators[2][0]), querySettings).Run();
        }
#endif // DOTNETCORE

        // Power user versions. Anything more complicated than this can be post-processed with LINQ.

        internal IEnumerable<TRecordId> QueryPSF<TPSFKey>(
                    IEnumerable<(IPSF psf, IEnumerable<TPSFKey> keys)> psfsAndKeys,
                    Func<bool[], bool> matchPredicate,
                    PSFQuerySettings querySettings = null)
            where TPSFKey : struct
        {
            this.VerifyIsOurPSF(psfsAndKeys);
            querySettings ??= PSFQuerySettings.Default;

            return new QueryRecordIterator<TRecordId>(new[] { psfsAndKeys.Select(tup => ((IPSF)tup.psf, this.QueryPSF(tup.psf, tup.keys, querySettings))) },
                                                      matchIndicators => matchPredicate(matchIndicators[0]), querySettings).Run();
        }

#if DOTNETCORE
        internal IAsyncEnumerable<TRecordId> QueryPSFAsync<TPSFKey>(
                    IEnumerable<(IPSF psf, IEnumerable<TPSFKey> keys)> psfsAndKeys,
                    Func<bool[], bool> matchPredicate,
                    PSFQuerySettings querySettings = null)
            where TPSFKey : struct
        {
            this.VerifyIsOurPSF(psfsAndKeys);
            querySettings ??= PSFQuerySettings.Default;

            return new AsyncQueryRecordIterator<TRecordId>(new[] { psfsAndKeys.Select(tup => ((IPSF)tup.psf, this.QueryPSFAsync(tup.psf, tup.keys, querySettings))) },
                                                      matchIndicators => matchPredicate(matchIndicators[0]), querySettings).Run();
        }
#endif // DOTNETCORE

        internal IEnumerable<TRecordId> QueryPSF<TPSFKey1, TPSFKey2>(
                    IEnumerable<(IPSF psf, IEnumerable<TPSFKey1> keys)> psfsAndKeys1,
                    IEnumerable<(IPSF psf, IEnumerable<TPSFKey2> keys)> psfsAndKeys2,
                    Func<bool[], bool[], bool> matchPredicate,
                    PSFQuerySettings querySettings = null)
            where TPSFKey1 : struct
            where TPSFKey2 : struct
        {
            this.VerifyIsOurPSF(psfsAndKeys1, psfsAndKeys2);
            querySettings ??= PSFQuerySettings.Default;

            return new QueryRecordIterator<TRecordId>(new[] {psfsAndKeys1.Select(tup => ((IPSF)tup.psf, this.QueryPSF(tup.psf, tup.keys, querySettings))),
                                                             psfsAndKeys2.Select(tup => ((IPSF)tup.psf, this.QueryPSF(tup.psf, tup.keys, querySettings)))},
                                                      matchIndicators => matchPredicate(matchIndicators[0], matchIndicators[1]), querySettings).Run();
        }

#if DOTNETCORE
        internal IAsyncEnumerable<TRecordId> QueryPSFAsync<TPSFKey1, TPSFKey2>(
                    IEnumerable<(IPSF psf, IEnumerable<TPSFKey1> keys)> psfsAndKeys1,
                    IEnumerable<(IPSF psf, IEnumerable<TPSFKey2> keys)> psfsAndKeys2,
                    Func<bool[], bool[], bool> matchPredicate,
                    PSFQuerySettings querySettings = null)
            where TPSFKey1 : struct
            where TPSFKey2 : struct
        {
            this.VerifyIsOurPSF(psfsAndKeys1, psfsAndKeys2);
            querySettings ??= PSFQuerySettings.Default;

            return new AsyncQueryRecordIterator<TRecordId>(new[] {psfsAndKeys1.Select(tup => ((IPSF)tup.psf, this.QueryPSFAsync(tup.psf, tup.keys, querySettings))),
                                                             psfsAndKeys2.Select(tup => ((IPSF)tup.psf, this.QueryPSFAsync(tup.psf, tup.keys, querySettings)))},
                                                      matchIndicators => matchPredicate(matchIndicators[0], matchIndicators[1]), querySettings).Run();
        }
#endif // DOTNETCORE

        internal IEnumerable<TRecordId> QueryPSF<TPSFKey1, TPSFKey2, TPSFKey3>(
                    IEnumerable<(IPSF psf, IEnumerable<TPSFKey1> keys)> psfsAndKeys1,
                    IEnumerable<(IPSF psf, IEnumerable<TPSFKey2> keys)> psfsAndKeys2,
                    IEnumerable<(IPSF psf, IEnumerable<TPSFKey3> keys)> psfsAndKeys3,
                    Func<bool[], bool[], bool[], bool> matchPredicate,
                    PSFQuerySettings querySettings = null)
            where TPSFKey1 : struct
            where TPSFKey2 : struct
            where TPSFKey3 : struct
        {
            this.VerifyIsOurPSF(psfsAndKeys1, psfsAndKeys2, psfsAndKeys3);
            querySettings ??= PSFQuerySettings.Default;

            return new QueryRecordIterator<TRecordId>(new[] {psfsAndKeys1.Select(tup => ((IPSF)tup.psf, this.QueryPSF(tup.psf, tup.keys, querySettings))),
                                                             psfsAndKeys2.Select(tup => ((IPSF)tup.psf, this.QueryPSF(tup.psf, tup.keys, querySettings))),
                                                             psfsAndKeys3.Select(tup => ((IPSF)tup.psf, this.QueryPSF(tup.psf, tup.keys, querySettings)))},
                                                      matchIndicators => matchPredicate(matchIndicators[0], matchIndicators[1], matchIndicators[2]), querySettings).Run();
        }

#if DOTNETCORE
        internal IAsyncEnumerable<TRecordId> QueryPSFAsync<TPSFKey1, TPSFKey2, TPSFKey3>(
                    IEnumerable<(IPSF psf, IEnumerable<TPSFKey1> keys)> psfsAndKeys1,
                    IEnumerable<(IPSF psf, IEnumerable<TPSFKey2> keys)> psfsAndKeys2,
                    IEnumerable<(IPSF psf, IEnumerable<TPSFKey3> keys)> psfsAndKeys3,
                    Func<bool[], bool[], bool[], bool> matchPredicate,
                    PSFQuerySettings querySettings = null)
            where TPSFKey1 : struct
            where TPSFKey2 : struct
            where TPSFKey3 : struct
        {
            this.VerifyIsOurPSF(psfsAndKeys1, psfsAndKeys2, psfsAndKeys3);
            querySettings ??= PSFQuerySettings.Default;

            return new AsyncQueryRecordIterator<TRecordId>(new[] {psfsAndKeys1.Select(tup => ((IPSF)tup.psf, this.QueryPSFAsync(tup.psf, tup.keys, querySettings))),
                                                             psfsAndKeys2.Select(tup => ((IPSF)tup.psf, this.QueryPSFAsync(tup.psf, tup.keys, querySettings))),
                                                             psfsAndKeys3.Select(tup => ((IPSF)tup.psf, this.QueryPSFAsync(tup.psf, tup.keys, querySettings)))},
                                                      matchIndicators => matchPredicate(matchIndicators[0], matchIndicators[1], matchIndicators[2]), querySettings).Run();
        }
#endif // DOTNETCORE

        #region Checkpoint Operations
        // TODO Separate Tasks for each group's commit/restore operations?
        public bool TakeFullCheckpoint()
            => this.psfGroups.Values.Aggregate(true, (result, group) => group.TakeFullCheckpoint() && result);

        public Task CompleteCheckpointAsync(CancellationToken token = default)
        {
            var tasks = this.psfGroups.Values.Select(group => group.CompleteCheckpointAsync(token).AsTask()).ToArray();
            return Task.WhenAll(tasks);
        }

        public bool TakeIndexCheckpoint()
            => this.psfGroups.Values.Aggregate(true, (result, group) => group.TakeIndexCheckpoint() && result);

        public bool TakeHybridLogCheckpoint() 
            => this.psfGroups.Values.Aggregate(true, (result, group) => group.TakeHybridLogCheckpoint() && result);

        public void Recover()
        {
            foreach (var group in this.psfGroups.Values)
                group.Recover();
        }
        #endregion Checkpoint Operations

        #region Log Operations

        public void FlushLogs(bool wait)
        {
            foreach (var group in this.psfGroups.Values)
                group.FlushLog(wait);
        }

        /// <summary>
        /// Flush log and evict all records from memory
        /// </summary>
        /// <param name="wait">Synchronous wait for operation to complete</param>
        /// <returns>When wait is false, this tells whether the full eviction was successfully registered with FASTER</returns>
        public bool FlushAndEvictLogs(bool wait)
        {
            foreach (var group in this.psfGroups.Values)
            {
                if (!group.FlushAndEvictLog(wait))
                {
                    // TODO handle error on FlushAndEvictLogs
                }
            }
            return true;
        }

        /// <summary>
        /// Delete log entirely from memory. Cannot allocate on the log
        /// after this point. This is a synchronous operation.
        /// </summary>
        public void DisposeLogsFromMemory()
        {
            foreach (var group in this.psfGroups.Values)
                group.DisposeLogFromMemory();
        }
        #endregion Log Operations
    }
}
