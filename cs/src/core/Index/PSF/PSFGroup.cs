// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core.Index.PSF;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace FASTER.core
{
    /// <summary>
    /// A group of <see cref="PSF{TPSFKey, TRecordId}"/>s. Ideally, most records in the group will either match all
    /// PSFs or none, for efficient use of log space.
    /// </summary>
    /// <typeparam name="TProviderData">The type of the wrapper for the provider's data (obtained from TRecordId)</typeparam>
    /// <typeparam name="TPSFKey">The type of the key returned by the Predicate and stored in the secondary FasterKV instance</typeparam>
    /// <typeparam name="TRecordId">The type of data record supplied by the data provider; in FasterKV it 
    ///     is the logicalAddress of the record in the primary FasterKV instance.</typeparam>
    public class PSFGroup<TProviderData, TPSFKey, TRecordId> : IExecutePSF<TProviderData, TRecordId>,
                                                               IQueryPSF<TPSFKey, TRecordId>
        where TPSFKey : struct
        where TRecordId : struct
    {
        internal FasterKV<TPSFKey, TRecordId> fht;
        private readonly PSFFunctions<TPSFKey, TRecordId> functions;
        internal IPSFDefinition<TProviderData, TPSFKey>[] psfDefinitions;
        private readonly PSFRegistrationSettings<TPSFKey> regSettings;

        /// <summary>
        /// ID of the group (used internally only)
        /// </summary>
        public long Id { get; }

        private readonly CheckpointSettings checkpointSettings;
        private readonly int keyPointerSize = Utility.GetSize(default(KeyPointer<TPSFKey>));
        private readonly int recordIdSize = (Utility.GetSize(default(TRecordId)) + sizeof(long) - 1) & ~(sizeof(long) - 1);

        internal ConcurrentStack<ClientSession<TPSFKey,
                        TRecordId, PSFInputSecondary<TPSFKey>,
                        PSFOutputSecondary<TPSFKey, TRecordId>, Empty, PSFFunctions<TPSFKey, TRecordId>>> freeSessions
            = new ConcurrentStack<ClientSession<TPSFKey,
                        TRecordId, PSFInputSecondary<TPSFKey>,
                        PSFOutputSecondary<TPSFKey, TRecordId>, Empty, PSFFunctions<TPSFKey, TRecordId>>>();
        internal ConcurrentBag<ClientSession<TPSFKey,
                        TRecordId, PSFInputSecondary<TPSFKey>,
                        PSFOutputSecondary<TPSFKey, TRecordId>, Empty, PSFFunctions<TPSFKey, TRecordId>>> allSessions
            = new ConcurrentBag<ClientSession<TPSFKey,
                        TRecordId, PSFInputSecondary<TPSFKey>,
                        PSFOutputSecondary<TPSFKey, TRecordId>, Empty, PSFFunctions<TPSFKey, TRecordId>>>();

        /// <summary>
        /// The list of <see cref="PSF{TPSFKey, TRecordId}"/>s in this group
        /// </summary>
        public PSF<TPSFKey, TRecordId>[] PSFs { get; private set; }

        private int PSFCount => this.PSFs.Length;

        private readonly IFasterEqualityComparer<TPSFKey> userKeyComparer;
        private readonly KeyAccessor<TPSFKey> keyAccessor;

        private readonly SectorAlignedBufferPool bufferPool;

        // Override equivalence testing for set membership

        /// <inheritdoc/>
        public override int GetHashCode() => this.Id.GetHashCode();

        /// <inheritdoc/>
        public override bool Equals(object obj) => this.Equals(obj as PSFGroup<TProviderData, TPSFKey, TRecordId>);

        /// <inheritdoc/>
        public bool Equals(PSFGroup<TProviderData, TPSFKey, TRecordId> other) => !(other is null) && this.Id == other.Id;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="regSettings">Optional registration settings</param>
        /// <param name="defs">PSF definitions</param>
        /// <param name="id">The ordinal of this PSFGroup in the <see cref="PSFManager{TProviderData, TRecordId}"/>'s
        /// PSFGroup list.</param>
        public PSFGroup(PSFRegistrationSettings<TPSFKey> regSettings, IPSFDefinition<TProviderData, TPSFKey>[] defs, long id)
        {
            this.psfDefinitions = defs;
            this.Id = id;
            this.regSettings = regSettings;
            this.userKeyComparer = GetUserKeyComparer();

            this.PSFs = defs.Select((def, ord) => new PSF<TPSFKey, TRecordId>(this.Id, ord, def.Name, this)).ToArray();
            this.keyAccessor = new KeyAccessor<TPSFKey>(this.userKeyComparer, this.PSFCount, this.keyPointerSize);

            this.checkpointSettings = regSettings?.CheckpointSettings;
            this.functions = new PSFFunctions<TPSFKey, TRecordId>();
            this.fht = new FasterKV<TPSFKey, TRecordId>(
                    regSettings.HashTableSize, regSettings.LogSettings, this.checkpointSettings, null /*SerializerSettings*/,
                    new CompositeKey<TPSFKey>.UnusedKeyComparer(),
                    new VariableLengthStructSettings<TPSFKey, TRecordId>
                    {
                        keyLength = new CompositeKey<TPSFKey>.VarLenLength(this.keyPointerSize, this.PSFCount)
                    }
                );
            this.fht.hlog.PsfKeyAccessor = keyAccessor;

            this.bufferPool = this.fht.hlog.bufferPool;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private ClientSession<TPSFKey, TRecordId, PSFInputSecondary<TPSFKey>,
                          PSFOutputSecondary<TPSFKey, TRecordId>, Empty,
                          PSFFunctions<TPSFKey, TRecordId>> GetSession()
        {
            // Sessions are used only on post-RegisterPSF actions (Upsert, RMW, Query).
            if (this.freeSessions.TryPop(out var session))
                return session;
            session = this.fht.NewSession<PSFInputSecondary<TPSFKey>, PSFOutputSecondary<TPSFKey, TRecordId>, Empty, PSFFunctions<TPSFKey, TRecordId>>(
                                        new PSFFunctions<TPSFKey, TRecordId>(), threadAffinitized: this.regSettings.ThreadAffinitized);
            this.allSessions.Add(session);
            return session;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ReleaseSession(ClientSession<TPSFKey, TRecordId, PSFInputSecondary<TPSFKey>,
                          PSFOutputSecondary<TPSFKey, TRecordId>, Empty,
                          PSFFunctions<TPSFKey, TRecordId>> session)
            => this.freeSessions.Push(session);

        private IFasterEqualityComparer<TPSFKey> GetUserKeyComparer()
        {
            if (!(this.regSettings.KeyComparer is null))
                return this.regSettings.KeyComparer;
            if (typeof(IFasterEqualityComparer<TPSFKey>).IsAssignableFrom(typeof(TPSFKey)))
                return new TPSFKey() as IFasterEqualityComparer<TPSFKey>;

            Console.WriteLine(
                $"***WARNING*** Creating default FASTER key equality comparer based on potentially slow {nameof(EqualityComparer<TPSFKey>)}." +
                $" To avoid this, provide a comparer in {nameof(PSFRegistrationSettings<TPSFKey>)}.{nameof(PSFRegistrationSettings<TPSFKey>.KeyComparer)}," +
                $" or make {typeof(TPSFKey).Name} implement the interface {nameof(IFasterEqualityComparer<TPSFKey>)}");
            return FasterEqualityComparer<TPSFKey>.Default;
        }

        /// <summary>
        /// Returns the named <see cref="PSF{TPSFKey, TRecordId}"/> from the PSFs list.
        /// </summary>
        /// <param name="name">The name of the <see cref="PSF{TPSFKey, TRecordId}"/>; unique among all groups</param>
        /// <returns></returns>
        public PSF<TPSFKey, TRecordId> this[string name]
            => Array.Find(this.PSFs, psf => psf.Name.Equals(name, StringComparison.CurrentCultureIgnoreCase))
                ?? throw new PSFArgumentException("PSF not found");

        private unsafe void StoreKeys(ref GroupKeys keys, byte* kPtr, int kLen, PSFResultFlags* flagsPtr, int flagsLen)
        {
            var poolKeyMem = this.bufferPool.Get(kLen);
            Buffer.MemoryCopy(kPtr, poolKeyMem.GetValidPointer(), kLen, kLen);
            var flagsMem = this.bufferPool.Get(flagsLen);
            Buffer.MemoryCopy(flagsPtr, flagsMem.GetValidPointer(), flagsLen, flagsLen);
            keys.Set(poolKeyMem, flagsMem);
        }

        internal unsafe void MarkChanges(ref GroupKeysPair keysPair)
        {
            ref GroupKeys before = ref keysPair.Before;
            ref GroupKeys after = ref keysPair.After;
            ref CompositeKey<TPSFKey> beforeCompKey = ref before.CastToKeyRef<CompositeKey<TPSFKey>>();
            ref CompositeKey<TPSFKey> afterCompKey = ref after.CastToKeyRef<CompositeKey<TPSFKey>>();
            for (var ii = 0; ii < this.PSFCount; ++ii)
            {
                var beforeIsNull = before.IsNullAt(ii);
                var afterIsNull = after.IsNullAt(ii);
                var keysEqual = !beforeIsNull && !afterIsNull
                    && beforeCompKey.GetKeyRef(ii, this.keyPointerSize).Equals(afterCompKey.GetKeyRef(ii, this.keyPointerSize));

                // IsNull is already set in PSFGroup.ExecuteAndStore.
                if (!before.IsNullAt(ii) && (after.IsNullAt(ii) || !keysEqual))
                    *after.ResultFlags |= PSFResultFlags.UnlinkOld;
                if (!after.IsNullAt(ii) && (before.IsNullAt(ii) || !keysEqual))
                    *after.ResultFlags |= PSFResultFlags.LinkNew;
            }
        }

        /// <inheritdoc/>
        public unsafe Status ExecuteAndStore(TProviderData providerData, TRecordId recordId, PSFExecutePhase phase,
                                             PSFChangeTracker<TProviderData, TRecordId> changeTracker)
        {
            // Note: stackalloc is safe because PendingContext or PSFChangeTracker will copy it to the bufferPool
            // if needed. On the Insert fast path, we don't want any allocations otherwise; changeTracker is null.
            var keyMemLen = this.keyPointerSize * this.PSFCount;
            var keyBytes = stackalloc byte[keyMemLen];

            var flagsMemLen = this.PSFCount * sizeof(PSFResultFlags);
            PSFResultFlags* flags = stackalloc PSFResultFlags[this.PSFCount];
            var anyMatch = false;

            for (var ii = 0; ii < this.PSFCount; ++ii)
            {
                ref KeyPointer<TPSFKey> keyPointer = ref Unsafe.AsRef<KeyPointer<TPSFKey>>(keyBytes + ii * this.keyPointerSize);
                keyPointer.PrevAddress = Constants.kInvalidAddress;
                keyPointer.PsfOrdinal = (byte)ii;

                var key = this.psfDefinitions[ii].Execute(providerData);
                keyPointer.IsNull = !key.HasValue;
                *(flags + ii) = key.HasValue ? PSFResultFlags.None : PSFResultFlags.IsNull;
                if (key.HasValue)
                {
                    keyPointer.Key = key.Value;
                    anyMatch = true;
                }
            }

            if (!anyMatch && phase == PSFExecutePhase.Insert)
                return Status.OK;

            ref CompositeKey<TPSFKey> compositeKey = ref Unsafe.AsRef<CompositeKey<TPSFKey>>(keyBytes);
            var input = new PSFInputSecondary<TPSFKey>(0, this.Id, flags);
            var value = recordId;

            int groupOrdinal = -1;
            if (!(changeTracker is null))
            {
                value = changeTracker.BeforeRecordId;
                if (phase == PSFExecutePhase.PreUpdate)
                {
                    // Get a free group ref and store the "before" values.
                    ref GroupKeysPair groupKeysPair = ref changeTracker.FindGroupRef(this.Id);
                    StoreKeys(ref groupKeysPair.Before, keyBytes, keyMemLen, flags, flagsMemLen);
                    return Status.OK;
                }

                if (phase == PSFExecutePhase.PostUpdate)
                {
                    // TODOtest: If not found, this is a new group added after the PreUpdate was done, so handle this as an insert.
                    if (!changeTracker.FindGroup(this.Id, out groupOrdinal))
                    {
                        phase = PSFExecutePhase.Insert;
                    }
                    else
                    {
                        ref GroupKeysPair groupKeysPair = ref changeTracker.GetGroupRef(groupOrdinal);
                        StoreKeys(ref groupKeysPair.After, keyBytes, keyMemLen, flags, flagsMemLen);
                        this.MarkChanges(ref groupKeysPair);
                        // TODOtest: In debug, for initial dev, follow chains to assert the values match what is in the record's compositeKey
                        if (!groupKeysPair.HasChanges)
                            return Status.OK;
                    }
                }

                // We don't need to do anything here for Delete.
            }

            var session = this.GetSession();
            try
            {
                var lsn = session.ctx.serialNum + 1;
                return phase switch
                {
                    PSFExecutePhase.Insert => session.PsfInsert(ref compositeKey.CastToFirstKeyPointerRefAsKeyRef(), ref value, ref input, lsn),
                    PSFExecutePhase.PostUpdate => session.PsfUpdate(ref changeTracker.GetGroupRef(groupOrdinal),
                                                                    ref value, ref input, lsn, changeTracker),
                    PSFExecutePhase.Delete => session.PsfDelete(ref compositeKey.CastToFirstKeyPointerRefAsKeyRef(), ref value, ref input, lsn,
                                                                    changeTracker),
                    _ => throw new PSFInternalErrorException("Unknown PSF execution Phase {phase}")
                };
            } 
            finally
            {
                this.ReleaseSession(session);
            }
        }

        /// <inheritdoc/>
        public Status GetBeforeKeys(PSFChangeTracker<TProviderData, TRecordId> changeTracker)
        {
            if (changeTracker.HasBeforeKeys)
                return Status.OK;

            // Obtain the "before" values. TODOcache: try to find TRecordId in the IPUCache first.
            return ExecuteAndStore(changeTracker.BeforeData, default, PSFExecutePhase.PreUpdate, changeTracker);
        }

        /// <summary>
        /// Update the RecordId
        /// </summary>
        public Status Update(PSFChangeTracker<TProviderData, TRecordId> changeTracker)
        {
            if (changeTracker.UpdateOp == UpdateOperation.Insert)
            {
                // RMW did not find the record so did an insert. Go through Insert logic here.
                return this.ExecuteAndStore(changeTracker.BeforeData, changeTracker.BeforeRecordId, PSFExecutePhase.Insert, changeTracker:null);
            }

            changeTracker.CachedBeforeLA = Constants.kInvalidAddress; // TODOcache: Find BeforeRecordId in IPUCache
            if (changeTracker.CachedBeforeLA != Constants.kInvalidAddress)
            {
                if (changeTracker.UpdateOp == UpdateOperation.RCU)
                {
                    // TODOcache: Tombstone it, and possibly unlink; or just copy its keys into changeTracker.Before.
                }
                else
                {
                    // TODOcache: Try to splice in-place; or just copy its keys into changeTracker.Before.
                }
            }
            else
            {
                if (this.GetBeforeKeys(changeTracker) != Status.OK)
                {
                    // TODOerr: handle errors from GetBeforeKeys
                }
            }
            return this.ExecuteAndStore(changeTracker.AfterData, default, PSFExecutePhase.PostUpdate, changeTracker);
        }

        /// <summary>
        /// Delete the RecordId
        /// </summary>
        public Status Delete(PSFChangeTracker<TProviderData, TRecordId> changeTracker)
        {
            changeTracker.CachedBeforeLA = Constants.kInvalidAddress; // TODOcache: Find BeforeRecordId in IPUCache
            if (changeTracker.CachedBeforeLA != Constants.kInvalidAddress)
            {
                // TODOcache: Tombstone it, and possibly unlink; or just copy its keys into changeTracker.Before.
                // If the latter, we can bypass ExecuteAndStore's PSF-execute loop
            }
            return this.ExecuteAndStore(changeTracker.BeforeData, default, PSFExecutePhase.Delete, changeTracker);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private unsafe PSFInputSecondary<TPSFKey> MakeQueryInput(int psfOrdinal, ref TPSFKey key)
        {
            // Putting the query key in PSFInput is necessary because iterator functions cannot contain unsafe code or have
            // byref args, and bufferPool is needed here because the stack goes away as part of the iterator operation.
            var psfInput = new PSFInputSecondary<TPSFKey>(psfOrdinal, this.Id);
            psfInput.SetQueryKey(this.bufferPool, this.keyAccessor, ref key);
            return psfInput;
        }

        /// <inheritdoc/>
        public unsafe IEnumerable<TRecordId> Query(int psfOrdinal, TPSFKey key, PSFQuerySettings querySettings)
            => Query(MakeQueryInput(psfOrdinal, ref key), querySettings);

        private IEnumerable<TRecordId> Query(PSFInputSecondary<TPSFKey> input, PSFQuerySettings querySettings)
        {
            // TODOperf: if there are multiple PSFs within this group we can step through in parallel and return them
            // as a single merged stream; will require multiple TPSFKeys and their indexes in queryKeyPtr. Also consider
            // having TPSFKeys[] for a single PSF walk through in parallel, so the FHT log memory access is sequential.
            var secondaryOutput = new PSFOutputSecondary<TPSFKey, TRecordId>(this.keyAccessor);
            var readArgs = new PSFReadArgs<TPSFKey, TRecordId>(input, secondaryOutput);

            var session = this.GetSession();
            var deadRecs = new DeadRecords<TRecordId>();
            try
            {
                // Because we traverse the chain, we must wait for any pending read operations to complete.
                // TODOperf: See if there is a better solution than spinWaiting in CompletePending.
                Status status = session.PsfReadKey(ref input.QueryKeyRef, ref readArgs, session.ctx.serialNum + 1);
                if (querySettings.IsCanceled)
                    yield break;
                if (status == Status.PENDING)
                    session.CompletePending(spinWait: true);
                if (status != Status.OK)    // TODOerr: check other status
                    yield break;

                if (secondaryOutput.Tombstone)
                    deadRecs.Add(secondaryOutput.RecordId);
                else
                    yield return secondaryOutput.RecordId;

                do
                {
                    readArgs.Input.ReadLogicalAddress = secondaryOutput.PreviousLogicalAddress;
                    status = session.PsfReadAddress(ref readArgs, session.ctx.serialNum + 1);
                    if (status == Status.PENDING)
                        session.CompletePending(spinWait: true);
                    if (querySettings.IsCanceled)
                        yield break;
                    if (status != Status.OK)    // TODOerr: check other status
                        yield break;

                    if (deadRecs.IsDead(secondaryOutput.RecordId, secondaryOutput.Tombstone))
                        continue;

                    yield return secondaryOutput.RecordId;
                } while (secondaryOutput.PreviousLogicalAddress != Constants.kInvalidAddress);
            }
            finally
            {
                this.ReleaseSession(session);
                input.Dispose();
            }
        }

#if DOTNETCORE
        /// <inheritdoc/>
        public unsafe IAsyncEnumerable<TRecordId> QueryAsync(int psfOrdinal, TPSFKey key, PSFQuerySettings querySettings)
            => QueryAsync(MakeQueryInput(psfOrdinal, ref key), querySettings);

        private async IAsyncEnumerable<TRecordId> QueryAsync(PSFInputSecondary<TPSFKey> input, PSFQuerySettings querySettings)
        {
            // TODOperf: if there are multiple PSFs within this group we can step through in parallel and return them
            // as a single merged stream; will require multiple TPSFKeys and their indexes in queryKeyPtr. Also consider
            // having TPSFKeys[] for a single PSF walk through in parallel, so the FHT log memory access is sequential.
            var secondaryOutput = new PSFOutputSecondary<TPSFKey, TRecordId>(this.keyAccessor);
            var readArgs = new PSFReadArgs<TPSFKey, TRecordId>(input, secondaryOutput);

            var session = this.GetSession();
            var deadRecs = new DeadRecords<TRecordId>();
            try
            {
                // Because we traverse the chain, we must wait for any pending read operations to complete.
                var readAsyncResult = await session.PsfReadKeyAsync(ref input.QueryKeyRef, ref readArgs, session.ctx.serialNum + 1, querySettings);
                if (querySettings.IsCanceled)
                    yield break;
                var (status, _) = readAsyncResult.CompleteRead();
                if (status != Status.OK)    // TODOerr: check other status
                    yield break;

                if (secondaryOutput.Tombstone)
                    deadRecs.Add(secondaryOutput.RecordId);
                else
                    yield return secondaryOutput.RecordId;

                do
                {
                    readArgs.Input.ReadLogicalAddress = secondaryOutput.PreviousLogicalAddress;
                    readAsyncResult = await session.PsfReadAddressAsync(ref readArgs, session.ctx.serialNum + 1, querySettings);
                    if (querySettings.IsCanceled)
                        yield break;
                    (status, _) = readAsyncResult.CompleteRead();
                    if (status != Status.OK)    // TODOerr: check other status
                        yield break;

                    if (deadRecs.IsDead(secondaryOutput.RecordId, secondaryOutput.Tombstone))
                        continue;

                    yield return secondaryOutput.RecordId;
                } while (secondaryOutput.PreviousLogicalAddress != Constants.kInvalidAddress);
            }
            finally
            {
                this.ReleaseSession(session);
                input.Dispose();
            }
        }
#endif

        #region Checkpoint Operations
        /// <inheritdoc/>
        public bool TakeFullCheckpoint() => this.fht.TakeFullCheckpoint(out _);

        /// <inheritdoc/>
        public ValueTask CompleteCheckpointAsync(CancellationToken token = default) => this.fht.CompleteCheckpointAsync(token);

        /// <inheritdoc/>
        public bool TakeIndexCheckpoint() => this.fht.TakeIndexCheckpoint(out _);

        /// <inheritdoc/>
        public bool TakeHybridLogCheckpoint() => this.fht.TakeHybridLogCheckpoint(out _);

        /// <inheritdoc/>
        public void Recover() => this.fht.Recover();
        #endregion Checkpoint Operations

        #region Log Operations
        /// <inheritdoc/>
        public void FlushLog(bool wait) => this.fht.Log.Flush(wait);

        /// <inheritdoc/>
        public bool FlushAndEvictLog(bool wait) => this.fht.Log.FlushAndEvict(wait);

        /// <inheritdoc/>
        public void DisposeLogFromMemory() => this.fht.Log.DisposeFromMemory();
        #endregion Log Operations
    }
}
