// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;

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
        // TODO: ensure consistency of the sequence of generic args
        internal FasterKV<PSFCompositeKey<TPSFKey>, PSFValue<TRecordId>, PSFInputSecondary<TPSFKey>,
                          PSFOutputSecondary<TPSFKey, TRecordId>, PSFContext, PSFFunctions<TPSFKey, TRecordId>> fht;
        private readonly PSFFunctions<TPSFKey, TRecordId> functions;
        internal IPSFDefinition<TProviderData, TPSFKey>[] psfDefinitions;
        private readonly PSFRegistrationSettings<TPSFKey> regSettings;

        /// <summary>
        /// ID of the group (used internally only)
        /// </summary>
        public long Id { get; }

        private readonly CheckpointSettings checkpointSettings;
        private readonly int keySize = Utility.GetSize(default(TPSFKey));
        private readonly int recordIdSize = (Utility.GetSize(default(TRecordId)) + sizeof(long) - 1) & ~(sizeof(long) - 1);

        internal ConcurrentStack<ClientSession<PSFCompositeKey<TPSFKey>,
                        PSFValue<TRecordId>, PSFInputSecondary<TPSFKey>,
                        PSFOutputSecondary<TPSFKey, TRecordId>, PSFContext, PSFFunctions<TPSFKey, TRecordId>>> freeSessions
            = new ConcurrentStack<ClientSession<PSFCompositeKey<TPSFKey>,
                        PSFValue<TRecordId>, PSFInputSecondary<TPSFKey>,
                        PSFOutputSecondary<TPSFKey, TRecordId>, PSFContext, PSFFunctions<TPSFKey, TRecordId>>>();
        internal ConcurrentBag<ClientSession<PSFCompositeKey<TPSFKey>,
                        PSFValue<TRecordId>, PSFInputSecondary<TPSFKey>,
                        PSFOutputSecondary<TPSFKey, TRecordId>, PSFContext, PSFFunctions<TPSFKey, TRecordId>>> allSessions
            = new ConcurrentBag<ClientSession<PSFCompositeKey<TPSFKey>,
                        PSFValue<TRecordId>, PSFInputSecondary<TPSFKey>,
                        PSFOutputSecondary<TPSFKey, TRecordId>, PSFContext, PSFFunctions<TPSFKey, TRecordId>>>();

        /// <summary>
        /// The list of <see cref="PSF{TPSFKey, TRecordId}"/>s in this group
        /// </summary>
        public PSF<TPSFKey, TRecordId>[] PSFs { get; private set; }

        private int ChainHeight => this.PSFs.Length;

        private readonly IFasterEqualityComparer<TPSFKey> userKeyComparer;
        private readonly ICompositeKeyComparer<PSFCompositeKey<TPSFKey>> compositeKeyComparer;
        private readonly IChainPost<PSFValue<TRecordId>> chainPost;

        private readonly SectorAlignedBufferPool bufferPool;

        // Override equivalence testing for set membership

        /// <inheritdoc/>
        public override int GetHashCode() => this.Id.GetHashCode();

        /// <inheritdoc/>
        public override bool Equals(object obj) => this.Equals(obj as PSFGroup<TProviderData, TPSFKey, TRecordId>);

        /// <inheritdoc/>
        public bool Equals(PSFGroup<TProviderData, TPSFKey, TRecordId> other) => other is null ? false : this.Id == other.Id;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="id">The ordinal of this PSFGroup in the <see cref="PSFManager{TProviderData, TRecordId}"/>'s
        /// PSFGroup list.</param>
        /// <param name="defs">PSF definitions</param>
        /// <param name="regSettings">Optional registration settings</param>
        public PSFGroup(long id, IPSFDefinition<TProviderData, TPSFKey>[] defs, PSFRegistrationSettings<TPSFKey> regSettings)
        {
            this.psfDefinitions = defs;
            this.Id = id;
            this.regSettings = regSettings;
            this.userKeyComparer = GetUserKeyComparer();

            this.PSFs = defs.Select((def, ord) => new PSF<TPSFKey, TRecordId>(this.Id, ord, def.Name, this)).ToArray();
            this.compositeKeyComparer = new PSFCompositeKeyComparer<TPSFKey>(this.userKeyComparer, this.keySize, this.ChainHeight);

            // TODO doc (or change) chainPost terminology
            this.chainPost = new PSFValue<TRecordId>.ChainPost(this.ChainHeight, this.recordIdSize);

            this.checkpointSettings = regSettings?.CheckpointSettings;
            this.functions = new PSFFunctions<TPSFKey, TRecordId>(chainPost);
            this.fht = new FasterKV<PSFCompositeKey<TPSFKey>, PSFValue<TRecordId>, PSFInputSecondary<TPSFKey>,
                                    PSFOutputSecondary<TPSFKey, TRecordId>, PSFContext, PSFFunctions<TPSFKey, TRecordId>>(
                    regSettings.HashTableSize, new PSFFunctions<TPSFKey, TRecordId>(chainPost),
                    regSettings.LogSettings, this.checkpointSettings, null /*SerializerSettings*/,
                    new PSFCompositeKey<TPSFKey>.UnusedKeyComparer(),
                    new VariableLengthStructSettings<PSFCompositeKey<TPSFKey>, PSFValue<TRecordId>>
                    {
                        keyLength = new PSFCompositeKey<TPSFKey>.VarLenLength(this.keySize, this.ChainHeight),
                        valueLength = new PSFValue<TRecordId>.VarLenLength(this.recordIdSize, this.ChainHeight)
                    }
                )
            { chainPost = chainPost };

            this.bufferPool = this.fht.hlog.bufferPool;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private ClientSession<PSFCompositeKey<TPSFKey>, PSFValue<TRecordId>, PSFInputSecondary<TPSFKey>,
                          PSFOutputSecondary<TPSFKey, TRecordId>, PSFContext,
                          PSFFunctions<TPSFKey, TRecordId>> GetSession()
        {
            // Sessions are used only on post-RegisterPSF actions (Upsert, RMW, Query).
            if (this.freeSessions.TryPop(out var session))
                return session;
            session = this.fht.NewSession(threadAffinitized: this.regSettings.ThreadAffinitized);
            this.allSessions.Add(session);
            return session;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ReleaseSession(ClientSession<PSFCompositeKey<TPSFKey>, PSFValue<TRecordId>, PSFInputSecondary<TPSFKey>,
                          PSFOutputSecondary<TPSFKey, TRecordId>, PSFContext,
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

        internal unsafe void MarkChanges(GroupKeysPair keysPair)
        {
            var before = keysPair.Before;
            var after = keysPair.After;
            var beforeCompKey = before.GetCompositeKeyRef<PSFCompositeKey<TPSFKey>>();
            var afterCompKey = after.GetCompositeKeyRef<PSFCompositeKey<TPSFKey>>();
            for (var ii = 0; ii < this.ChainHeight; ++ii)
            {
                bool keysEqual() => beforeCompKey.GetKeyRef(ii, this.keySize).Equals(afterCompKey.GetKeyRef(ii, this.keySize));

                // IsNull is already set in PSFGroup.ExecuteAndStore.
                if (!before.IsNullAt(ii) && (after.IsNullAt(ii) || !keysEqual()))
                    *after.ResultFlags |= PSFResultFlags.UnlinkOld;
                if (!after.IsNullAt(ii) && (before.IsNullAt(ii) || !keysEqual()))
                    *after.ResultFlags |= PSFResultFlags.LinkNew;
            }
        }

        /// <inheritdoc/>
        public unsafe Status ExecuteAndStore(TProviderData providerData, TRecordId recordId, PSFExecutePhase phase,
                                             PSFChangeTracker<TProviderData, TRecordId> changeTracker)
        {
            // Note: stackalloc is safe because PendingContext or PSFChangeTracker will copy it to the bufferPool
            // if needed. On the Insert fast path, we don't want any allocations otherwise; changeTracker is null.
            // TODO: Change ChainHeight to psfCount
            var keyMemLen = ((keySize * this.ChainHeight + sizeof(int) - 1) & ~(sizeof(int) - 1));
            var keyMemInt = stackalloc int[keyMemLen / sizeof(int)];
            for (var ii = 0; ii < keyMemLen; ++ii)
                keyMemInt[ii] = 0;
            var keyMem = (byte*)keyMemInt;
            ref PSFCompositeKey<TPSFKey> compositeKey = ref Unsafe.AsRef<PSFCompositeKey<TPSFKey>>(keyMem);

            var flagsMemLen = this.ChainHeight * sizeof(PSFResultFlags);
            PSFResultFlags* flags = stackalloc PSFResultFlags[this.ChainHeight];
            var anyMatch = false;
            for (int ii = 0; ii < this.psfDefinitions.Length; ++ii)
            {
                var key = this.psfDefinitions[ii].Execute(providerData);
                
                *(flags + ii) = key.HasValue ? PSFResultFlags.None : PSFResultFlags.IsNull;
                if (key.HasValue)
                {
                    ref TPSFKey keyPtr = ref Unsafe.AsRef<TPSFKey>(keyMem + keySize * ii);
                    keyPtr = key.Value;
                    anyMatch = true;
                }
            }

            if (!anyMatch && phase == PSFExecutePhase.Insert)
                return Status.OK;

            var input = new PSFInputSecondary<TPSFKey>(0, compositeKeyComparer, this.Id, flags);

            var valueMem = stackalloc byte[recordIdSize + sizeof(long) * this.ChainHeight];
            ref PSFValue<TRecordId> psfValue = ref Unsafe.AsRef<PSFValue<TRecordId>>(valueMem);
            psfValue.RecordId = recordId;

            int groupOrdinal = -1;
            if (!(changeTracker is null))
            {
                psfValue.RecordId = changeTracker.BeforeRecordId;
                if (phase == PSFExecutePhase.PreUpdate)
                {
                    // Get a free group ref and store the "before" values.
                    ref GroupKeysPair groupKeysPair = ref changeTracker.FindFreeGroupRef(this.Id, this.keySize);
                    StoreKeys(ref groupKeysPair.Before, keyMem, keyMemLen, flags, flagsMemLen);
                    return Status.OK;
                }

                if (phase == PSFExecutePhase.PostUpdate)
                {
                    // If not found, this is a new group added after the PreUpdate was done, so handle this as an insert.
                    if (!changeTracker.FindGroup(this.Id, out groupOrdinal))
                    {
                        phase = PSFExecutePhase.Insert;
                    }
                    else
                    {
                        ref GroupKeysPair groupKeysPair = ref changeTracker.GetGroupRef(groupOrdinal);
                        StoreKeys(ref groupKeysPair.After, keyMem, keyMemLen, flags, flagsMemLen);
                        this.MarkChanges(groupKeysPair);
                        // TODO in debug, for initial dev, follow chains to assert the values match what is in the record's compositeKey
                        if (!groupKeysPair.HasChanges)
                            return Status.OK;
                    }
                }

                // We don't need to do anything here for Delete.
            }

            long* chainLinkPtrs = this.fht.chainPost.GetChainLinkPtrs(ref psfValue);
            for (int ii = 0; ii < this.psfDefinitions.Length; ++ii)
                *(chainLinkPtrs + ii) = Constants.kInvalidAddress;

            var session = this.GetSession();
            try
            {
                var lsn = session.ctx.serialNum + 1;
                return phase switch
                {
                    PSFExecutePhase.Insert => session.PsfInsert(ref compositeKey, ref psfValue, ref input, lsn),
                    PSFExecutePhase.PostUpdate => session.PsfUpdate(ref changeTracker.GetGroupRef(groupOrdinal),
                                                                    ref psfValue, ref input, lsn, changeTracker),
                    PSFExecutePhase.Delete => session.PsfDelete(ref compositeKey, ref psfValue, ref input, lsn, changeTracker),
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
            // Obtain the "before" values. TODO try to find TRecordId in the IPUCache first.
            return ExecuteAndStore(changeTracker.BeforeData, default, PSFExecutePhase.PreUpdate, changeTracker);
        }

        /// <summary>
        /// Update the RecordId
        /// </summary>
        public Status Update(PSFChangeTracker<TProviderData, TRecordId> changeTracker)
        {
            changeTracker.CachedBeforeLA = Constants.kInvalidAddress; // TODO Find BeforeRecordId in IPUCache
            if (changeTracker.CachedBeforeLA != Constants.kInvalidAddress)
            {
                if (changeTracker.UpdateOp == UpdateOperation.RCU)
                {
                    // TODO: Tombstone it, and possibly unlink; or just copy its keys into changeTracker.Before.
                }
                else
                {
                    // TODO: Try to splice in-place; or just copy its keys into changeTracker.Before.
                }
            }
            else
            {
                if (this.GetBeforeKeys(changeTracker) != Status.OK)
                {
                    // TODO handle errors from GetBeforeKeys
                }
            }
            return this.ExecuteAndStore(changeTracker.AfterData, default, PSFExecutePhase.PostUpdate, changeTracker);
        }

        /// <summary>
        /// Delete the RecordId
        /// </summary>
        public Status Delete(PSFChangeTracker<TProviderData, TRecordId> changeTracker)
        {
            changeTracker.CachedBeforeLA = Constants.kInvalidAddress; // TODO Find BeforeRecordId in IPUCache
            if (changeTracker.CachedBeforeLA != Constants.kInvalidAddress)
            {
                // TODO: Tombstone it, and possibly unlink; or just copy its keys into changeTracker.Before.
                // If the latter, we can bypass ExecuteAndStore's PSF-execute loop
            }
            return this.ExecuteAndStore(changeTracker.BeforeData, default, PSFExecutePhase.Delete, changeTracker);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Verify(TProviderData providerData, int psfOrdinal)  // TODO revise to new spec
            => !(this.psfDefinitions[psfOrdinal].Execute(providerData) is null);

        /// <inheritdoc/>
        public unsafe IEnumerable<TRecordId> Query(int psfOrdinal, TPSFKey key)
        {
            // Create a varlen CompositeKey with just one item. This is ONLY used as the query key to
            // PSFCompositeKeyComparer. Iterator functions cannot contain unsafe code or have byref args,
            // and bufferPool is needed here because the stack goes away as part of the iterator operation.
            var keyPtr = new PSFCompositeKey<TPSFKey>.PtrWrapper(this.keySize, this.bufferPool);
            keyPtr.Set(ref key);

            // These indirections are necessary to avoid issues with passing ref or unsafe to a enumerator function.
            // TODOperf: PSFInput* and PSFOutput* are classes because we communicate through interfaces to avoid
            // having to add additional generic args. Interfaces on structs incur boxing overhead (plus the complexity
            // of mutable structs). But check the performance here; if necessary perhaps I can figure out a way to
            // pass a struct with no TRecordId, TPSFKey, etc. and use an FHT-level interface to manage it.
            var psfInput = new PSFInputSecondary<TPSFKey>(psfOrdinal, compositeKeyComparer, this.Id);
            return Query(keyPtr, psfInput);
        }

        private IEnumerable<TRecordId> Query(PSFCompositeKey<TPSFKey>.PtrWrapper queryKeyPtr,
                                             PSFInputSecondary<TPSFKey> input)
        {
            var readArgs = new PSFReadArgs<PSFCompositeKey<TPSFKey>, PSFValue<TRecordId>>(
                            input, new PSFOutputSecondary<TPSFKey, TRecordId>(this.chainPost));

            var session = this.GetSession();
            Status status;
            HashSet<TRecordId> deadRecs = null;
            try
            {
                status = session.PsfReadKey(ref queryKeyPtr.GetRef(), ref readArgs, session.ctx.serialNum + 1);
                if (status != Status.OK)    // TODO check other status
                    yield break;

                var secondaryOutput = readArgs.Output as IPSFSecondaryOutput<TRecordId>;

                if (secondaryOutput.Tombstone)
                {
                    deadRecs ??= new HashSet<TRecordId>();
                    deadRecs.Add(secondaryOutput.RecordId);
                }
                else
                {
                    yield return secondaryOutput.RecordId;
                }

                do
                {
                    readArgs.Input.ReadLogicalAddress = secondaryOutput.PreviousLogicalAddress;
                    status = session.PsfReadAddress(ref readArgs, session.ctx.serialNum + 1);
                    if (status != Status.OK)    // TODO check other status
                        yield break;
                    
                    if (secondaryOutput.Tombstone)
                    {
                        deadRecs ??= new HashSet<TRecordId>();
                        deadRecs.Add(secondaryOutput.RecordId);
                        continue;
                    }

                    if (!(deadRecs is null) && deadRecs.Contains(secondaryOutput.RecordId))
                    {
                        if (!secondaryOutput.Tombstone) // A live record will not be encountered again so remove it
                            deadRecs.Remove(secondaryOutput.RecordId);
                        continue;
                    }
                    yield return secondaryOutput.RecordId;
                } while (secondaryOutput.PreviousLogicalAddress != Constants.kInvalidAddress);
            }
            finally
            {
                this.ReleaseSession(session);
                queryKeyPtr.Dispose();
            }
        }
    }
}
