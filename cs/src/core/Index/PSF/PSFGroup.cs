// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
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
        internal FasterKV<PSFCompositeKey<TPSFKey>, PSFValue<TRecordId>, PSFInputSecondary<TPSFKey>,
                          PSFOutputSecondary<TPSFKey, TRecordId>, PSFContext, PSFFunctions<TPSFKey, TRecordId>> fht;
        private readonly PSFFunctions<TPSFKey, TRecordId> functions;
        internal IPSFDefinition<TProviderData, TPSFKey>[] psfDefinitions;
        private readonly int ordinal;   // in the parent's PSFGroup list TODO needed?
        private readonly PSFRegistrationSettings<TPSFKey> regSettings;

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

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="ordinal">The ordinal of this PSFGroup in the <see cref="PSFManager{TProviderData, TRecordId}"/>'s
        /// PSFGroup list.</param>
        /// <param name="defs">PSF definitions</param>
        /// <param name="regSettings">Optional registration settings</param>
        public PSFGroup(int ordinal, IPSFDefinition<TProviderData, TPSFKey>[] defs, PSFRegistrationSettings<TPSFKey> regSettings)
        {
            // TODO check for null defs regSettings etc; for PSFs defined on a FasterKV instance we create intelligent
            // defaults in regSettings but other clients will have to specify at least hashTableSize, logSettings, etc.
            this.psfDefinitions = defs;
            this.ordinal = ordinal;
            this.regSettings = regSettings;
            this.userKeyComparer = GetUserKeyComparer();

            this.PSFs = defs.Select((def, ord) => new PSF<TPSFKey, TRecordId>(this.ordinal, ord, def.Name, this)).ToArray();
            this.compositeKeyComparer = new PSFCompositeKeyComparer<TPSFKey>(this.userKeyComparer, this.keySize, this.ChainHeight);

            // TODO doc (or change) chainPost terminology
            this.chainPost = new PSFValue<TRecordId>.ChainPost(this.ChainHeight, this.keySize);

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
                          PSFFunctions<TPSFKey, TRecordId>> GetSession ()
        {
            // Sessions are used only on post-RegisterPSF actions (Upsert, RMW, Query).
            if (this.freeSessions.TryPop(out var session))
                return session;
            session = this.fht.NewSession(threadAffinitized:this.regSettings.ThreadAffinitized);
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
                ?? throw new InvalidOperationException("TODO PSF Exception classes: PSF not found");

        /// <inheritdoc/>
        public unsafe Status ExecuteAndStore(TProviderData providerData, TRecordId recordId, bool isInserted)
        {
            // Note: stackalloc is safe here because PendingContext will copy it to the bufferPool if needed.
            var keyMemLen = ((keySize * this.ChainHeight + sizeof(int) - 1) & ~(sizeof(int) - 1)) / sizeof(int);
            var keyMem = stackalloc int[keyMemLen];
            for (var ii = 0; ii < keyMemLen; ++ii)
                keyMem[ii] = 0;
            ref PSFCompositeKey<TPSFKey> compositeKey = ref Unsafe.AsRef<PSFCompositeKey<TPSFKey>>(keyMem);

            var valueMem = stackalloc byte[recordIdSize + sizeof(long) * this.ChainHeight];
            ref PSFValue<TRecordId> psfValue = ref Unsafe.AsRef<PSFValue<TRecordId>>(valueMem);
            psfValue.RecordId = recordId;

            bool* nullIndicators = stackalloc bool[this.ChainHeight];
            var anyMatch = false;
            long* chainLinkPtrs = this.fht.chainPost.GetChainLinkPtrs(ref psfValue);
            for (int ii = 0; ii < this.psfDefinitions.Length; ++ii)
            {
                var key = this.psfDefinitions[ii].Execute(providerData);
                *(nullIndicators + ii) = !key.HasValue;
                *(chainLinkPtrs + ii) = Constants.kInvalidAddress;
                if (key.HasValue)
                {
                    ref TPSFKey keyPtr = ref Unsafe.AsRef<TPSFKey>(keyMem + keySize * ii);
                    keyPtr = key.Value;
                    anyMatch = true;
                }
            }

            if (!anyMatch)
                return Status.OK;

            var input = new PSFInputSecondary<TPSFKey>(0, compositeKeyComparer, nullIndicators);
            var session = this.GetSession();
            Status status;
            try     // TODOperf: Assess overhead of try/finally
            {
                status = isInserted
                    ? session.PsfInsert(ref compositeKey, ref psfValue, ref input, 1 /*TODO: lsn*/)
                    : throw new NotImplementedException("TODO updates");
            }
            finally
            {
                this.ReleaseSession(session);
            }
            return status;
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Verify(TProviderData providerData, int psfOrdinal) 
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
            var psfInput = new PSFInputSecondary<TPSFKey>(psfOrdinal, compositeKeyComparer);
            return Query(keyPtr, psfInput);
        }

        private IEnumerable<TRecordId> Query(PSFCompositeKey<TPSFKey>.PtrWrapper queryKeyPtr,
                                             PSFInputSecondary<TPSFKey> input)
        {
            var readArgs = new PSFReadArgs<PSFCompositeKey<TPSFKey>, PSFValue<TRecordId>>(
                            input, new PSFOutputSecondary<TPSFKey, TRecordId>(this.chainPost));

            var session = this.GetSession();
            Status status;
            try
            {
                status = session.PsfReadKey(ref queryKeyPtr.GetRef(), ref readArgs, 1 /*TODO lsn*/);
                if (status != Status.OK)    // TODO check other status
                    yield break;

                var secondaryOutput = readArgs.Output as IPSFSecondaryOutput<TRecordId>;
                yield return secondaryOutput.RecordId;

                do
                {
                    readArgs.Input.ReadLogicalAddress = secondaryOutput.PreviousLogicalAddress;
                    status = session.PsfReadAddress(ref readArgs, 1 /*TODO lsn*/);
                    if (status != Status.OK)    // TODO check other status
                        yield break;
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
