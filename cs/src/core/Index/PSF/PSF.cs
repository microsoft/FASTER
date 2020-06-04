// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Collections.Generic;

namespace FASTER.core
{
    /// <summary>
    /// The implementation of the Predicate Subset Function.
    /// </summary>
    /// <typeparam name="TPSFKey">The type of the key returned by the Predicate and store in the secondary
    ///     FasterKV instance</typeparam>
    /// <typeparam name="TRecordId">The type of data record supplied by the data provider; in FasterKV it 
    ///     is the logicalAddress of the record in the primary FasterKV instance.</typeparam>
    public class PSF<TPSFKey, TRecordId> : IPSF
    {
        private readonly IQueryPSF<TPSFKey, TRecordId> psfGroup;

        internal long GroupId { get; }           // unique in the PSFManager.psfGroup list

        internal int PsfOrdinal { get; }        // in the psfGroup

        /// <inheritdoc/>
        public string Name { get; }

        internal PSF(long groupId, int psfOrdinal, string name, IQueryPSF<TPSFKey, TRecordId> iqp)
        {
            this.GroupId = groupId;
            this.PsfOrdinal = psfOrdinal;
            this.Name = name;
            this.psfGroup = iqp;
        }

        /// <summary>
        /// Issues a query on this PSF to return <typeparamref name="TRecordId"/>s.
        /// </summary>
        /// <returns></returns>
        public IEnumerable<TRecordId> Query(TPSFKey key)
            => this.psfGroup.Query(this.PsfOrdinal, key);
    }
}
