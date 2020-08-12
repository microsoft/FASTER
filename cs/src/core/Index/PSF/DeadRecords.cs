// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace FASTER.core.Index.PSF
{
    internal struct DeadRecords<TRecordId>
        where TRecordId : struct
    {
        private HashSet<TRecordId> deadRecs;

        internal void Add(TRecordId recordId)
        {
            this.deadRecs ??= new HashSet<TRecordId>();
            this.deadRecs.Add(recordId);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool IsDead(TRecordId recordId, bool thisInstanceIsDead)
        {
            if (thisInstanceIsDead)
            {
                this.Add(recordId);
                return true;
            }
            return ContainsAndUpdate(recordId, thisInstanceIsDead);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool ContainsAndUpdate(TRecordId recordId, bool thisInstanceIsDead)
        {
            if (this.deadRecs is null || !this.deadRecs.Contains(recordId))
                return false;
            if (!thisInstanceIsDead)    // A live record will not be encountered again so remove it
                this.Remove(recordId);
            return true;
        }

        internal void Remove(TRecordId recordId)
        {
            if (!(this.deadRecs is null))
                this.deadRecs.Remove(recordId);
        }
    }
}
