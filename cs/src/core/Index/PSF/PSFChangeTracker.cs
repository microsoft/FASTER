// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

namespace FASTER.core
{
    public enum UpdateOperation
    {
        Insert,
        IPU,
        RCU,
        Delete
    }

    public unsafe class PSFChangeTracker<TProviderData, TRecordId> : IDisposable
        where TRecordId : new()
    {
        #region Data API
        internal TProviderData BeforeData { get; private set; }
        internal TRecordId BeforeRecordId { get; private set; }

        internal void SetBeforeData(TProviderData data, TRecordId recordId)
        {
            this.BeforeData = data;
            this.BeforeRecordId = recordId;
        }

        internal TProviderData AfterData { get; private set; }
        internal TRecordId AfterRecordId { get; private set; }

        internal void SetAfterData(TProviderData data, TRecordId recordId)
        {
            this.AfterData = data;
            this.AfterRecordId = recordId;
        }

        public UpdateOperation UpdateOp { get; set; }
        #endregion Data API

        private GroupKeysPair[] groups;
        internal bool HasBeforeKeys { get; set; }

        internal long CachedBeforeLA = Constants.kInvalidAddress;

        internal PSFChangeTracker(IEnumerable<long> groupIds)
        {
            this.groups = groupIds.Select(id => new GroupKeysPair(id)).ToArray();
        }

        internal bool FindGroup(long groupId, out int ordinal)
        {
            for (var ii = 0; ii < this.groups.Length; ++ii) // TODOperf: will there be enough groups for sequential search to matter?
            {
                if (groups[ii].GroupId == groupId)
                {
                    ordinal = ii;
                    return true;
                }
            }

            // Likely the groupId was from a group added since this PSFChangeTracker instance was created.
            ordinal = -1;
            return false;
        }

        internal ref GroupKeysPair GetGroupRef(int ordinal) => ref groups[ordinal];

        internal ref GroupKeysPair FindGroupRef(long groupId, long logAddr = Constants.kInvalidAddress)
        {
            if (!this.FindGroup(groupId, out var ordinal))
            {
                // A new group was added while we were populating this changeTracker; should be quite rare. // TODOtest: this case
                var groups = new GroupKeysPair[this.groups.Length + 1];
                Array.Copy(this.groups, groups, this.groups.Length);
                this.groups = groups;
                ordinal = this.groups.Length - 1;
            }
            ref GroupKeysPair ret = ref this.groups[ordinal];
            ret.GroupId = groupId;
            ret.LogicalAddress = logAddr;
            return ref ret;
        }

        public void Dispose()
        {
            foreach (var group in this.groups)
                group.Dispose();
        }
    }
}
