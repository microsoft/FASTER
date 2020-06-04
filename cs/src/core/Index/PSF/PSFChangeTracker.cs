// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
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
        where TRecordId : struct
    {
        #region External API
        public TProviderData BeforeData { get; set; }
        public TRecordId BeforeRecordId { get; set; }

        public TProviderData AfterData { get; set; }
        public TRecordId AfterRecordId { get; set; }

        public UpdateOperation UpdateOp { get; set; }
        #endregion External API

        private GroupKeysPair[] groups;

        internal long CachedBeforeLA = Constants.kInvalidAddress;

        internal void PrepareGroups(int numGroups)
        {
            this.groups = new GroupKeysPair[numGroups];
            for (var ii = 0; ii < numGroups; ++ii)
                this.groups[ii].GroupId = Constants.kInvalidPsfGroupId;
        }

        internal bool FindGroup(long groupId, out int ordinal)
        {
            for (var ii = 0; ii < this.groups.Length; ++ii) // TODO will there be enough groups for sequential search to matter?
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

        internal ref GroupKeysPair GetGroupRef(int ordinal) 
            => ref groups[ordinal];

        internal ref GroupKeysPair FindFreeGroupRef(long groupId, int keySize, long logAddr = Constants.kInvalidAddress)
        {
            if (!this.FindGroup(Constants.kInvalidPsfGroupId, out var ordinal))
            {
                // A new group was added while we were populating this; should be quite rare. // TODO test this case
                var groups = new GroupKeysPair[this.groups.Length + 1];
                Array.Copy(this.groups, groups, this.groups.Length);
                this.groups = groups;
                ordinal = this.groups.Length - 1;
            }
            ref GroupKeysPair ret = ref this.groups[ordinal];
            ret.GroupId = groupId;
            ret.KeySize = keySize;
            ret.LogicalAddress = logAddr;
            return ref ret;
        }

        public void AssignRcuRecordId(ref PSFValue<TRecordId> value)
            => value.RecordId = this.AfterRecordId;

        public void Dispose()
        {
            foreach (var group in this.groups)
                group.Dispose();
        }
    }
}
