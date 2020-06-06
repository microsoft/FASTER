// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;

namespace FasterPSFSample
{
    public struct Key : IFasterEqualityComparer<Key>
    {
        // Note: int instead of long because we won't use enough values to overflow and having it a 
        // different length than TRecordId (which is long) makes sure the PSFValue offsetting is in sync.
        public int Id { get; set; }

        public Key(int id) => this.Id = id;

        public long GetHashCode64(ref Key key) => Utility.GetHashCode(key.Id);

        public bool Equals(ref Key k1, ref Key k2) => k1.Id == k2.Id;

        public (int, int, int, int) MemberTuple => FasterPSFSample.keyDict[this].MemberTuple;
    }
}
