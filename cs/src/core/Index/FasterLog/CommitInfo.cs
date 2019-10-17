// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma warning disable 0162

using System.Threading.Tasks;

namespace FASTER.core
{
    /// <summary>
    /// Info contained in task associated with commit
    /// </summary>
    public struct CommitInfo
    {
        internal long fromAddress;
        internal long untilAddress;
        internal uint errorCode;
        internal TaskCompletionSource<CommitInfo> nextTcs;
    }
}
