// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma warning disable 0162

using System;
using System.Threading.Tasks;

namespace FASTER.core
{
    /// <summary>
    /// Exception thrown when commit fails
    /// </summary>
    public class CommitFailureException : Exception
    {
        /// <summary>
        /// Next commit task after failed one
        /// </summary>
        public LinkedCommitInfo LinkedCommitInfo { get; private set; }
        internal CommitFailureException(LinkedCommitInfo linkedCommitInfo, string message)
            : base(message)
            => LinkedCommitInfo = linkedCommitInfo;
    }
}
