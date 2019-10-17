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
        public Task<CommitInfo> NextCommitTask { get; private set; }
        internal CommitFailureException(Task<CommitInfo> nextCommitTask, string message)
            : base(message)
            => NextCommitTask = nextCommitTask;
    }
}
