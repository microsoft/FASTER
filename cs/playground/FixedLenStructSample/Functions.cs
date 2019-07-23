// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using System;

namespace FixedLenStructSample
{
    /// <summary>
    /// Callback functions for FASTER operations
    /// </summary>
    public class FixedLenFunctions : IFunctions<FixedLenKey, FixedLenValue, string, string, Empty>
    {
        public void CheckpointCompletionCallback(Guid sessionId, long serialNum)
        {
        }

        public void ConcurrentReader(ref FixedLenKey key, ref string input, ref FixedLenValue value, ref string dst)
        {
            dst = value.ToString();
        }

        public void ConcurrentWriter(ref FixedLenKey key, ref FixedLenValue src, ref FixedLenValue dst)
        {
            src.CopyTo(ref dst);
        }

        public void CopyUpdater(ref FixedLenKey key, ref string input, ref FixedLenValue oldValue, ref FixedLenValue newValue)
        {
        }

        public void DeleteCompletionCallback(ref FixedLenKey key, Empty ctx)
        {
        }

        public void InitialUpdater(ref FixedLenKey key, ref string input, ref FixedLenValue value)
        {
        }

        public void InPlaceUpdater(ref FixedLenKey key, ref string input, ref FixedLenValue value)
        {
        }

        public void ReadCompletionCallback(ref FixedLenKey key, ref string input, ref string output, Empty ctx, Status status)
        {
        }

        public void RMWCompletionCallback(ref FixedLenKey key, ref string input, Empty ctx, Status status)
        {
        }

        public void SingleReader(ref FixedLenKey key, ref string input, ref FixedLenValue value, ref string dst)
        {
            dst = value.ToString();
        }

        public void SingleWriter(ref FixedLenKey key, ref FixedLenValue src, ref FixedLenValue dst)
        {
            src.CopyTo(ref dst);
        }

        public void UpsertCompletionCallback(ref FixedLenKey key, ref FixedLenValue value, Empty ctx)
        {
        }
    }
}
