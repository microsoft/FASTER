// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma warning disable 1591

using System.Runtime.CompilerServices;

namespace FASTER.core
{
    public class MixedUserFunctions : IUserFunctions<MixedKey, MixedValue, MixedInput, MixedOutput, MixedContext>
    {
        public void ReadCompletionCallback(MixedContext ctx, MixedOutput output, Status status)
        {
        }

        public void RMWCompletionCallback(MixedContext ctx, Status status)
        {
        }

        public void UpsertCompletionCallback(MixedContext ctx)
        {
        }

        // Read function
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Reader(MixedKey key, MixedInput input, MixedValue value, ref MixedOutput dst)
        {
           // dst.value = value.value;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void InitialUpdater(MixedKey key, MixedInput input, ref MixedValue value)
        {
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void InPlaceUpdater(MixedKey key, MixedInput input, ref MixedValue value)
        {
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void CopyUpdater(MixedKey key, MixedInput input, MixedValue oldValue, ref MixedValue newValue)
        {
        }
    }
}
