// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;

namespace FASTER.core
{
    public interface IUserFunctions<K, V, I, O, C>
    {
        // Callbacks
        void RMWCompletionCallback(C ctx, Status status);
        void ReadCompletionCallback(C ctx, O output, Status status);
        void UpsertCompletionCallback(C ctx);


        // Read function
        void Reader(K key, I input, V value, ref O dst);

        // RMW functions
        void InitialUpdater(K key, I input, ref V value);
        void InPlaceUpdater(K key, I input, ref V value);
        void CopyUpdater(K key, I input, V oldValue, ref V newValue);
    }
}
