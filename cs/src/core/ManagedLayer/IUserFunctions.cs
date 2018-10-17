// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.Runtime.CompilerServices;

namespace FASTER.core
{
    /// <summary>
    /// Interface for user functions
    /// </summary>
    /// <typeparam name="K"></typeparam>
    /// <typeparam name="V"></typeparam>
    /// <typeparam name="I"></typeparam>
    /// <typeparam name="O"></typeparam>
    /// <typeparam name="C"></typeparam>
    public interface IUserFunctions<K, V, I, O, C>
    {
        // Callbacks

        /// <summary>
        /// Callback when async RMW operation completes 
        /// </summary>
        /// <param name="ctx">User context</param>
        /// <param name="status">Operation status</param>
        void RMWCompletionCallback(C ctx, Status status);

        /// <summary>
        /// Callback when async read operation completes
        /// </summary>
        /// <param name="ctx">User context</param>
        /// <param name="output">Output of read</param>
        /// <param name="status">Operation status</param>
        void ReadCompletionCallback(C ctx, O output, Status status);

        /// <summary>
        /// Callback when async upsert operation completes
        /// </summary>
        /// <param name="ctx">User context</param>
        void UpsertCompletionCallback(C ctx);


        // Read function

        /// <summary>
        /// Callback to read data from value
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="input">Input used as parameter to read</param>
        /// <param name="value">Value being read from</param>
        /// <param name="dst">Output of read</param>
        void Reader(K key, I input, V value, ref O dst);

        // RMW functions

        /// <summary>
        /// Initial value for RMW
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="input">Input used as parameter to RMW</param>
        /// <param name="value">Value being initialized</param>
        void InitialUpdater(K key, I input, ref V value);

        /// <summary>
        /// In-place update for RMW
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="input">Input used as parameter to RMW</param>
        /// <param name="value">Value being updated in-place</param>
        void InPlaceUpdater(K key, I input, ref V value);

        /// <summary>
        /// Copy-update for RMW
        /// </summary>
        /// <param name="key">Key</param>
        /// <param name="input">Input used as parameter to RMW</param>
        /// <param name="oldValue">Old value to be updated</param>
        /// <param name="newValue">New value after update</param>
        void CopyUpdater(K key, I input, V oldValue, ref V newValue);
    }
}
