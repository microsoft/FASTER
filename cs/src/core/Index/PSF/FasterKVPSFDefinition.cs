// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;

namespace FASTER.core
{
    /// <summary>
    /// The definition of a single PSF (Predicate Subset Function)
    /// </summary>
    /// <typeparam name="TKVKey">The type of the key in the primary FasterKV instance</typeparam>
    /// <typeparam name="TKVValue">The type of the value in the primary FasterKV instance</typeparam>
    /// <typeparam name="TPSFKey">The type of the key returned by the Predicate and store in the secondary
    ///     (PSF-implementing) FasterKV instances</typeparam>
    public class FasterKVPSFDefinition<TKVKey, TKVValue, TPSFKey> : IPSFDefinition<FasterKVProviderData<TKVKey, TKVValue>, TPSFKey>
        where TKVKey : new()
        where TKVValue : new()
        where TPSFKey : struct
    {
        /// <summary>
        /// The definition of the delegate used to obtain a new key matching the Value for this PSF definition.
        /// </summary>
        /// <param name="kvKey">The key sent to FasterKV on Upsert or RMW</param>
        /// <param name="kvValue">The value sent to FasterKV on Upsert or RMW</param>
        /// <remarks>This must be a delegate instead of a lambda to allow ref parameters</remarks>
        /// <returns>Null if the value does not match the predicate, else a key for the value in the PSF hash table</returns>
        public delegate TPSFKey? PredicateFunc(ref TKVKey kvKey, ref TKVValue kvValue);

        /// <summary>
        /// The predicate function that will be called by FasterKV on Upsert or RMW.
        /// </summary>
        public PredicateFunc Predicate;

        /// <summary>
        /// Executes the Predicate
        /// </summary>
        /// <param name="record">The record obtained from the primary FasterKV instance</param>
        /// <returns></returns>
        /// <returns>Null if the value does not match the predicate, else a key for the value in the PSF hash table</returns>
        public TPSFKey? Execute(FasterKVProviderData<TKVKey, TKVValue> record) 
            => Predicate(ref record.GetKey(), ref record.GetValue());

        /// <summary>
        /// The Name of the PSF, assigned by the caller. Must be unique among all PSFs (TODO: enforce).
        /// </summary>
        public string Name { get; }

        /// <summary>
        /// Instantiates the instance with the name and predicate delegate
        /// </summary>
        /// <param name="name"></param>
        /// <param name="predicate"></param>
        public FasterKVPSFDefinition(string name, PredicateFunc predicate)
        {
            this.Name = name;
            this.Predicate = predicate;
        }

        /// <summary>
        /// Instantiates the instance with the name and predicate Func{}, which we wrap in a delegate.
        /// This allows a streamlined API call.
        /// </summary>
        /// <param name="name"></param>
        /// <param name="predicate"></param>
        public FasterKVPSFDefinition(string name, Func<TKVKey, TKVValue, TPSFKey> predicate)
        {
            TPSFKey? wrappedPredicate(ref TKVKey key, ref TKVValue value) => predicate(key, value);

            this.Name = name;
            this.Predicate = wrappedPredicate;
        }
    }
}
 