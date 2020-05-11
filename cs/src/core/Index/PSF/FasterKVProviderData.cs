// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;

#pragma warning disable CS1591 // Missing XML comment for publicly visible type or member

namespace FASTER.core
{
    /// <summary>
    /// The wrapper around the provider data stored in the primary faster instance.
    /// </summary>
    /// <typeparam name="TKVKey">The type of the key in the primary FasterKV instance</typeparam>
    /// <typeparam name="TKVValue">The type of the value in the primary FasterKV instance</typeparam>
    /// <remarks>Having this enables separation between the LogicalAddress stored in the PSF-implementing
    ///     FasterKV instances, and the actual <typeparamref name="TKVKey"/> and <typeparamref name="TKVValue"/>
    ///     types.</remarks>
    public class FasterKVProviderData<TKVKey, TKVValue> : IDisposable
        where TKVKey : new()
        where TKVValue : new()
    {
        // C# doesn't allow ref fields and even if it did, if the client held the FasterKVProviderData
        // past the ref lifetime, bad things would happen when accessing the ref key/value.
        internal IHeapContainer<TKVKey> keyContainer;       // TODO: Perf efficiency
        internal IHeapContainer<TKVValue> valueContainer;

        internal FasterKVProviderData(AllocatorBase<TKVKey, TKVValue> allocator, ref TKVKey key, ref TKVValue value)
        {
            this.keyContainer = allocator.GetKeyContainer(ref key);
            this.valueContainer = allocator.GetValueContainer(ref value);
        }

        public unsafe ref TKVKey GetKey() => ref this.keyContainer.Get();
        
        public unsafe ref TKVValue GetValue() => ref this.valueContainer.Get();

        /// <inheritdoc/>
        public void Dispose()
        {
            this.keyContainer.Dispose();
            this.valueContainer.Dispose();
        }
    }
}
