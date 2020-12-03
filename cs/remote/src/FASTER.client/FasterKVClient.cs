// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.common;
using System;
using System.Buffers;

namespace FASTER.client
{
    /// <summary>
    /// Client for remote FasterKV
    /// </summary>
    /// <typeparam name="Key"></typeparam>
    /// <typeparam name="Value"></typeparam>
    public class FasterKVClient<Key, Value> : IDisposable
    {
        internal readonly string address;
        internal readonly int port;

        /// <summary>
        /// Create client instance
        /// </summary>
        /// <param name="address">IP address of server</param>
        /// <param name="port">Port of server</param>
        public FasterKVClient(string address, int port)
        {
            this.address = address;
            this.port = port;
        }

        /// <summary>
        /// Dispose instance
        /// </summary>
        public void Dispose()
        {
        }

        /// <summary>
        /// Create session with remote FasterKV server (establishes network connection)
        /// </summary>
        /// <typeparam name="Input"></typeparam>
        /// <typeparam name="Output"></typeparam>
        /// <typeparam name="Context"></typeparam>
        /// <typeparam name="Functions"></typeparam>
        /// <typeparam name="ParameterSerializer"></typeparam>
        /// <param name="functions">Local callback functions</param>
        /// <param name="serializer">Parameter serializer</param>
        /// <param name="maxSizeSettings"></param>
        /// <returns></returns>
        public ClientSession<Key, Value, Input, Output, Context, Functions, ParameterSerializer> NewSession<Input, Output, Context, Functions, ParameterSerializer>(Functions functions, ParameterSerializer serializer = default, MaxSizeSettings maxSizeSettings = default)
            where Functions : ICallbackFunctions<Key, Value, Input, Output, Context>
            where ParameterSerializer : IClientSerializer<Key, Value, Input, Output>
        {
            return new ClientSession<Key, Value, Input, Output, Context, Functions, ParameterSerializer>(address, port, functions, serializer, maxSizeSettings ?? new MaxSizeSettings());
        }
    }

    /// <summary>
    /// Extension methods for client
    /// </summary>
    public static class ClientExtensions
    {
        /// <summary>
        /// Create new session
        /// </summary>
        /// <typeparam name="T">Type of memory (unmanaged)</typeparam>
        /// <param name="store">Client instance of store wrapper</param>
        /// <param name="maxSizeSettings">Settings for max sizes</param>
        /// <param name="memoryPool">Memory pool</param>
        /// <returns>Instance of client session</returns>
        public static ClientSession<ReadOnlyMemory<T>, ReadOnlyMemory<T>, ReadOnlyMemory<T>, (IMemoryOwner<T>, int), byte, MemoryFunctionsBase<T>, MemoryParameterSerializer<T>> NewSession<T>(this FasterKVClient<ReadOnlyMemory<T>, ReadOnlyMemory<T>> store, MaxSizeSettings maxSizeSettings = default, MemoryPool<T> memoryPool = default)
            where T : unmanaged
        {
            return new ClientSession<ReadOnlyMemory<T>, ReadOnlyMemory<T>, ReadOnlyMemory<T>, (IMemoryOwner<T>, int), byte, MemoryFunctionsBase<T>, MemoryParameterSerializer<T>>
                (store.address, store.port, new MemoryFunctionsBase<T>(), new MemoryParameterSerializer<T>(memoryPool), maxSizeSettings);
        }

        /// <summary>
        /// Create session with remote FasterKV server (establishes network connection)
        /// </summary>
        /// <typeparam name="Key">Key</typeparam>
        /// <typeparam name="Value">Value</typeparam>
        /// <returns></returns>
        public static ClientSession<Key, Value, Value, Value, byte, CallbackFunctionsBase<Key, Value, Value, Value, byte>, BlittableParameterSerializer<Key, Value, Value, Value>> NewSession<Key, Value>(this FasterKVClient<Key, Value> store, MaxSizeSettings maxSizeSettings = default)
            where Key : unmanaged
            where Value : unmanaged
        {
            return new ClientSession<Key, Value, Value, Value, byte, CallbackFunctionsBase<Key, Value, Value, Value, byte>, BlittableParameterSerializer<Key, Value, Value, Value>>
                (store.address, store.port, new CallbackFunctionsBase<Key, Value, Value, Value, byte>(), new BlittableParameterSerializer<Key, Value, Value, Value>(), maxSizeSettings);
        }

        /// <summary>
        /// Create new session
        /// </summary>
        /// <typeparam name="T">Type of memory (unmanaged)</typeparam>
        /// <typeparam name="Functions">Type of functions</typeparam>
        /// <param name="store">Client instance of store wrapper</param>
        /// <param name="functions">Functions</param>
        /// <param name="maxSizeSettings">Settings for max sizes</param>
        /// <param name="memoryPool">Memory pool</param>
        /// <returns>Instance of client session</returns>
        public static ClientSession<ReadOnlyMemory<T>, ReadOnlyMemory<T>, ReadOnlyMemory<T>, (IMemoryOwner<T>, int), byte, Functions, MemoryParameterSerializer<T>> NewSession<T, Functions>(this FasterKVClient<ReadOnlyMemory<T>, ReadOnlyMemory<T>> store, Functions functions, MaxSizeSettings maxSizeSettings = default, MemoryPool<T> memoryPool = default)
            where T : unmanaged
            where Functions : MemoryFunctionsBase<T>
        {
            return new ClientSession<ReadOnlyMemory<T>, ReadOnlyMemory<T>, ReadOnlyMemory<T>, (IMemoryOwner<T>, int), byte, Functions, MemoryParameterSerializer<T>>
                (store.address, store.port, functions, new MemoryParameterSerializer<T>(memoryPool), maxSizeSettings);
        }

        /// <summary>
        /// Create session with remote FasterKV server (establishes network connection)
        /// </summary>
        /// <typeparam name="Key"></typeparam>
        /// <typeparam name="Value"></typeparam>
        /// <typeparam name="Functions"></typeparam>
        /// <returns></returns>
        public static ClientSession<Key, Value, Value, Value, byte, Functions, BlittableParameterSerializer<Key, Value, Value, Value>> NewSession<Key, Value, Functions>(this FasterKVClient<Key, Value> store, Functions functions, MaxSizeSettings maxSizeSettings = default)
            where Key : unmanaged
            where Value : unmanaged
            where Functions : CallbackFunctionsBase<Key, Value, Value, Value, byte>
        {
            return new ClientSession<Key, Value, Value, Value, byte, Functions, BlittableParameterSerializer<Key, Value, Value, Value>>
                (store.address, store.port, functions, new BlittableParameterSerializer<Key, Value, Value, Value>(), maxSizeSettings);
        }

        /// <summary>
        /// Create session with remote FasterKV server (establishes network connection)
        /// </summary>
        /// <typeparam name="Key"></typeparam>
        /// <typeparam name="Value"></typeparam>
        /// <typeparam name="Input"></typeparam>
        /// <typeparam name="Output"></typeparam>
        /// <typeparam name="Functions"></typeparam>
        /// <returns></returns>
        public static ClientSession<Key, Value, Input, Output, byte, Functions, BlittableParameterSerializer<Key, Value, Input, Output>> NewSession<Key, Value, Input, Output, Functions>(this FasterKVClient<Key, Value> store, Functions functions, MaxSizeSettings maxSizeSettings = default)
            where Key : unmanaged
            where Value : unmanaged
            where Input : unmanaged
            where Output : unmanaged
            where Functions : CallbackFunctionsBase<Key, Value, Input, Output, byte>
        {
            return new ClientSession<Key, Value, Input, Output, byte, Functions, BlittableParameterSerializer<Key, Value, Input, Output>>
                (store.address, store.port, functions, new BlittableParameterSerializer<Key, Value, Input, Output>(), maxSizeSettings);
        }
    }
}
