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
    public sealed class FasterKVClient<Key, Value> : IDisposable
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
        /// <param name="wireFormat">Wire format</param>
        /// <param name="serializer">Parameter serializer</param>
        /// <param name="maxSizeSettings"></param>
        /// <returns></returns>
        public ClientSession<Key, Value, Input, Output, Context, Functions, ParameterSerializer> NewSession<Input, Output, Context, Functions, ParameterSerializer>(Functions functions, WireFormat wireFormat, ParameterSerializer serializer = default, MaxSizeSettings maxSizeSettings = default)
            where Functions : ICallbackFunctions<Key, Value, Input, Output, Context>
            where ParameterSerializer : IClientSerializer<Key, Value, Input, Output>
        {
            return new ClientSession<Key, Value, Input, Output, Context, Functions, ParameterSerializer>(address, port, functions, wireFormat, serializer, maxSizeSettings ?? new MaxSizeSettings());
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
        public static ClientSession<ReadOnlyMemory<T>, ReadOnlyMemory<T>, ReadOnlyMemory<T>, (IMemoryOwner<T>, int), byte, MemoryFunctionsBase<T>, MemoryParameterSerializer<T>> NewSession<T>(this FasterKVClient<ReadOnlyMemory<T>, ReadOnlyMemory<T>> store, WireFormat wireFormat = WireFormat.DefaultVarLenKV, MaxSizeSettings maxSizeSettings = default, MemoryPool<T> memoryPool = default)
            where T : unmanaged
        {
            return new ClientSession<ReadOnlyMemory<T>, ReadOnlyMemory<T>, ReadOnlyMemory<T>, (IMemoryOwner<T>, int), byte, MemoryFunctionsBase<T>, MemoryParameterSerializer<T>>
                (store.address, store.port, new MemoryFunctionsBase<T>(), wireFormat, new MemoryParameterSerializer<T>(memoryPool), maxSizeSettings);
        }

        /// <summary>
        /// Create new session
        /// </summary>
        public static ClientSession<Key, Value, Value, Value, byte, CallbackFunctionsBase<Key, Value, Value, Value, byte>, FixedLenSerializer<Key, Value, Value, Value>> NewSession<Key, Value>(this FasterKVClient<Key, Value> store, WireFormat wireFormat = WireFormat.DefaultFixedLenKV, MaxSizeSettings maxSizeSettings = default)
            where Key : unmanaged
            where Value : unmanaged
        {
            return new ClientSession<Key, Value, Value, Value, byte, CallbackFunctionsBase<Key, Value, Value, Value, byte>, FixedLenSerializer<Key, Value, Value, Value>>
                (store.address, store.port, new CallbackFunctionsBase<Key, Value, Value, Value, byte>(), wireFormat, new FixedLenSerializer<Key, Value, Value, Value>(), maxSizeSettings);
        }

        /// <summary>
        /// Create new session
        /// </summary>
        public static ClientSession<ReadOnlyMemory<T>, ReadOnlyMemory<T>, ReadOnlyMemory<T>, (IMemoryOwner<T>, int), byte, Functions, MemoryParameterSerializer<T>> NewSession<T, Functions>(this FasterKVClient<ReadOnlyMemory<T>, ReadOnlyMemory<T>> store, Functions functions, WireFormat wireFormat = WireFormat.DefaultVarLenKV, MaxSizeSettings maxSizeSettings = default, MemoryPool<T> memoryPool = default)
            where T : unmanaged
            where Functions : MemoryFunctionsBase<T>
        {
            return new ClientSession<ReadOnlyMemory<T>, ReadOnlyMemory<T>, ReadOnlyMemory<T>, (IMemoryOwner<T>, int), byte, Functions, MemoryParameterSerializer<T>>
                (store.address, store.port, functions, wireFormat, new MemoryParameterSerializer<T>(memoryPool), maxSizeSettings);
        }

        /// <summary>
        /// Create new session
        /// </summary>
        public static ClientSession<Key, Value, Value, Value, byte, Functions, FixedLenSerializer<Key, Value, Value, Value>> NewSession<Key, Value, Functions>(this FasterKVClient<Key, Value> store, Functions functions, WireFormat wireFormat = WireFormat.DefaultFixedLenKV, MaxSizeSettings maxSizeSettings = default)
            where Key : unmanaged
            where Value : unmanaged
            where Functions : CallbackFunctionsBase<Key, Value, Value, Value, byte>
        {
            return new ClientSession<Key, Value, Value, Value, byte, Functions, FixedLenSerializer<Key, Value, Value, Value>>
                (store.address, store.port, functions, wireFormat, new FixedLenSerializer<Key, Value, Value, Value>(), maxSizeSettings);
        }

        /// <summary>
        /// Create new session
        /// </summary>
        public static ClientSession<Key, Value, Input, Output, byte, Functions, FixedLenSerializer<Key, Value, Input, Output>> NewSession<Key, Value, Input, Output, Functions>(this FasterKVClient<Key, Value> store, Functions functions, WireFormat wireFormat = WireFormat.DefaultFixedLenKV, MaxSizeSettings maxSizeSettings = default)
            where Key : unmanaged
            where Value : unmanaged
            where Input : unmanaged
            where Output : unmanaged
            where Functions : CallbackFunctionsBase<Key, Value, Input, Output, byte>
        {
            return new ClientSession<Key, Value, Input, Output, byte, Functions, FixedLenSerializer<Key, Value, Input, Output>>
                (store.address, store.port, functions, wireFormat, new FixedLenSerializer<Key, Value, Input, Output>(), maxSizeSettings);
        }
        
        /// <summary>
        /// Create new session
        /// </summary>
        public static ClientSession<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, byte, SpanByteFunctionsBase, SpanByteClientSerializer> NewSession(this FasterKVClient<SpanByte, SpanByte> store, SpanByteFunctionsBase functions, WireFormat wireFormat = WireFormat.DefaultVarLenKV, MaxSizeSettings maxSizeSettings = default, MemoryPool<byte> memoryPool = default)

        {
            return new ClientSession<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, byte, SpanByteFunctionsBase, SpanByteClientSerializer>
            (store.address, store.port, functions, wireFormat, new SpanByteClientSerializer(memoryPool),
                maxSizeSettings);
        }

        /// <summary>
        /// Create new session
        /// </summary>
        public static ClientSession<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, byte, Functions, SpanByteClientSerializer> NewSession<Functions>(this FasterKVClient<SpanByte, SpanByte> store, Functions functions, WireFormat wireFormat = WireFormat.DefaultVarLenKV, MaxSizeSettings maxSizeSettings = default, MemoryPool<byte> memoryPool = default)
           where Functions : SpanByteFunctionsBase

        {
            return new ClientSession<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, byte, Functions, SpanByteClientSerializer>
            (store.address, store.port, functions, wireFormat, new SpanByteClientSerializer(memoryPool),
                maxSizeSettings);
        }
    }
}
