using System;
using System.Runtime.CompilerServices;
using FASTER.common;
using FASTER.core;

namespace FASTER.server
{
    /// <summary>
    /// Provides backend store (e.g., FasterLog, FasterKV) of the Faster Server
    /// </summary>
    public interface IFasterRemoteBackendProvider : IDisposable
    {
        /// <summary>
        /// Given messages of wire format type, returns the backend object for that type. If no backend is configured
        /// for the given type or if the supplied BackendType does not match the type of object configured, will
        /// throw an exception 
        /// </summary>
        /// <param name="type"> the wire format type</param>
        /// <typeparam name="BackendType">object type of the configured backend for given type</typeparam>
        /// <returns>configured backend object</returns>
        BackendType GetBackendForProtocol<BackendType>(WireFormat type);
    }

    public class FasterKVBackend<Key, Value, Functions, ParameterSerializer> : IDisposable
    {
        internal FasterKV<Key, Value> store;
        internal Func<WireFormat, Functions> functionsGen;
        internal ParameterSerializer serializer;

        internal FasterKVBackend(FasterKV<Key, Value> store, Func<WireFormat, Functions> functionsGen,
            ParameterSerializer serializer = default)
        {
            this.store = store;
            this.functionsGen = functionsGen;
            this.serializer = serializer;
        }

        public void Dispose()
        {
            store.Dispose();
        }
    }
    
    public class FasterKVBackendProvider<Key, Value, Functions, ParameterSerializer> : IFasterRemoteBackendProvider
    {
        private FasterKVBackend<Key, Value, Functions, ParameterSerializer> backend;

        public FasterKVBackendProvider(FasterKVBackend<Key, Value, Functions, ParameterSerializer> backend)
        {
            this.backend = backend;
        }

        public FasterKVBackendProvider(FasterKV<Key, Value> store, Func<WireFormat, Functions> functionsGen,
            ParameterSerializer serializer = default)
        {
            backend =
                new FasterKVBackend<Key, Value, Functions, ParameterSerializer>(store, functionsGen, serializer);
        }
        
        public BackendType GetBackendForProtocol<BackendType>(WireFormat type)
        {
            if (type != WireFormat.Binary)
                throw new InvalidOperationException(
                    $"Backend required for protocol {type.ToString()} is not supported on this server");
            if (typeof(BackendType) != typeof(FasterKVBackend<Key, Value, Functions, ParameterSerializer>))
            {
                throw new InvalidCastException(
                    $"Backend is configured to provide {typeof(FasterKVBackend<Key, Value, Functions, ParameterSerializer>)} to" +
                    $"protocol {type.ToString()}, instead, {typeof(BackendType)} was requested");
            }
            // Throws invalid cast exception if user calls with wrong type.
            return (BackendType) (object) backend;
        }

        public void Dispose()
        {
            backend.Dispose();
        }
    }
}