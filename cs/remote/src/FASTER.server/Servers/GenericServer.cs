// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.IO;
using FASTER.core;
using FASTER.common;

namespace FASTER.server
{
    /// <summary>
    /// FASTER server for generic types
    /// </summary>
    public class GenericServer<Key, Value, Input, Output, Functions, ParameterSerializer> : IDisposable
            where Functions : IFunctions<Key, Value, Input, Output, long>
            where ParameterSerializer : IServerSerializer<Key, Value, Input, Output>
    {
        readonly ServerOptions opts;
        readonly IFasterServer server;
        readonly FasterKV<Key, Value> store;
        readonly FasterKVProvider<Key, Value, Input, Output, Functions, ParameterSerializer> provider;
        readonly SubscribeKVBroker<Key, Value, Input, IKeyInputSerializer<Key, Input>> kvBroker;
        readonly SubscribeBroker<Key, Value, IKeySerializer<Key>> broker;

        /// <summary>
        /// Create server instance; use Start to start the server.
        /// </summary>
        /// <param name="opts"></param>
        /// <param name="functionsGen"></param>
        /// <param name="serializer"></param>
        /// <param name="keyInputSerializer"></param>
        /// <param name="maxSizeSettings"></param>
        public GenericServer(ServerOptions opts, Func<Functions> functionsGen, ParameterSerializer serializer, IKeyInputSerializer<Key, Input> keyInputSerializer, MaxSizeSettings maxSizeSettings = default)
        {
            this.opts = opts;

            if (opts.LogDir != null && !Directory.Exists(opts.LogDir))
                Directory.CreateDirectory(opts.LogDir);

            if (opts.CheckpointDir != null && !Directory.Exists(opts.CheckpointDir))
                Directory.CreateDirectory(opts.CheckpointDir);

            opts.GetSettings(out var logSettings, out var checkpointSettings, out var indexSize);
            store = new FasterKV<Key, Value>(indexSize, logSettings, checkpointSettings);

            if (opts.Recover)
            {
                try
                {
                    store.Recover();
                }
                catch { }
            }

            if (opts.EnablePubSub)
            {
                kvBroker = new SubscribeKVBroker<Key, Value, Input, IKeyInputSerializer<Key, Input>>(keyInputSerializer, null, true);
                broker = new SubscribeBroker<Key, Value, IKeySerializer<Key>>(keyInputSerializer, null, true);
            }

            // Create session provider for VarLen
            provider = new FasterKVProvider<Key, Value, Input, Output, Functions, ParameterSerializer>(functionsGen, store, serializer, kvBroker, broker, opts.Recover, maxSizeSettings);

            server = new FasterServerTcp(opts.Address, opts.Port);
            server.Register(WireFormat.DefaultFixedLenKV, provider);
        }

        /// <summary>
        /// Start server instance
        /// </summary>
        public void Start() => server.Start();

        /// <summary>
        /// Dispose store (including log and checkpoint directory)
        /// </summary>
        public void Dispose()
        {
            InternalDispose();
            DeleteDirectory(opts.LogDir);
            DeleteDirectory(opts.CheckpointDir);
        }

        /// <summary>
        /// Dipose, optionally deleting logs and checkpoints
        /// </summary>
        /// <param name="deleteDir">Whether to delete logs and checkpoints</param>
        public void Dispose(bool deleteDir = true)
        {
            InternalDispose();
            if (deleteDir) DeleteDirectory(opts.LogDir);
            if (deleteDir) DeleteDirectory(opts.CheckpointDir);
        }

        private void InternalDispose()
        {
            server.Dispose();
            broker?.Dispose();
            kvBroker?.Dispose();
            store.Dispose();
        }

        private static void DeleteDirectory(string path)
        {
            if (path == null) return;

            foreach (string directory in Directory.GetDirectories(path))
            {
                DeleteDirectory(directory);
            }

            // Exceptions may happen due to a handle briefly remaining held after Dispose().
            try
            {
                Directory.Delete(path, true);
            }
            catch (Exception ex) when (ex is IOException ||
                                       ex is UnauthorizedAccessException)
            {
                try
                {
                    Directory.Delete(path, true);
                }
                catch { }
            }
        }
    }
}
