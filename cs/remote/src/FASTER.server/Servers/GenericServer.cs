// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using System.IO;
using FASTER.core;
using FASTER.common;
using Microsoft.Extensions.Logging;

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
        readonly LogSettings logSettings;

        /// <summary>
        /// Create server instance; use Start to start the server.
        /// </summary>
        /// <param name="opts"></param>
        /// <param name="functionsGen"></param>
        /// <param name="serializer"></param>
        /// <param name="keyInputSerializer"></param>
        /// <param name="disableEphemeralLocking"></param>
        /// <param name="maxSizeSettings"></param>
        /// <param name="loggerFactory"></param>
        public GenericServer(ServerOptions opts, Func<Functions> functionsGen, ParameterSerializer serializer, IKeyInputSerializer<Key, Input> keyInputSerializer, 
                             bool disableEphemeralLocking, MaxSizeSettings maxSizeSettings = default, ILoggerFactory loggerFactory = null)
        {
            this.opts = opts;

            opts.GetSettings(out logSettings, out var checkpointSettings, out var indexSize);
            if (opts.EnableStorageTier && !Directory.Exists(opts.LogDir))
                Directory.CreateDirectory(opts.LogDir);
            if (!Directory.Exists(opts.CheckpointDir))
                Directory.CreateDirectory(opts.CheckpointDir);

            store = new FasterKV<Key, Value>(indexSize, logSettings, checkpointSettings, disableEphemeralLocking: disableEphemeralLocking, loggerFactory: loggerFactory);

            if (opts.Recover)
            {
                try
                {
                    store.Recover();
                }
                catch { }
            }

            if (!opts.DisablePubSub)
            {
                kvBroker = new SubscribeKVBroker<Key, Value, Input, IKeyInputSerializer<Key, Input>>(keyInputSerializer, null, opts.PubSubPageSizeBytes(), true);
                broker = new SubscribeBroker<Key, Value, IKeySerializer<Key>>(keyInputSerializer, null, opts.PubSubPageSizeBytes(), true);
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

            if (opts.EnableStorageTier && Directory.Exists(opts.LogDir))
                DeleteDirectory(opts.LogDir);
            if (Directory.Exists(opts.CheckpointDir))
                DeleteDirectory(opts.CheckpointDir);
        }

        /// <summary>
        /// Dipose, optionally deleting logs and checkpoints
        /// </summary>
        /// <param name="deleteDir">Whether to delete logs and checkpoints</param>
        public void Dispose(bool deleteDir = true)
        {
            InternalDispose();
            if (deleteDir)
            {
                if (opts.EnableStorageTier && Directory.Exists(opts.LogDir))
                    DeleteDirectory(opts.LogDir);
                if (Directory.Exists(opts.CheckpointDir))
                    DeleteDirectory(opts.CheckpointDir);
            }
        }
        private void InternalDispose()
        {
            server.Dispose();
            broker?.Dispose();
            kvBroker?.Dispose();
            store.Dispose();
            logSettings.LogDevice?.Dispose();
            logSettings.ObjectLogDevice?.Dispose();
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
