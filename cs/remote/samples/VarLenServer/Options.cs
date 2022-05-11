// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using CommandLine;
using FASTER.server;

namespace FasterServerOptions
{
    class Options
    {
        [Option("port", Required = false, Default = 3278, HelpText = "Port to run server on")]
        public int Port { get; set; }

        [Option("bind", Required = false, Default = null, HelpText = "IP address to bind server to (default: any)")]
        public string Address { get; set; }

        [Option('m', "memory", Required = false, Default = "16g", HelpText = "Total log memory used in bytes (rounds down to power of 2)")]
        public string MemorySize { get; set; }

        [Option('p', "page", Required = false, Default = "32m", HelpText = "Size of each page in bytes (rounds down to power of 2)")]
        public string PageSize { get; set; }

        [Option('s', "segment", Required = false, Default = "1g", HelpText = "Size of each log segment in bytes on disk (rounds down to power of 2)")]
        public string SegmentSize { get; set; }

        [Option('i', "index", Required = false, Default = "8g", HelpText = "Size of hash index in bytes (rounds down to power of 2)")]
        public string IndexSize { get; set; }

        [Option("storage-tier", Required = false, Default = false, HelpText = "Enable tiering of records (hybrid log) to storage, to support a larger-than-memory store. Use --logdir to specify storage directory.")]
        public bool EnableStorageTier { get; set; }

        [Option('l', "logdir", Required = false, Default = null, HelpText = "Storage directory for tiered records (hybrid log), if storage tiering (--storage) is enabled. Uses current directory if unspecified.")]
        public string LogDir { get; set; }

        [Option('c', "checkpointdir", Required = false, Default = null, HelpText = "Storage directory for checkpoints. Uses logdir if unspecified.")]
        public string CheckpointDir { get; set; }

        [Option('r', "recover", Required = false, Default = false, HelpText = "Recover from latest checkpoint.")]
        public bool Recover { get; set; }

        [Option("no-pubsub", Required = false, Default = false, HelpText = "Disable pub/sub feature on server.")]
        public bool DisablePubSub { get; set; }

        [Option("pubsub-pagesize", Required = false, Default = "4k", HelpText = "Page size of log used for pub/sub (rounds down to power of 2)")]
        public string PubSubPageSize { get; set; }

        public ServerOptions GetServerOptions()
        {
            return new ServerOptions
            {
                Port = Port,
                Address = Address,
                MemorySize = MemorySize,
                PageSize = PageSize,
                SegmentSize = SegmentSize,
                IndexSize = IndexSize,
                EnableStorageTier = EnableStorageTier,
                LogDir = LogDir,
                CheckpointDir = CheckpointDir,
                Recover = Recover,
                DisablePubSub = DisablePubSub,
                PubSubPageSize = PubSubPageSize,
            };
        }
    }
}