// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using FASTER.core;
using FASTER.server;

namespace FASTER.remote.test
{
    internal static class TestUtils
    {
        /// <summary>
        /// Address
        /// </summary>
        public static string Address = "127.0.0.1";

        /// <summary>
        /// Port
        /// </summary>
        public static int Port = 33278;

        /// <summary>
        /// Create VarLenServer
        /// </summary>
        public static FixedLenServer<long, long, long, long, SimpleFunctions<long, long, long>> CreateFixedLenServer(string logDir, Func<long, long, long> merger, bool disablePubSub = false, bool tryRecover = false)
        {
            ServerOptions opts = new()
            {
                EnableStorageTier = logDir != null,
                LogDir = logDir,
                Address = Address,
                Port = Port,
                DisablePubSub = disablePubSub,
                Recover = tryRecover,
                IndexSize = "1m",
            };
            return new FixedLenServer<long, long, long, long, SimpleFunctions<long, long, long>>(opts, () => new SimpleFunctions<long, long, long>(merger), disableEphemeralLocking: true);
        }

        /// <summary>
        /// Create VarLenServer
        /// </summary>
        public static VarLenServer CreateVarLenServer(string logDir, bool disablePubSub = false, bool tryRecover = false)
        {
            ServerOptions opts = new()
            {
                EnableStorageTier = logDir != null,
                LogDir = logDir,
                Address = Address,
                Port = Port,
                DisablePubSub = disablePubSub,
                Recover = tryRecover,
                IndexSize = "1m",
            };
            return new VarLenServer(opts);
        }
    }
}