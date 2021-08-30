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
        public static FixedLenServer<long, long, long, long, SimpleFunctions<long, long, long>> CreateFixedLenServer(string logDir, Func<long, long, long> merger, bool enablePubSub = false, bool tryRecover = false)
        {
            ServerOptions opts = new()
            {
                LogDir = logDir,
                Address = Address,
                Port = Port,
                EnablePubSub = enablePubSub,
                Recover = tryRecover,
                IndexSize = "1m",
            };
            return new FixedLenServer<long, long, long, long, SimpleFunctions<long, long, long>>(opts, e => new SimpleFunctions<long, long, long>(merger));
        }

        /// <summary>
        /// Create VarLenServer
        /// </summary>
        public static VarLenServer CreateVarLenServer(string logDir, bool enablePubSub = false, bool tryRecover = false)
        {
            ServerOptions opts = new()
            {
                LogDir = logDir,
                Address = Address,
                Port = Port,
                EnablePubSub = enablePubSub,
                Recover = tryRecover,
                IndexSize = "1m",
            };
            return new VarLenServer(opts);
        }
    }
}