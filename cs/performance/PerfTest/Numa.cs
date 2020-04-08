// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using FASTER.core;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using System;

namespace FASTER.PerfTest
{
    [JsonConverter(typeof(StringEnumConverter))]
    public enum NumaMode
    {
        None,
        RoundRobin, // round robin across all processors
        Sharded2,   // Sharded across 2 groups
        Sharded4    // Sharded across 4 groups
    }

    public static class Numa
    {
        public static void AffinitizeThread(NumaMode mode, int threadIndex)
        {
            switch (mode) {
                case NumaMode.None:
                    return;
                case NumaMode.RoundRobin:
                    Native32.AffinitizeThreadRoundRobin((uint)threadIndex);
                    return;
                case NumaMode.Sharded2:
                case NumaMode.Sharded4:
                    Native32.AffinitizeThreadShardedNuma((uint)threadIndex, (ushort)(mode == NumaMode.Sharded2 ? 2 : 4));
                    return;
                default:
                    throw new ArgumentException("mode");
            } 
        }
    }
}
