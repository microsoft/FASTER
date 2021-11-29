// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System;
using FASTER.core;
using FASTER.common;

namespace FASTER.server
{
    /// <summary>
    /// FASTER server for variable-length data
    /// </summary>
    public sealed class FixedLenServer<Key, Value, Input, Output, Functions> : GenericServer<Key, Value, Input, Output, Functions, FixedLenSerializer<Key, Value, Input, Output>>
        where Key : unmanaged
        where Value : unmanaged
        where Input : unmanaged
        where Output : unmanaged
        where Functions : IFunctions<Key, Value, Input, Output, long>
    {
        /// <summary>
        /// Create server instance; use Start to start the server.
        /// </summary>
        /// <param name="opts"></param>
        /// <param name="functionsGen"></param>
        /// <param name="supportsLocking"></param>
        /// <param name="maxSizeSettings"></param>
        public FixedLenServer(ServerOptions opts, Func<WireFormat, Functions> functionsGen, bool supportsLocking, MaxSizeSettings maxSizeSettings = default)
            : base(opts, functionsGen, new FixedLenSerializer<Key, Value, Input, Output>(), new FixedLenKeySerializer<Key, Input>(), supportsLocking, maxSizeSettings)
        {
        }
    }
}
